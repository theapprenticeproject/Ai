"""PostgreSQL + pgvector backend for TAP AI RAG retrieval."""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional

import frappe
import psycopg2
from psycopg2.extras import RealDictCursor

from tap_ai.infra.config import get_config
from tap_ai.infra.sql_catalog import load_schema
from tap_ai.services.kb.direct_response_bank import _parse_aliases, get_direct_response_entries
from tap_ai.services.rag.pinecone_store import embed_documents_cached, embed_query_cached, _record_to_text
from tap_ai.services.routing.doctype_profiler import route_by_similarity
from tap_ai.services.sql.doctype_selector import pick_doctypes
from tap_ai.utils.remote_db import execute_remote_query, execute_remote_query_paginated, get_remote_connection


_VECTOR_TABLE = "tap_ai_rag_vectors"
_KB_NAMESPACE = "TAP Response Knowledge"
_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY = False
_SEARCH_EXECUTOR = ThreadPoolExecutor(max_workers=8)


def _pgvector_prereq_error(exc: Exception) -> RuntimeError:
	error = RuntimeError(
		"pgvector is not installed on the remote PostgreSQL server. "
		"Install the PostgreSQL pgvector package on the DB host (for PostgreSQL 14, "
		"usually the `postgresql-14-pgvector` package), restart PostgreSQL, and rerun "
		"`bench execute tap_ai.services.rag.pgvector_store.cli_migrate_pgvector`."
	)
	error.__cause__ = exc
	return error


def _vector_dimension() -> int:
	try:
		return max(1, int(get_config("embedding_dimension") or 1536))
	except Exception:
		return 1536


def _vector_literal(vector: List[float]) -> str:
	return "[" + ",".join(f"{float(value):.8f}" for value in vector) + "]"


def _ensure_schema() -> None:
	global _SCHEMA_READY
	if _SCHEMA_READY:
		return
	with _SCHEMA_LOCK:
		if _SCHEMA_READY:
			return
		sql = f"""
		CREATE EXTENSION IF NOT EXISTS vector;
		CREATE TABLE IF NOT EXISTS "{_VECTOR_TABLE}" (
			id TEXT PRIMARY KEY,
			namespace TEXT NOT NULL,
			doctype TEXT NOT NULL,
			record_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
			count INTEGER NOT NULL DEFAULT 1,
			context_preview TEXT NOT NULL DEFAULT '',
			grade TEXT,
			batch TEXT,
			course TEXT,
			content_type TEXT,
			embedding vector({_vector_dimension()}) NOT NULL,
			source_backend TEXT NOT NULL DEFAULT 'pgvector',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS "{_VECTOR_TABLE}_namespace_idx" ON "{_VECTOR_TABLE}" (namespace);
		CREATE INDEX IF NOT EXISTS "{_VECTOR_TABLE}_doctype_idx" ON "{_VECTOR_TABLE}" (doctype);
		CREATE INDEX IF NOT EXISTS "{_VECTOR_TABLE}_grade_idx" ON "{_VECTOR_TABLE}" (grade);
		CREATE INDEX IF NOT EXISTS "{_VECTOR_TABLE}_batch_idx" ON "{_VECTOR_TABLE}" (batch);
		CREATE INDEX IF NOT EXISTS "{_VECTOR_TABLE}_course_idx" ON "{_VECTOR_TABLE}" (course);
		CREATE INDEX IF NOT EXISTS "{_VECTOR_TABLE}_content_type_idx" ON "{_VECTOR_TABLE}" (content_type);
		"""
		with get_remote_connection() as conn:
			with conn.cursor() as cursor:
				cursor.execute(sql)
			conn.commit()
		_SCHEMA_READY = True


def cli_migrate_pgvector() -> Dict[str, Any]:
	"""Create or repair the pgvector schema in the remote PostgreSQL database.

	Example:
	bench execute tap_ai.services.rag.pgvector_store.cli_migrate_pgvector
	"""
	try:
		_ensure_schema()
	except psycopg2.errors.UndefinedFile as exc:
		raise _pgvector_prereq_error(exc)
	except psycopg2.Error as exc:
		if getattr(exc, "pgcode", None) == "58P01":
			raise _pgvector_prereq_error(exc)
		raise
	result = {
		"status": "ok",
		"table": _VECTOR_TABLE,
		"dimension": _vector_dimension(),
		"backend": "pgvector",
	}
	print(frappe.as_json(result, indent=2))
	return result


def _get_excluded_doctypes() -> set[str]:
	cache_key = "tap_ai:excluded_doctypes"
	try:
		cached = frappe.cache().get(cache_key)
		if cached:
			if isinstance(cached, bytes):
				cached = cached.decode("utf-8", errors="ignore")
			return set(json.loads(cached))
	except Exception:
		pass
	return set()


def _filter_excluded(doctypes: List[str]) -> List[str]:
	excluded = _get_excluded_doctypes()
	return [doctype for doctype in doctypes if doctype not in excluded]


def _build_metadata_filter(user_profile=None, content_details=None) -> Optional[Dict[str, Any]]:
	filters = {}
	if user_profile:
		if getattr(user_profile, "grade", None):
			filters["grade"] = user_profile.grade
		if getattr(user_profile, "batch", None):
			filters["batch"] = user_profile.batch
		current_enrollment = getattr(user_profile, "current_enrollment", None)
		if current_enrollment and getattr(current_enrollment, "course", None):
			filters["course"] = current_enrollment.course
	if content_details and getattr(content_details, "type", None):
		filters["content_type"] = content_details.type
	return filters or None


def _max_context_hits() -> int:
	try:
		return max(1, min(int(get_config("rag_max_context_hits") or 6), 6))
	except Exception:
		return 6


def _effective_k(k: int) -> int:
	try:
		parsed = int(k)
	except Exception:
		parsed = 6
	return max(1, min(parsed, 6))


def _build_context_from_hits(hits: List[Dict[str, Any]], max_chars: int = 12000) -> Dict[str, Any]:
	context_chunks: List[str] = []
	sources: List[Dict[str, Any]] = []
	used_chars = 0
	for hit in (hits or [])[:_max_context_hits()]:
		if used_chars >= max_chars:
			break
		meta = hit.get("metadata") or {}
		doctype = meta.get("doctype")
		record_ids = meta.get("record_ids") or []
		preview = (meta.get("context_preview") or "").strip()
		if not doctype or not record_ids or not preview:
			continue
		chunk = f"DocType: {doctype}\nID: {record_ids[0]}\n{preview}"
		if used_chars + len(chunk) > max_chars:
			continue
		context_chunks.append(chunk)
		sources.append({"doctype": doctype, "id": record_ids[0], "score": hit.get("score")})
		used_chars += len(chunk)
	return {
		"context_text": "\n\n---\n\n".join(context_chunks),
		"sources": sources,
		"stats": {
			"total_hits_in": len(hits or []),
			"top_hits_used": len((hits or [])[:_max_context_hits()]),
			"context_chars": used_chars,
		},
	}


def _search_namespace(namespace: str, qvec: List[float], k: int, filters: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
	_ensure_schema()
	where_clauses = ["namespace = %s"]
	where_params: List[Any] = [namespace]
	if filters:
		for field in ("grade", "batch", "course", "content_type"):
			value = filters.get(field)
			if value:
				where_clauses.append(f'"{field}" = %s')
				where_params.append(value)
	qvec_literal = _vector_literal(qvec)
	params: List[Any] = [qvec_literal, *where_params, qvec_literal, k]
	sql = f'''
		SELECT
			id, namespace, doctype, record_ids, count, context_preview,
			grade, batch, course, content_type,
			1 - (embedding <=> %s::vector) AS score
		FROM "{_VECTOR_TABLE}"
		WHERE {' AND '.join(where_clauses)}
		ORDER BY embedding <=> %s::vector
		LIMIT %s
	'''
	with get_remote_connection() as conn:
		with conn.cursor(cursor_factory=RealDictCursor) as cursor:
			cursor.execute(sql, tuple(params))
			rows = cursor.fetchall()
	return [
		{
			"id": row.get("id"),
			"score": float(row.get("score") or 0),
			"namespace": row.get("namespace") or namespace,
			"metadata": {
				"doctype": row.get("doctype") or namespace,
				"record_ids": row.get("record_ids") or [],
				"count": row.get("count") or 1,
				"context_preview": row.get("context_preview") or "",
				"grade": row.get("grade"),
				"batch": row.get("batch"),
				"course": row.get("course"),
				"content_type": row.get("content_type"),
				"source_backend": "pgvector",
			},
		}
		for row in rows
	]


def search_auto_namespaces(q: str, k: int = 6, route_top_n: int = 4, filters: Optional[Dict[str, Any]] = None, use_parallel: bool = True) -> Dict[str, Any]:
	_ensure_schema()
	qvec = embed_query_cached(q)
	raw_flag = get_config("enable_doctype_profiler")
	profiler_enabled = True if raw_flag is None else bool(raw_flag)
	if profiler_enabled:
		doctypes = route_by_similarity(qvec, top_n=route_top_n)
		if not doctypes:
			doctypes = pick_doctypes(q, top_n=route_top_n) or []
	else:
		schema = load_schema()
		doctypes = [table.replace("tab", "") for table in schema.get("allowlist", [])]
	doctypes = _filter_excluded(doctypes)
	if not doctypes:
		schema = load_schema()
		doctypes = [table.replace("tab", "") for table in schema.get("allowlist", [])][:route_top_n]
	all_matches: List[Dict[str, Any]] = []
	namespace_timings_ms: Dict[str, float] = {}

	def _query_one(namespace: str) -> tuple[str, List[Dict[str, Any]], float]:
		t0 = time.perf_counter()
		matches = _search_namespace(namespace, qvec, k, filters)
		elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
		return namespace, matches, elapsed_ms

	if use_parallel and len(doctypes) > 1:
		future_map = {
			_SEARCH_EXECUTOR.submit(_query_one, namespace): namespace
			for namespace in doctypes
		}
		for future in as_completed(future_map):
			namespace = future_map[future]
			try:
				ns, matches, elapsed_ms = future.result()
				namespace_timings_ms[ns] = elapsed_ms
				all_matches.extend(matches)
			except Exception as e:
				namespace_timings_ms[namespace] = -1.0
				frappe.log_error(f"pgvector query failed for namespace {namespace}", str(e))
	else:
		for namespace in doctypes:
			try:
				ns, matches, elapsed_ms = _query_one(namespace)
				namespace_timings_ms[ns] = elapsed_ms
				all_matches.extend(matches)
			except Exception as e:
				namespace_timings_ms[namespace] = -1.0
				frappe.log_error(f"pgvector query failed for namespace {namespace}", str(e))
	all_matches.sort(key=lambda match: match.get("score", 0), reverse=True)
	return {
		"q": q,
		"routed_doctypes": doctypes,
		"profiler_enabled": profiler_enabled,
		"k": k,
		"matches": all_matches[:k],
		"backend": "pgvector",
		"search_mode": "parallel" if use_parallel and len(doctypes) > 1 else "sequential",
		"namespace_timings_ms": namespace_timings_ms,
	}


def _upsert_rows(rows: List[Dict[str, Any]]) -> int:
	if not rows:
		return 0
	_ensure_schema()
	insert_sql = f'''
		INSERT INTO "{_VECTOR_TABLE}" (
			id, namespace, doctype, record_ids, count, context_preview,
			grade, batch, course, content_type, embedding, source_backend, updated_at
		) VALUES (
			%(id)s, %(namespace)s, %(doctype)s, %(record_ids)s::jsonb, %(count)s,
			%(context_preview)s, %(grade)s, %(batch)s, %(course)s, %(content_type)s,
			%(embedding)s::vector, %(source_backend)s, NOW()
		)
		ON CONFLICT (id) DO UPDATE SET
			namespace = EXCLUDED.namespace,
			doctype = EXCLUDED.doctype,
			record_ids = EXCLUDED.record_ids,
			count = EXCLUDED.count,
			context_preview = EXCLUDED.context_preview,
			grade = EXCLUDED.grade,
			batch = EXCLUDED.batch,
			course = EXCLUDED.course,
			content_type = EXCLUDED.content_type,
			embedding = EXCLUDED.embedding,
			source_backend = EXCLUDED.source_backend,
			updated_at = NOW()
	'''
	with get_remote_connection() as conn:
		with conn.cursor() as cursor:
			for row in rows:
				cursor.execute(insert_sql, row)
		conn.commit()
	return len(rows)


def upsert_doctype(doctype: str, since: Optional[str] = None, embed_batch: int = 100) -> Dict[str, Any]:
	_ensure_schema()
	sem_cfg = None
	try:
		from tap_ai.services.rag.pinecone_store import _SEMANTIC_GROUP_CONFIG
		sem_cfg = _SEMANTIC_GROUP_CONFIG.get(doctype)
	except Exception:
		sem_cfg = None
	table = f'tab{doctype}'
	total_records = 0
	buffer_rows: List[Dict[str, Any]] = []
	meta_cache: Dict[str, Any] = {}

	def flush() -> int:
		if not buffer_rows:
			return 0
		upserted = _upsert_rows(buffer_rows)
		buffer_rows.clear()
		return upserted

	try:
		print(f"[{doctype}] Starting pgvector upsert...", flush=True)
		if sem_cfg:
			group_field, _ = sem_cfg
			query = f'SELECT * FROM "{table}" WHERE docstatus < 2' + (' AND modified >= %s' if since else '') + f' ORDER BY "{group_field}", name'
		else:
			query = f'SELECT * FROM "{table}" WHERE docstatus < 2' + (' AND modified >= %s' if since else '') + ' ORDER BY name'
		params = [since] if since else []
		rows = execute_remote_query_paginated(query, tuple(params) if params else None)
		current_group: List[Dict[str, Any]] = []
		current_key: Optional[str] = None
		total_vectors = 0
		for row in rows:
			total_records += 1
			if total_records == 1 or total_records % 10 == 0:
				print(
					f"\r[{doctype}] records={total_records}, vectors={total_vectors + len(buffer_rows)}...",
					end="",
					flush=True,
				)
			if sem_cfg:
				group_field, max_size = sem_cfg
				row_key = str(row.get(group_field) or "__null__")
				if current_key != row_key or len(current_group) >= max_size:
					if current_group:
						text = "\n\n---\n\n".join(_record_to_text(doctype, item, meta_cache=meta_cache) for item in current_group)
						buffer_rows.append({
							"id": f"{doctype}:{str(current_group[0].get('name'))}".encode("ascii", "ignore").decode("ascii"),
							"namespace": doctype,
							"doctype": doctype,
							"record_ids": json.dumps([str(item.get("name")) for item in current_group]),
							"count": len(current_group),
							"context_preview": text[:5000],
							"grade": current_group[0].get("grade"),
							"batch": current_group[0].get("batch"),
							"course": current_group[0].get("course"),
							"content_type": current_group[0].get("content_type"),
							"embedding": _vector_literal(embed_documents_cached([text])[0]),
							"source_backend": "pgvector",
						})
						if len(buffer_rows) >= embed_batch:
							total_vectors += flush()
					current_group = []
					current_key = row_key
				current_group.append(row)
				continue
			buffer_rows.append({
				"id": f"{doctype}:{str(row.get('name'))}".encode("ascii", "ignore").decode("ascii"),
				"namespace": doctype,
				"doctype": doctype,
				"record_ids": json.dumps([str(row.get("name"))]),
				"count": 1,
				"context_preview": _record_to_text(doctype, row, meta_cache=meta_cache)[:5000],
				"grade": row.get("grade"),
				"batch": row.get("batch"),
				"course": row.get("course"),
				"content_type": row.get("content_type"),
				"embedding": _vector_literal(embed_documents_cached([_record_to_text(doctype, row, meta_cache=meta_cache)])[0]),
				"source_backend": "pgvector",
			})
			if len(buffer_rows) >= embed_batch:
				total_vectors += flush()
		if sem_cfg and current_group:
			text = "\n\n---\n\n".join(_record_to_text(doctype, item, meta_cache=meta_cache) for item in current_group)
			buffer_rows.append({
				"id": f"{doctype}:{str(current_group[0].get('name'))}".encode("ascii", "ignore").decode("ascii"),
				"namespace": doctype,
				"doctype": doctype,
				"record_ids": json.dumps([str(item.get("name")) for item in current_group]),
				"count": len(current_group),
				"context_preview": text[:5000],
				"grade": current_group[0].get("grade"),
				"batch": current_group[0].get("batch"),
				"course": current_group[0].get("course"),
				"content_type": current_group[0].get("content_type"),
				"embedding": _vector_literal(embed_documents_cached([text])[0]),
				"source_backend": "pgvector",
			})
		if buffer_rows:
			total_vectors += flush()
		frappe.cache().set(f"upsert_timestamp:{doctype}", datetime.now().isoformat())
		print("", flush=True)
		print(f"[{doctype}] records={total_records}, vectors={total_vectors} complete", flush=True)
		print(f"[{doctype}] [ok] records={total_records}, vectors={total_vectors}", flush=True)
		return {"doctype": doctype, "records_seen": total_records, "vectors_upserted": total_vectors, "backend": "pgvector"}
	except Exception as e:
		print(f"Error fetching remote data for {doctype}: {e}")
		return {"doctype": doctype, "records_seen": total_records, "vectors_upserted": 0, "backend": "pgvector", "error": str(e)}


def upsert_all(doctypes: Optional[List[str]] = None, since: Optional[str] = None) -> Dict[str, Any]:
	if doctypes is None:
		schema = load_schema()
		doctypes = [table.replace("tab", "") for table in schema.get("allowlist", [])]
	total = len(doctypes)
	out: Dict[str, Any] = {}
	print(f"\n Starting pgvector upsert for {total} DocTypes...\n", flush=True)
	for index, doctype in enumerate(doctypes, 1):
		print(f"[{index}/{total}] Processing: {doctype} ...", end="", flush=True)
		try:
			result = upsert_doctype(doctype, since=since)
			out[doctype] = result
			if result.get("error"):
				print(
					f"\r[{index}/{total}] [error] {doctype:<30} ERROR: {result['error']}",
					flush=True,
				)
			else:
				print(
					f"\r[{index}/{total}] [ok] {doctype:<30} records={result['records_seen']}, vectors={result['vectors_upserted']}",
					flush=True,
				)
		except Exception as e:
			out[doctype] = {"error": str(e)}
			print(f"\r[{index}/{total}] [error] {doctype:<30} ERROR: {e}", flush=True)
			frappe.log_error(f"Upsert failed for {doctype}", str(e))
	print(f"\nDone. Processed {total} DocTypes.\n", flush=True)
	return out


def cli_upsert_all(
	doctypes: Optional[List[str]] = None,
	since: Optional[str] = None,
) -> Dict[str, Any]:
	"""Bench entry point for bulk upserting allowlisted DocTypes into pgvector.

	Example:
	bench execute tap_ai.services.rag.pgvector_store.cli_upsert_all
	bench execute tap_ai.services.rag.pgvector_store.cli_upsert_all --kwargs "{'doctypes': ['VideoClass']}"
	"""
	result = upsert_all(doctypes=doctypes, since=since)
	print(frappe.as_json(result, indent=2))
	return result


def upsert_kb_entries() -> Dict[str, Any]:
	entries = get_direct_response_entries(force_refresh=True)
	active = [entry for entry in entries if entry.get("is_active", 1)]
	if not active:
		return {"upserted": 0, "backend": "pgvector"}
	rows = []
	for entry in active:
		alt_list = _parse_aliases(entry.get("alternate_queries") or "")
		alt_str = "\n".join(alt_list) if alt_list else ""
		text = f"Student query: {entry.get('student_query', '')}\nAlternate queries: {alt_str}\nResponse: {entry.get('response', '')}"
		rows.append({
			"id": f"{_KB_NAMESPACE}:{entry.get('name', '')}".encode("ascii", "ignore").decode("ascii"),
			"namespace": _KB_NAMESPACE,
			"doctype": _KB_NAMESPACE,
			"record_ids": json.dumps([entry.get("name", "")]),
			"count": 1,
			"context_preview": text[:5000],
			"grade": None,
			"batch": None,
			"course": None,
			"content_type": entry.get("category", ""),
			"embedding": _vector_literal(embed_documents_cached([text])[0]),
			"source_backend": "pgvector",
		})
	return {"upserted": _upsert_rows(rows), "backend": "pgvector"}


def sync_kb_entry_to_pgvector(doc, method):
	try:
		frappe.enqueue(
			"tap_ai.services.rag.pgvector_store._do_sync_kb_entry",
			name=doc.name,
			is_active=int(getattr(doc, "is_active", 1) or 0),
			student_query=doc.student_query or "",
			alternate_queries=str(doc.alternate_queries or ""),
			response=doc.response or "",
			category=str(getattr(doc, "category", "") or ""),
			queue="long",
			job_name=f"kb_pgvector_sync_{doc.name}",
		)
	except Exception as e:
		frappe.log_error(f"[kb] Failed to queue pgvector sync for {doc.name}: {e}")


def _do_sync_kb_entry(name, is_active, student_query, alternate_queries, response, category=""):
	safe_id = f"{_KB_NAMESPACE}:{name}".encode("ascii", "ignore").decode("ascii")
	_ensure_schema()
	if not is_active:
		with get_remote_connection() as conn:
			with conn.cursor() as cursor:
				cursor.execute(f'DELETE FROM "{_VECTOR_TABLE}" WHERE id = %s', (safe_id,))
			conn.commit()
		return
	alt_list = _parse_aliases(alternate_queries)
	alt_str = "\n".join(alt_list) if alt_list else ""
	text = f"Student query: {student_query}\nAlternate queries: {alt_str}\nResponse: {response}"
	payload = {
		"id": safe_id,
		"namespace": _KB_NAMESPACE,
		"doctype": _KB_NAMESPACE,
		"record_ids": json.dumps([name]),
		"count": 1,
		"context_preview": text[:5000],
		"grade": None,
		"batch": None,
		"course": None,
		"content_type": category,
		"embedding": _vector_literal(embed_documents_cached([text])[0]),
		"source_backend": "pgvector",
	}
	_upsert_rows([payload])


def delete_kb_entry_from_pgvector(doc, method):
	try:
		safe_id = f"{_KB_NAMESPACE}:{doc.name}".encode("ascii", "ignore").decode("ascii")
		with get_remote_connection() as conn:
			with conn.cursor() as cursor:
				cursor.execute(f'DELETE FROM "{_VECTOR_TABLE}" WHERE id = %s', (safe_id,))
			conn.commit()
	except Exception as e:
		frappe.log_error(f"[kb] pgvector delete failed for {doc.name}: {e}")


def sync_to_pgvector_on_insert(doc, method):
	try:
		frappe.enqueue("tap_ai.services.rag.pgvector_store.upsert_doctype", doctype=doc.doctype, since=doc.creation, queue="long", job_name=f"pgvector_insert_{doc.doctype}_{doc.name}")
	except Exception as e:
		frappe.log_error(f"Failed to queue pgvector sync on insert for {doc.doctype}:{doc.name}: {e}")


def sync_to_pgvector_on_update(doc, method):
	try:
		frappe.enqueue("tap_ai.services.rag.pgvector_store.upsert_doctype", doctype=doc.doctype, since=doc.modified, queue="long", job_name=f"pgvector_update_{doc.doctype}_{doc.name}")
	except Exception as e:
		frappe.log_error(f"Failed to queue pgvector sync on update for {doc.doctype}:{doc.name}: {e}")


def cli_compare_backends(
	queries: Optional[List[str]] = None,
	k: int = 6,
	route_top_n: int = 4,
) -> Dict[str, Any]:
	"""Compare Pinecone and pgvector search latency and top-K overlap.

	Example:
	bench execute tap_ai.services.rag.pgvector_store.cli_compare_backends \
	  --kwargs "{'queries': ['What is financial literacy?', 'Summarize the arts activity on Zentangle'], 'k': 5}"
	"""
	from tap_ai.services.rag.pinecone_store import search_auto_namespaces as search_pinecone

	default_queries = [
		"What is financial literacy?",
		"Summarize the arts activity on Zentangle",
		"Explain the course about goal setting",
	]
	queries = queries or default_queries
	results: List[Dict[str, Any]] = []
	total_queries = len(queries)

	for q_index, query in enumerate(queries, 1):
		print(f"[compare] Query {q_index}/{total_queries}: {query}", flush=True)
		row: Dict[str, Any] = {"query": query}
		for backend_name, search_fn in (("pinecone", search_pinecone), ("pgvector", search_auto_namespaces)):
			print(f"[compare]   running {backend_name}...", flush=True)
			t0 = time.perf_counter()
			search_result = search_fn(q=query, k=k, route_top_n=route_top_n)
			elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
			matches = search_result.get("matches") or []
			match_keys = [f"{match.get('namespace')}::{match.get('id')}" for match in matches]
			row[backend_name] = {
				"ms": elapsed_ms,
				"routed_doctypes": search_result.get("routed_doctypes") or [],
				"top_ids": match_keys,
				"top_scores": [round(float(match.get("score") or 0), 4) for match in matches],
				"search_mode": search_result.get("search_mode"),
				"namespace_timings_ms": search_result.get("namespace_timings_ms") or {},
			}
			print(f"[compare]   done {backend_name}: {elapsed_ms} ms", flush=True)
		extra = set(row["pinecone"]["top_ids"]) ^ set(row["pgvector"]["top_ids"])
		row["overlap"] = {
			"shared_top_ids": len(set(row["pinecone"]["top_ids"]) & set(row["pgvector"]["top_ids"])),
			"pinecone_only": len(set(row["pinecone"]["top_ids"]) - set(row["pgvector"]["top_ids"])),
			"pgvector_only": len(set(row["pgvector"]["top_ids"]) - set(row["pinecone"]["top_ids"])),
			"symmetric_diff": len(extra),
		}
		results.append(row)

	output = {"k": k, "route_top_n": route_top_n, "results": results}
	print(frappe.as_json(output, indent=2))
	return output
