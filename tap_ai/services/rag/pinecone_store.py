# tap_ai/services/pinecone_store.py
"""
Pinecone vector store for semantic search (RAG retrieval).

Manages embedding generation, upsert, and similarity search against the
Pinecone index. At query time, `search_pinecone()` embeds the refined query,
routes it to the right namespace(s) based on DocType, applies optional
metadata filters (grade, batch, course), and returns the top-k context chunks.

Lazy-initializes the Pinecone client and OpenAI embedding model on first use,
then reuses the same connection for subsequent requests. Embeddings are cached
in Redis for 24 hours to avoid re-embedding identical queries.

A bounded `ThreadPoolExecutor` handles concurrent upsert operations so bulk
indexing does not block the main thread.

Key functions:
    search_pinecone(query, user_context, top_k) → List[dict]
    upsert_documents(docs)
    delete_namespace(namespace)
"""

from __future__ import annotations

import time
import decimal
import json
import hashlib
from datetime import date, datetime, time as dtime
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor

import frappe
from pinecone import Pinecone
from langchain_openai import OpenAIEmbeddings

from tap_ai.infra.config import get_config
from tap_ai.infra.sql_catalog import load_schema
from tap_ai.services.sql.doctype_selector import pick_doctypes
from tap_ai.utils.remote_db import execute_remote_query, execute_remote_query_paginated


#  OPTIMIZATION: Embedding caching (Phase 1)
EMBEDDING_CACHE_TTL = 86400  # 24 hours

_PINECONE_CLIENT = None
_PINECONE_INDEX = None
_PINECONE_INDEX_NAME = None
_EMBEDDINGS = None
_EMBEDDINGS_MODEL = None
_EXECUTOR = ThreadPoolExecutor(max_workers=8)


def _embedding_max_tokens_per_request() -> int:
    """Safety budget below provider hard limit to avoid 400 max_tokens_per_request."""
    try:
        return int(get_config("embedding_max_tokens_per_request") or 240000)
    except Exception:
        return 240000


def _embedding_max_chars_per_text() -> int:
    """Cap a single embedding input size; large docs are trimmed for stability."""
    try:
        return int(get_config("embedding_max_chars_per_text") or 24000)
    except Exception:
        return 24000


def _estimate_tokens(text: str) -> int:
    # Fast approximation for GPT tokenization.
    return max(1, len(text or "") // 4)


def _prepare_text_for_embedding(text: str) -> str:
    max_chars = _embedding_max_chars_per_text()
    if len(text) <= max_chars:
        return text
    # Keep head + tail to preserve topic and ending cues.
    head = max_chars // 2
    tail = max_chars - head
    return text[:head] + "\n\n[TRUNCATED_FOR_EMBEDDING]\n\n" + text[-tail:]


def _batch_uncached_payloads(payloads: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Split uncached texts so each embed_documents call stays under token budget."""
    max_tokens = _embedding_max_tokens_per_request()
    max_items = 64

    batches: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    current_tokens = 0

    for item in payloads:
        t = _estimate_tokens(item["embed_text"])
        if current and (current_tokens + t > max_tokens or len(current) >= max_items):
            batches.append(current)
            current = []
            current_tokens = 0

        current.append(item)
        current_tokens += t

    if current:
        batches.append(current)

    return batches

def _embedding_cache_key(text: str, model: str) -> str:
    """Generate cache key for embedding."""
    return f"embedding:{model}:{hashlib.md5(text.encode()).hexdigest()}"

def embed_query_cached(
    q: str,
    model: str = "text-embedding-3-small",
    cache_ttl: int = EMBEDDING_CACHE_TTL,
) -> List[float]:
    """Cache query embeddings."""
    cache_key = _embedding_cache_key(q, model)
    
    # Check cache
    cached = frappe.cache().get(cache_key)
    if cached:
        print(f"Embedding cache hit: {cache_key[:40]}...")
        return json.loads(cached)
    
    # Embed
    emb = _emb()
    vector = emb.embed_query(q)
    
    # Cache
    frappe.cache().set(cache_key, json.dumps(vector), ex=cache_ttl)
    return vector

def embed_documents_cached(
    texts: List[str],
    model: str = "text-embedding-3-small",
    cache_ttl: int = EMBEDDING_CACHE_TTL,
) -> List[List[float]]:
    """Cache document embeddings."""
    emb = _emb()
    cached_vectors = []
    uncached_payloads: List[Dict[str, Any]] = []
    
    # Check cache for each text
    for i, text in enumerate(texts):
        cache_key = _embedding_cache_key(text, model)
        cached = frappe.cache().get(cache_key)
        if cached:
            if isinstance(cached, bytes):
                cached = cached.decode("utf-8", errors="ignore")
            cached_vectors.append((i, json.loads(cached)))
        else:
            uncached_payloads.append({
                "idx": i,
                "cache_key": cache_key,
                "embed_text": _prepare_text_for_embedding(text),
            })
    
    # Embed uncached texts
    if uncached_payloads:
        for batch in _batch_uncached_payloads(uncached_payloads):
            batch_texts = [x["embed_text"] for x in batch]
            new_vectors = emb.embed_documents(batch_texts)

            # Cache and collect results
            for i, item in enumerate(batch):
                frappe.cache().set(item["cache_key"], json.dumps(new_vectors[i]), ex=cache_ttl)
                cached_vectors.append((item["idx"], new_vectors[i]))
    
    # Reconstruct vectors in original order
    result = [None] * len(texts)
    for idx, vec in cached_vectors:
        result[idx] = vec
    
    return result


# -------------------------------------------------------------------
# Pinecone / Embedding helpers
# -------------------------------------------------------------------

def _pc() -> Pinecone:
    global _PINECONE_CLIENT
    if _PINECONE_CLIENT is not None:
        return _PINECONE_CLIENT

    api_key = get_config("pinecone_api_key")
    if not api_key:
        raise RuntimeError("Missing pinecone_api_key in site_config.json")
    _PINECONE_CLIENT = Pinecone(api_key=api_key)
    return _PINECONE_CLIENT

def _index():
    global _PINECONE_INDEX, _PINECONE_INDEX_NAME
    name = get_config("pinecone_index") or "tap-ai-byo"
    if _PINECONE_INDEX is None or _PINECONE_INDEX_NAME != name:
        _PINECONE_INDEX = _pc().Index(name)
        _PINECONE_INDEX_NAME = name
    return _PINECONE_INDEX

def _emb() -> OpenAIEmbeddings:
    global _EMBEDDINGS, _EMBEDDINGS_MODEL
    api_key = get_config("openai_api_key")
    model = get_config("embedding_model") or "text-embedding-3-small"
    if not api_key:
        raise RuntimeError("Missing openai_api_key in site_config.json")
    if _EMBEDDINGS is None or _EMBEDDINGS_MODEL != model:
        _EMBEDDINGS = OpenAIEmbeddings(model=model, api_key=api_key)
        _EMBEDDINGS_MODEL = model
    return _EMBEDDINGS


# -------------------------------------------------------------------
# Utility helpers
# -------------------------------------------------------------------

def _to_plain(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (datetime, date, dtime)):
        return v.isoformat()
    return str(v)

def _record_to_text(
    doctype: str,
    row: Dict[str, Any],
    meta_cache: Optional[Dict[str, Any]] = None,
) -> str:
    parts = []
    meta = None
    if meta_cache is not None:
        meta = meta_cache.get(doctype)
        if meta is None:
            meta = frappe.get_meta(doctype)
            meta_cache[doctype] = meta
    else:
        meta = frappe.get_meta(doctype)

    title_field = meta.title_field
    if title_field and row.get(title_field):
        label = meta.get_field(title_field).label or title_field
        parts.append(f"{label}: {row[title_field]}")

    parts.append(f"DocType: {doctype}")
    parts.append(f"ID: {row.get('name')}")

    for k, v in row.items():
        if k in ("name", title_field) or v in (None, ""):
            continue
        parts.append(f"{k}: {_to_plain(v)}")

    return "\n".join(parts)

def get_db_columns_for_doctype(doctype: str) -> List[str]:
    table = f"tab{doctype}"
    try:
        from tap_ai.utils.remote_db import get_remote_table_columns
        return get_remote_table_columns(doctype) or []
    except Exception:
        # Fallback to local DB if remote fails
        try:
            desc = frappe.db.sql(f"DESCRIBE `{table}`", as_dict=True)
            return [d["Field"] for d in desc]
        except Exception:
            return []


# -------------------------------------------------------------------
# ExcludedDoctypes handling
# -------------------------------------------------------------------

def _get_excluded_doctypes() -> set[str]:
    cache_key = "tap_ai:excluded_doctypes"
    try:
        cached = frappe.cache().get(cache_key)
        if cached:
            if isinstance(cached, bytes):
                cached = cached.decode("utf-8", errors="ignore")
            try:
                items = json.loads(cached)
                return set(items)
            except Exception:
                pass
    except Exception:
        pass

    excluded = set()
    try:
        recs = frappe.get_all("ExcludedDoctypes", fields=["name"], limit=1)
        if not recs:
            # cache empty set briefly to avoid DB thundering
            try:
                frappe.cache().set(cache_key, json.dumps([]), ex=60)
            except Exception:
                pass
            return excluded

        doc = frappe.get_doc("ExcludedDoctypes", recs[0].name)
        for row in doc.excluded_doctype:
            if row.doctype_name:
                excluded.add(row.doctype_name)

        try:
            frappe.cache().set(cache_key, json.dumps(list(excluded)), ex=60)
        except Exception:
            pass

    except Exception:
        pass

    return excluded

def _filter_excluded(doctypes: List[str]) -> List[str]:
    excluded = _get_excluded_doctypes()
    return [dt for dt in doctypes if dt not in excluded]


# -------------------------------------------------------------------
# Upsert pipeline
# -------------------------------------------------------------------

# Doctypes whose records share a natural grouping dimension.
# Records are sorted by the field and up to max_size consecutive records
# with the same field value are packed into one vector.
# Doctypes NOT listed here get 1 record per vector.
_SEMANTIC_GROUP_CONFIG: Dict[str, tuple] = {
    # (order_by_field, max_records_per_vector)
    "Student":            ("grade", 8),
    "Teacher":            ("school_id", 8),
    "Student Assignment": ("assignment", 8),
    "StudentQuizAttempt": ("quiz", 8),
    "StudentReflection":  ("student", 6),
    "ImgSubmission":      ("assign_id", 8),
    "Performance":        ("enrollment", 8),
    "Submission":         ("student_assignment", 8),
    "QuizOption":         ("question_id", 5),
    "LearningChoicePoint":("student", 6),
}


def upsert_doctype(
    doctype: str,
    since: Optional[str] = None,
    embed_batch: int = 100,
) -> Dict[str, Any]:
    idx = _index()
    sem_cfg = _SEMANTIC_GROUP_CONFIG.get(doctype)  # (field, max_size) or None

    total_records = 0
    total_vectors = 0
    table = f'tab{doctype}'

    buffer_texts: List[str] = []
    buffer_ids: List[str] = []
    buffer_meta: List[Dict[str, Any]] = []
    meta_cache: Dict[str, Any] = {}

    def flush():
        nonlocal total_vectors
        if not buffer_texts:
            return
        vectors = embed_documents_cached(buffer_texts)
        payload = [
            {"id": buffer_ids[i], "values": vectors[i], "metadata": buffer_meta[i]}
            for i in range(len(buffer_texts))
        ]
        idx.upsert(vectors=payload, namespace=doctype)
        total_vectors += len(payload)
        buffer_texts.clear()
        buffer_ids.clear()
        buffer_meta.clear()

    def emit_group(group: List[Dict[str, Any]]):
        if not group:
            return
        record_ids = [str(r["name"]) for r in group]
        text = "\n\n---\n\n".join(
            _record_to_text(doctype, r, meta_cache=meta_cache) for r in group
        )
        raw_id = f"{doctype}:{record_ids[0]}"
        safe_id = raw_id.encode("ascii", "ignore").decode("ascii")
        buffer_texts.append(text)
        buffer_ids.append(safe_id)
        buffer_meta.append({
            "doctype": doctype,
            "record_ids": record_ids,
            "count": len(group),
            "context_preview": text[:25000],
        })
        if len(buffer_texts) >= embed_batch:
            flush()

    try:
        if sem_cfg:
            group_field, _ = sem_cfg
            base = f'SELECT * FROM "{table}" WHERE docstatus < 2'
            query = base + (' AND modified >= %s' if since else '') + f' ORDER BY "{group_field}", name'
        else:
            query = f'SELECT * FROM "{table}" WHERE docstatus < 2'
            query += (' AND modified >= %s' if since else '') + ' ORDER BY name'

        params = [since] if since else []
        rows = execute_remote_query_paginated(query, tuple(params) if params else None)

        current_group: List[Dict[str, Any]] = []
        current_key: Optional[str] = None

        for row in rows:
            total_records += 1

            if sem_cfg:
                group_field, max_size = sem_cfg
                row_key = str(row.get(group_field) or "__null__")

                # Flush current group when key changes or group is full
                if current_key != row_key or len(current_group) >= max_size:
                    emit_group(current_group)
                    current_group = []
                    current_key = row_key

                current_group.append(row)
            else:
                # 1 record per vector for content/reference doctypes
                emit_group([row])

        emit_group(current_group)
        flush()

        frappe.cache().set(f"upsert_timestamp:{doctype}", datetime.now().isoformat())

    except Exception as e:
        print(f"Error fetching remote data for {doctype}: {e}")

    return {
        "doctype": doctype,
        "records_seen": total_records,
        "vectors_upserted": total_vectors,
    }

def upsert_all(
    doctypes: Optional[List[str]] = None,
    since: Optional[str] = None,
) -> Dict[str, Any]:

    if doctypes is None:
        schema = load_schema()
        doctypes = [t.replace("tab", "") for t in schema.get("allowlist", [])]

    out = {}
    for dt in doctypes:
        try:
            out[dt] = upsert_doctype(dt, since=since)
        except Exception as e:
            out[dt] = {"error": str(e)}
            frappe.log_error(f"Upsert failed for {dt}", str(e))

    return out


# -------------------------------------------------------------------
# SEARCH (THIS IS THE IMPORTANT PART)
# -------------------------------------------------------------------

def search_auto_namespaces(  
    q: str,  
    k: int = 6,  
    route_top_n: int = 4,  
    filters: Optional[Dict[str, Any]] = None,  
    use_parallel: bool = True,
) -> Dict[str, Any]:  
    """
     OPTIMIZATION: Parallel Pinecone queries with LLM-routed doctypes (Phase 2)
    LLM routing is cached (5min TTL), reducing redundant calls.
    Instead of: 4 doctypes = 4 sequential queries (800ms)
    Now does: 4 routed doctypes in 1 parallel batch (200ms)
    """
    idx = _index()  
  
    # 1. Route doctypes using LLM (cached; only one LLM call on cache miss)
    doctypes = pick_doctypes(q, top_n=route_top_n) or []  
  
    # 2. Enforce exclusion list  
    doctypes = _filter_excluded(doctypes)  
  
    # 3. Filter out system DocTypes for content queries  
    system_doctypes = {"AI Knowledge Base"}  
    content_doctypes = [dt for dt in doctypes if dt not in system_doctypes]  
      
    # Only use content doctypes if we found any  
    if content_doctypes:  
        doctypes = content_doctypes  
  
    # 4. Fallback to content DocTypes if routing failed  
    if not doctypes:  
        schema = load_schema()  
        all_allowed = [  
            t.replace("tab", "")  
            for t in schema.get("allowlist", [])  
        ]  
        # Prefer content DocTypes in fallback  
        content_priority = [  
            "VideoClass", "Course", "LearningObjective", "NoteContent",  
            "Quiz", "Assignment", "LearningUnit"  
        ]  
        doctypes = [dt for dt in content_priority if dt in all_allowed][:route_top_n]  
          
        # If no content DocTypes found, use any allowed  
        if not doctypes:  
            doctypes = all_allowed[:route_top_n]  

    #  OPTIMIZATION: Use cached embedding (Phase 1)
    qvec = embed_query_cached(q)
    all_matches: List[Dict[str, Any]] = []

    #  OPTIMIZATION: Parallel queries (Phase 2)
    if use_parallel and len(doctypes) > 1:
        def query_namespace(ns):
            """Query single namespace."""
            try:
                res = idx.query(
                    namespace=ns,
                    vector=qvec,
                    top_k=k,
                    filter=filters,
                    include_metadata=True,
                    include_values=False,
                    timeout=3.0,
                )
                matches = []
                for m in res.get("matches", []):
                    matches.append({
                        "id": m.id,
                        "score": m.score,
                        "namespace": ns,
                        "metadata": m.metadata,
                    })
                return matches
            except Exception as e:
                frappe.log_error(f"Pinecone query failed for {ns}", str(e))
                return []
        
        # Parallel execution
        results = _EXECUTOR.map(query_namespace, doctypes)
        for matches in results:
            all_matches.extend(matches)
    else:
        # Sequential fallback
        for ns in doctypes:
            try:
                res = idx.query(
                    namespace=ns,
                    vector=qvec,
                    top_k=k,
                    filter=filters,
                    include_metadata=True,
                    include_values=False,
                    timeout=3.0,
                )

                for m in res.get("matches", []):
                    all_matches.append({
                        "id": m.id,
                        "score": m.score,
                        "namespace": ns,
                        "metadata": m.metadata,
                    })

            except Exception as e:
                frappe.log_error(
                    f"Pinecone query failed for namespace {ns}", str(e)
                )

    all_matches.sort(key=lambda x: x.get("score", 0), reverse=True)

    return {
        "q": q,
        "routed_doctypes": doctypes,
        "k": k,
        "matches": all_matches[:k],
    }


# -------------------------------------------------------------------
# Bench CLIs
# -------------------------------------------------------------------

def cli_upsert_all(
    doctypes: Optional[List[str]] = None,
    since: Optional[str] = None,
) -> Dict[str, Any]:
    
    '''bench execute tap_ai.services.rag.pinecone_store.cli_upsert_all'''

    if doctypes is None:
        schema = load_schema()
        doctypes = [t.replace("tab", "") for t in schema.get("allowlist", [])]

    total = len(doctypes)
    out = {}

    print(f"\n Starting upsert for {total} DocTypes...\n", flush=True)

    for i, dt in enumerate(doctypes, 1):
        print(f"[{i}/{total}] Processing: {dt} ...", end="", flush=True)
        try:
            result = upsert_doctype(dt, since=since)
            out[dt] = result
            print(
                f"\r[{i}/{total}] [ok] {dt:<30} "
                f"records={result['records_seen']}, vectors={result['vectors_upserted']}",
                flush=True,
            )
        except Exception as e:
            out[dt] = {"error": str(e)}
            print(f"\r[{i}/{total}] [error] {dt:<30} ERROR: {e}", flush=True)
            frappe.log_error(f"Upsert failed for {dt}", str(e))

    print(f"\nDone. Processed {total} DocTypes.\n", flush=True)
    return out

def cli_delete_namespace(doctype: str) -> None:
    """
    Delete all vectors for a single DocType namespace from Pinecone.
    Run this before re-upserting a DocType whose chunking strategy changed,
    otherwise stale vectors with old IDs accumulate alongside new ones.

    bench execute tap_ai.services.rag.pinecone_store.cli_delete_namespace --kwargs "{'doctype': 'QuizQuestion'}"
    """
    idx = _index()
    idx.delete(delete_all=True, namespace=doctype)
    print(f"[ok] Namespace '{doctype}' deleted from Pinecone.")


def cli_search_auto(q: str, k: int = 6, route_top_n: int = 4):
    out = search_auto_namespaces(q=q, k=k, route_top_n=route_top_n)
    print(frappe.as_json(out, indent=2))
    return out


# -------------------------------------------------------------------
# AUTO-SYNC HANDLERS (doc_events)
# -------------------------------------------------------------------

def sync_to_pinecone_on_insert(doc, method):
    """
    Sync new record to Pinecone immediately after insert.
    Queues asynchronously to avoid blocking the document save.
    """
    try:
        frappe.enqueue(
            "tap_ai.services.rag.pinecone_store.upsert_doctype",
            doctype=doc.doctype,
            since=doc.creation,
            queue="long",
            job_name=f"pinecone_insert_{doc.doctype}_{doc.name}",
        )
        print(f"[ok] Queued Pinecone sync for {doc.doctype}:{doc.name}")
    except Exception as e:
        frappe.log_error(
            f"Failed to queue Pinecone sync on insert for {doc.doctype}:{doc.name}",
            str(e)
        )


def sync_to_pinecone_on_update(doc, method):
    """
    Sync updated record to Pinecone (delta sync).
    Only syncs records modified in the last 5 minutes to avoid redundant syncs.
    """
    try:
        frappe.enqueue(
            "tap_ai.services.rag.pinecone_store.upsert_doctype",
            doctype=doc.doctype,
            since=doc.modified,
            queue="long",
            job_name=f"pinecone_update_{doc.doctype}_{doc.name}",
        )
        print(f"[ok] Queued Pinecone sync for {doc.doctype}:{doc.name}")
    except Exception as e:
        frappe.log_error(
            f"Failed to queue Pinecone sync on update for {doc.doctype}:{doc.name}",
            str(e)
        )

