"""
DocType Routing Profiler

For each DocType in the allowlist:
  1. Collect schema metadata (field names, labels, Select options) from Frappe meta
  2. Fetch all record titles from the remote DB
  3. One-time LLM call → 2-3 sentence summary of what the DocType stores
  4. Embed the summary using the existing embedding infrastructure
  5. Persist {summary, vector} to the DoctypeRoutingProfile Frappe doctype (survives Redis flush)
  6. Cache in Redis for fast repeated access

At query time:
  - Reuse the already-computed Pinecone query embedding (zero extra cost)
  - Cosine similarity vs all stored DocType vectors → top-N namespaces to search

Auto-refresh:
  - doc_events hook fires on any allowlisted DocType save → background job regenerates that DocType's profile

CLI bootstrap:
  bench execute tap_ai.services.routing.doctype_profiler.generate_all_profiles
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import frappe
from loguru import logger

from tap_ai.infra.config import get_config
from tap_ai.infra.llm_client import llm_invoke_cached
from tap_ai.infra.sql_catalog import load_schema
from tap_ai.utils.remote_db import execute_remote_query


# ── Redis keys ────────────────────────────────────────────────────────────────

_REDIS_PREFIX = "tap_ai:doctype_profile:"
_REDIS_TTL = 86400 * 7          # 7 days — refreshed by hook on any DB change
_PROFILE_DOCTYPE = "Doctype Routing Profile"

_PROFILE_SYSTEM = (
    "You are writing routing descriptions for an AI that must decide which database table "
    "to search when a user asks a question. Your description must be grounded in the ACTUAL "
    "record titles and content shown — not the field names. "
    "Name real topics, subjects, and categories you see in the data. Do not describe field structure."
)


# ── Schema context ─────────────────────────────────────────────────────────────

def _build_schema_context(doctype: str) -> str:
    """Build a readable description of the DocType's fields from Frappe meta."""
    try:
        meta = frappe.get_meta(doctype)
        skip_types = {"Section Break", "Column Break", "HTML", "Button", "Fold", "Heading"}
        lines = [f"DocType: {doctype}"]
        for field in meta.fields:
            if field.fieldtype in skip_types:
                continue
            line = f"  - {field.label} ({field.fieldtype})"
            if field.fieldtype == "Select" and field.options:
                opts = ", ".join(field.options.strip().splitlines())
                line += f": [{opts}]"
            elif field.fieldtype == "Link" and field.options:
                line += f" → {field.options}"
            lines.append(line)
        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"Could not read meta for {doctype}: {e}")
        return f"DocType: {doctype}"


# ── Remote DB title fetch ──────────────────────────────────────────────────────

_CONTENT_FIELDS = {"subject", "topic", "grade", "category", "course", "batch", "vertical", "unit"}


def _fetch_all_titles(doctype: str) -> List[str]:
    """
    Fetch record titles plus any subject/topic/grade fields from the remote DB.
    Combining multiple content fields gives the LLM richer material to extract
    actual topics from, rather than just generic names.
    Capped at 2000 rows.
    """
    try:
        meta = frappe.get_meta(doctype)
        title_field = meta.title_field
        table = f'tab{doctype}'

        # Collect fields to SELECT: title + any content-rich fields present in this DocType.
        # Cross-check against actual DB columns — meta and remote DB can diverge.
        from tap_ai.services.rag.pinecone_store import get_db_columns_for_doctype
        meta_fields = {f.fieldname for f in meta.fields}
        db_columns = set(get_db_columns_for_doctype(doctype) or [])
        extra = sorted(_CONTENT_FIELDS & meta_fields & db_columns)

        select_fields = ["name"]
        if title_field and title_field != "name":
            select_fields.append(f'"{title_field}"')
        for f in extra:
            select_fields.append(f'"{f}"')

        sql = f'SELECT {", ".join(select_fields)} FROM "{table}" WHERE docstatus < 2 LIMIT 2000'
        rows = execute_remote_query(sql)

        entries = []
        for row in rows:
            parts = []
            # Primary: title or name
            primary = row.get(title_field) or row.get("name") if title_field else row.get("name")
            if primary:
                parts.append(str(primary).strip())
            # Extra content fields as "field: value" annotations
            for f in extra:
                val = row.get(f)
                if val:
                    parts.append(f"{f}:{val}")
            if parts:
                entries.append(" | ".join(parts))

        return [e for e in entries if e]

    except Exception as e:
        frappe.log_error(f"[profiler] Failed to fetch titles for {doctype}: {e}")
        return []


# ── LLM summary ────────────────────────────────────────────────────────────────

def _generate_summary(doctype: str, schema_text: str, titles: List[str]) -> str:
    """
    Call LLM once to produce a topic-aware routing summary for this DocType.
    The summary must name actual subjects/topics from the data so cosine
    similarity can match user queries to the right namespace.
    """
    sample = titles[:300]
    titles_block = "\n".join(f"- {t}" for t in sample) if sample else "No records found."

    user_msg = (
        f"{schema_text}\n\n"
        f"Actual records ({len(titles)} total, showing {len(sample)}):\n"
        f"{titles_block}\n\n"
        "Write a 3-4 sentence routing description. Rules:\n"
        "- Mention the ACTUAL topics, subjects, or categories visible in the records above (e.g. 'financial literacy', 'goal setting', 'grade 8'). Be specific.\n"
        "- State what kinds of user questions this table can answer.\n"
        "- Mention grade levels, difficulty levels, or course names if present in the data.\n"
        "- Do NOT just list field names. Ground every claim in the actual record data shown."
    )

    return llm_invoke_cached(
        [("system", _PROFILE_SYSTEM), ("user", user_msg)],
        model="gpt-4o-mini",
        temperature=0.0,
        max_tokens=350,
        cache_ttl=86400 * 7,
    )


# ── Persistence ────────────────────────────────────────────────────────────────

def _save_to_db(doctype: str, summary: str, vector: List[float]) -> None:
    """Upsert profile into DoctypeRoutingProfile doctype."""
    try:
        vector_json = json.dumps(vector)
        now = datetime.now().isoformat()

        if frappe.db.exists(_PROFILE_DOCTYPE, doctype):
            doc = frappe.get_doc(_PROFILE_DOCTYPE, doctype)
            doc.summary = summary
            doc.vector = vector_json
            doc.generated_at = now
            doc.save(ignore_permissions=True)
        else:
            doc = frappe.get_doc({
                "doctype": _PROFILE_DOCTYPE,
                "doctype_name": doctype,
                "summary": summary,
                "vector": vector_json,
                "generated_at": now,
            })
            doc.insert(ignore_permissions=True)

        frappe.db.commit()
    except Exception as e:
        frappe.log_error(f"[profiler] DB save failed for {doctype}: {e}")


def _load_from_db(doctype: str) -> Optional[Dict[str, Any]]:
    """Load profile from DoctypeRoutingProfile doctype."""
    try:
        if not frappe.db.exists(_PROFILE_DOCTYPE, doctype):
            return None
        doc = frappe.get_doc(_PROFILE_DOCTYPE, doctype)
        if not doc.vector:
            return None
        return {
            "summary": doc.summary,
            "vector": json.loads(doc.vector),
        }
    except Exception as e:
        logger.warning(f"[profiler] DB load failed for {doctype}: {e}")
        return None


def _cache_in_redis(doctype: str, summary: str, vector: List[float]) -> None:
    try:
        payload = json.dumps({"summary": summary, "vector": vector})
        frappe.cache().set(f"{_REDIS_PREFIX}{doctype}", payload, ex=_REDIS_TTL)
    except Exception as e:
        logger.warning(f"[profiler] Redis cache write failed for {doctype}: {e}")


def _get_from_redis(doctype: str) -> Optional[Dict[str, Any]]:
    try:
        raw = frappe.cache().get(f"{_REDIS_PREFIX}{doctype}")
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)
    except Exception:
        return None


# ── Core: generate one profile ─────────────────────────────────────────────────

def generate_doctype_profile(doctype: str) -> bool:
    """
    Generate and persist a routing profile for a single DocType.

    Steps: schema context → remote DB titles → LLM summary → embed → save to
    DoctypeRoutingProfile + Redis.

    Safe to call in a background job.
    """
    # Import here to avoid circular import (pinecone_store imports from this module)
    from tap_ai.services.rag.pinecone_store import embed_documents_cached

    try:
        logger.info(f"[profiler] Generating profile for {doctype}")

        schema_text = _build_schema_context(doctype)
        titles = _fetch_all_titles(doctype)
        summary = _generate_summary(doctype, schema_text, titles)

        if not summary:
            logger.warning(f"[profiler] Empty summary for {doctype}, skipping")
            return False

        vectors = embed_documents_cached([summary])
        vector = vectors[0]

        _save_to_db(doctype, summary, vector)
        _cache_in_redis(doctype, summary, vector)

        logger.info(f"[profiler] Profile saved for {doctype} ({len(titles)} titles)")
        return True

    except Exception as e:
        frappe.log_error(f"[profiler] Failed to generate profile for {doctype}: {e}")
        return False


# ── Load all vectors (routing entrypoint) ──────────────────────────────────────

def get_profile_vectors() -> Dict[str, List[float]]:
    """
    Return {doctype: vector} for all allowlisted DocTypes.

    Priority: Redis → DoctypeRoutingProfile doctype → skip (profile not yet generated).
    Never blocks to regenerate — missing profiles fall back to LLM routing for that query.
    """
    schema = load_schema()
    doctypes = [t.replace("tab", "") for t in schema.get("allowlist", [])]

    result: Dict[str, List[float]] = {}

    for doctype in doctypes:
        # 1. Redis hit
        cached = _get_from_redis(doctype)
        if cached:
            result[doctype] = cached["vector"]
            continue

        # 2. Frappe doctype fallback (fast — local SQL, ~1ms per row)
        db_profile = _load_from_db(doctype)
        if db_profile:
            result[doctype] = db_profile["vector"]
            # Repopulate Redis so next call is a cache hit
            _cache_in_redis(doctype, db_profile["summary"], db_profile["vector"])
            continue

        # 3. Not yet generated — skip silently, caller falls back to LLM routing
        logger.debug(f"[profiler] No profile found for {doctype}, will use LLM fallback")

    return result


# ── Cosine similarity routing ──────────────────────────────────────────────────

def route_by_similarity(query_vector: List[float], top_n: int = 4) -> List[str]:
    """
    Return top-N DocTypes by cosine similarity to the query vector.

    OpenAI embeddings are L2-normalised so cosine similarity = dot product.
    Returns empty list if no profiles are available (caller falls back to LLM routing).
    """
    profiles = get_profile_vectors()

    if not profiles:
        return []

    scores = [
        (doctype, sum(a * b for a, b in zip(query_vector, vec)))
        for doctype, vec in profiles.items()
    ]
    scores.sort(key=lambda x: x[1], reverse=True)
    return [dt for dt, _ in scores[:top_n]]


# ── Hook handler ───────────────────────────────────────────────────────────────

def queue_profile_refresh(doc, method):
    """
    doc_events hook: enqueue a background profile regeneration whenever a
    record in an allowlisted DocType is inserted or updated.
    """
    try:
        frappe.enqueue(
            "tap_ai.services.routing.doctype_profiler.generate_doctype_profile",
            doctype=doc.doctype,
            queue="long",
            job_name=f"profile_refresh_{doc.doctype}",
            deduplicate=True,
        )
    except Exception as e:
        frappe.log_error(f"[profiler] Failed to queue profile refresh for {doc.doctype}: {e}")


# ── CLI ────────────────────────────────────────────────────────────────────────

def generate_all_profiles() -> Dict[str, str]:
    """
    Bootstrap profiles for all allowlisted DocTypes.

    Run once after deploying, with the remote DB tunnel open:
        bench execute tap_ai.services.routing.doctype_profiler.generate_all_profiles
    """
    schema = load_schema()
    doctypes = [t.replace("tab", "") for t in schema.get("allowlist", [])]
    total = len(doctypes)
    results: Dict[str, str] = {}

    print(f"\nGenerating routing profiles for {total} DocTypes...\n")

    for i, doctype in enumerate(doctypes, 1):
        print(f"[{i}/{total}] {doctype} ...", end="", flush=True)
        try:
            ok = generate_doctype_profile(doctype)
            results[doctype] = "ok" if ok else "skipped"
            print(f"\r[{i}/{total}] {'ok' if ok else 'skipped':8} {doctype}")
        except Exception as e:
            results[doctype] = f"error: {e}"
            print(f"\r[{i}/{total}] error    {doctype}: {e}")

    ok_count = sum(1 for v in results.values() if v == "ok")
    print(f"\nDone. {ok_count}/{total} profiles generated.\n")
    return results
