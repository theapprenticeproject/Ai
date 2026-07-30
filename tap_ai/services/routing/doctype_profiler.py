"""
DocType Routing Profiler

For each DocType in the allowlist, two complementary embeddings are built:

  1. Summary vector — collect schema metadata + a sample of record titles,
     ask the LLM for a 2-3 sentence routing summary, embed that summary.
     Good at broad/conceptual queries, but the LLM only sees a sample and
     paraphrases — exact vocabulary from the data can get lost.
  2. Titles vector — embed a deduplicated, capped list of the ACTUAL record
     titles/topics directly (no LLM paraphrase). Grounds routing in the
     literal terms present in the data (e.g. an exact course/quiz name),
     which the summary alone can miss.

Both are persisted to the DoctypeRoutingProfile Frappe doctype (survives
Redis flush) and cached in Redis for fast repeated access.

At query time:
  - Reuse the already-computed pgvector query embedding (zero extra cost)
  - Cosine similarity vs both vectors per DocType, score = max(summary, titles)
  - Top-N DocTypes by that score → namespaces to search

Auto-refresh:
  - doc_events hook fires on insert/update/delete of any allowlisted DocType
    record (or TAP Response Knowledge entry) → background job regenerates
    that DocType's profile, since a changed/removed record changes its titles.

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
        from tap_ai.services.rag.pgvector_store import get_db_columns_for_doctype
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


# ── Titles text (for the second, literal embedding) ────────────────────────────

_TITLES_TEXT_MAX_ITEMS = 500
_TITLES_TEXT_MAX_CHARS = 20000


def _build_titles_text(titles: List[str], max_items: int = _TITLES_TEXT_MAX_ITEMS, max_chars: int = _TITLES_TEXT_MAX_CHARS) -> str:
    """
    Dedupe and cap a raw titles list into embeddable text.

    Unlike the LLM summary (a paraphrase), this text is embedded verbatim so
    exact terms in it directly influence the titles_vector — capping keeps a
    single embedding call from averaging out into noise over thousands of
    disparate short strings.
    """
    seen: List[str] = []
    for title in titles:
        cleaned = (title or "").strip()
        if cleaned and cleaned not in seen:
            seen.append(cleaned)
        if len(seen) >= max_items:
            break

    text = "\n".join(seen)
    return text[:max_chars]


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

    model = get_config("profiler_summary_model") or "gpt-4o"
    return llm_invoke_cached(
        [("system", _PROFILE_SYSTEM), ("user", user_msg)],
        model=model,
        temperature=0.0,
        max_tokens=350,
        cache_ttl=86400 * 7,
    )


# ── Persistence ────────────────────────────────────────────────────────────────

def _save_to_db(
    doctype: str,
    summary: str,
    vector: List[float],
    titles_text: Optional[str] = None,
    titles_vector: Optional[List[float]] = None,
) -> None:
    """Upsert profile into DoctypeRoutingProfile doctype."""
    try:
        vector_json = json.dumps(vector)
        titles_vector_json = json.dumps(titles_vector) if titles_vector else None
        now = datetime.now().isoformat()

        if frappe.db.exists(_PROFILE_DOCTYPE, doctype):
            doc = frappe.get_doc(_PROFILE_DOCTYPE, doctype)
            doc.summary = summary
            doc.vector = vector_json
            doc.titles_text = titles_text or ""
            doc.titles_vector = titles_vector_json
            doc.generated_at = now
            doc.save(ignore_permissions=True)
        else:
            doc = frappe.get_doc({
                "doctype": _PROFILE_DOCTYPE,
                "doctype_name": doctype,
                "summary": summary,
                "vector": vector_json,
                "titles_text": titles_text or "",
                "titles_vector": titles_vector_json,
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
        result: Dict[str, Any] = {
            "summary": doc.summary,
            "vector": json.loads(doc.vector),
        }
        titles_vector_raw = getattr(doc, "titles_vector", None)
        if titles_vector_raw:
            try:
                result["titles_vector"] = json.loads(titles_vector_raw)
            except Exception:
                pass
        return result
    except Exception as e:
        logger.warning(f"[profiler] DB load failed for {doctype}: {e}")
        return None


def _cache_in_redis(
    doctype: str,
    summary: str,
    vector: List[float],
    titles_vector: Optional[List[float]] = None,
) -> None:
    try:
        payload: Dict[str, Any] = {"summary": summary, "vector": vector}
        if titles_vector:
            payload["titles_vector"] = titles_vector
        frappe.cache().set(f"{_REDIS_PREFIX}{doctype}", json.dumps(payload), ex=_REDIS_TTL)
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

    Steps: schema context → remote DB titles → LLM summary → embed summary
    + embed the raw titles list (separately) → save both vectors to
    DoctypeRoutingProfile + Redis.

    Safe to call in a background job.
    """
    # Import here to avoid circular import (pgvector_store imports from this module)
    from tap_ai.services.rag.pgvector_store import embed_documents_cached

    try:
        logger.info(f"[profiler] Generating profile for {doctype}")

        schema_text = _build_schema_context(doctype)
        titles = _fetch_all_titles(doctype)
        summary = _generate_summary(doctype, schema_text, titles)

        if not summary:
            logger.warning(f"[profiler] Empty summary for {doctype}, skipping")
            return False

        titles_text = _build_titles_text(titles)
        embed_inputs = [summary, titles_text] if titles_text else [summary]
        vectors = embed_documents_cached(embed_inputs)
        vector = vectors[0]
        titles_vector = vectors[1] if titles_text else None

        _save_to_db(doctype, summary, vector, titles_text=titles_text, titles_vector=titles_vector)
        _cache_in_redis(doctype, summary, vector, titles_vector=titles_vector)

        logger.info(f"[profiler] Profile saved for {doctype} ({len(titles)} titles)")
        return True

    except Exception as e:
        frappe.log_error(f"[profiler] Failed to generate profile for {doctype}: {e}")
        return False


# ── KB profile (hand-crafted — KB entries are short phrases, not prose) ────────

_KB_DOCTYPE = "TAP Response Knowledge"

# Built from the actual KB content so cosine similarity correctly routes
# conversational queries (greetings, help, done, who are you, etc.) to this namespace.
_KB_PROFILE_SUMMARY = (
    "Handles all conversational, social, and administrative student interactions: "
    "greetings (hi, hello, hey, hii, sup, namaste, good morning, gm, shubh prabhat), "
    "farewells and sign-offs (bye, goodbye, tata, baad mein), "
    "good night messages (good night, sone jaa raha hun), "
    "festival and occasion wishes (happy diwali, eid mubarak, happy holi, merry christmas, happy summer holiday), "
    "gibberish, random character spam, emoji-only and emoji spam messages, "
    "very short or unclear mid-flow messages (ok, k, hmm, acha, thik hai), "
    "identity and program questions (who are you, aap kaun ho, your name, tap buddy kaun hai, "
    "what is TAP program, what is activity, what are points, what is a special gem, how to submit, what are videos), "
    "help and stuck requests (problem hai, help me, mujhe madad chahiye, issue, no ideas, "
    "kya likhun, what to do, kuch samajh nahi, I don't know what to submit), "
    "submission done confirmations (done, kar diya, I have done, submit kar diya, ho gaya), "
    "refusal to submit (no I won't, nahi karunga, boring, not doing it), "
    "school and admin questions (school open hai, registration number, which teacher, school closed), "
    "emotional expressions and excitement (I'm bored, bore ho raha hun, maza aa gaya, awesome, I love TAP buddy, I'm impressed), "
    "class promotion and achievement (pass ho gaya, promoted, naya class, badhai do), "
    "requests for language change, wrong name correction, time requests (time chahiye, busy hoon, not now, baad mein karunga), "
    "and ready or continue signals (yes, haan, ready, continue, chalo, I want to)."
)


def _build_kb_titles_text() -> str:
    """Titles text for the KB profile: every active entry's student_query + alternate_queries."""
    from tap_ai.services.kb.direct_response_bank import _parse_aliases, get_direct_response_entries

    entries = get_direct_response_entries(force_refresh=True)
    phrases: List[str] = []
    for entry in entries:
        if not entry or not entry.get("is_active", 1):
            continue
        student_query = (entry.get("student_query") or "").strip()
        if student_query:
            phrases.append(student_query)
        phrases.extend(_parse_aliases(entry.get("alternate_queries")))

    return _build_titles_text(phrases)


def generate_kb_profile() -> bool:
    """
    Generate and persist a routing profile for TAP Response Knowledge.

    Bypasses the standard LLM-summary flow because KB entries are short intent
    phrases — the hand-crafted _KB_PROFILE_SUMMARY produces a better routing vector.
    The titles vector is still built normally, embedding every active entry's
    student_query + alternate_queries verbatim (e.g. "Hi", "Hey") so exact
    conversational phrases match directly, not just the summary's paraphrase.

    Run once after deploying KB pgvector indexing:
        bench execute tap_ai.services.routing.doctype_profiler.generate_kb_profile
    """
    from tap_ai.services.rag.pgvector_store import embed_documents_cached

    try:
        logger.info(f"[profiler] Generating KB profile for {_KB_DOCTYPE}")
        titles_text = _build_kb_titles_text()
        embed_inputs = [_KB_PROFILE_SUMMARY, titles_text] if titles_text else [_KB_PROFILE_SUMMARY]
        vectors = embed_documents_cached(embed_inputs)
        vector = vectors[0]
        titles_vector = vectors[1] if titles_text else None
        _save_to_db(_KB_DOCTYPE, _KB_PROFILE_SUMMARY, vector, titles_text=titles_text, titles_vector=titles_vector)
        _cache_in_redis(_KB_DOCTYPE, _KB_PROFILE_SUMMARY, vector, titles_vector=titles_vector)
        logger.info(f"[profiler] KB profile saved for {_KB_DOCTYPE}")
        return True
    except Exception as e:
        frappe.log_error(f"[profiler] Failed to generate KB profile: {e}")
        return False


# ── Load all vectors (routing entrypoint) ──────────────────────────────────────

def get_profile_vectors() -> Dict[str, Dict[str, Any]]:
    """
    Return {doctype: {"vector": [...], "titles_vector": [...] | None}} for all
    allowlisted DocTypes plus TAP Response Knowledge.

    Priority: Redis → DoctypeRoutingProfile doctype → skip (profile not yet generated).
    Never blocks to regenerate — missing profiles fall back to LLM routing for that query.
    """
    schema = load_schema()
    doctypes = [t.replace("tab", "") for t in schema.get("allowlist", [])]

    # Include KB so route_by_similarity can route conversational queries to it.
    # Profile is generated by generate_kb_profile(), not the standard LLM summariser.
    if _KB_DOCTYPE not in doctypes:
        doctypes.append(_KB_DOCTYPE)

    result: Dict[str, Dict[str, Any]] = {}

    for doctype in doctypes:
        # 1. Redis hit
        cached = _get_from_redis(doctype)
        if cached:
            result[doctype] = {"vector": cached["vector"], "titles_vector": cached.get("titles_vector")}
            continue

        # 2. Frappe doctype fallback (fast — local SQL, ~1ms per row)
        db_profile = _load_from_db(doctype)
        if db_profile:
            result[doctype] = {"vector": db_profile["vector"], "titles_vector": db_profile.get("titles_vector")}
            # Repopulate Redis so next call is a cache hit
            _cache_in_redis(doctype, db_profile["summary"], db_profile["vector"], titles_vector=db_profile.get("titles_vector"))
            continue

        # 3. Not yet generated — skip silently, caller falls back to LLM routing
        logger.debug(f"[profiler] No profile found for {doctype}, will use LLM fallback")

    return result


# ── Cosine similarity routing ──────────────────────────────────────────────────

def _dot(query_vector: List[float], vec: Optional[List[float]]) -> float:
    if not vec:
        return float("-inf")
    return sum(a * b for a, b in zip(query_vector, vec))


def route_by_similarity(query_vector: List[float], top_n: int = 4) -> List[str]:
    """
    Return top-N DocTypes by cosine similarity to the query vector.

    Each DocType has two candidate vectors — an LLM-summary embedding (broad,
    conceptual) and a titles embedding (literal record titles/topics). The
    score is max(summary_score, titles_score): either signal can win a match
    independently, so an exact term hit in titles isn't diluted by averaging
    against a summary that may not mention it, and vice versa.

    OpenAI embeddings are L2-normalised so cosine similarity = dot product.
    Returns empty list if no profiles are available (caller falls back to LLM routing).
    """
    profiles = get_profile_vectors()

    if not profiles:
        return []

    scores = [
        (doctype, max(_dot(query_vector, profile.get("vector")), _dot(query_vector, profile.get("titles_vector"))))
        for doctype, profile in profiles.items()
    ]
    scores.sort(key=lambda x: x[1], reverse=True)
    return [dt for dt, _ in scores[:top_n]]


# ── Hook handler ───────────────────────────────────────────────────────────────

def queue_profile_refresh(doc, method):
    """
    doc_events hook: enqueue a background profile regeneration whenever a
    record in an allowlisted DocType is inserted, updated, or deleted — any of
    these changes the DocType's title list, so the titles_vector needs a refresh.
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


def queue_kb_profile_refresh(doc, method):
    """
    doc_events hook: enqueue a background KB profile regeneration whenever a
    TAP Response Knowledge entry is inserted, updated, or deleted — the KB
    titles_vector is built from every active entry's student_query +
    alternate_queries, so any of these changes the titles list.
    """
    try:
        frappe.enqueue(
            "tap_ai.services.routing.doctype_profiler.generate_kb_profile",
            queue="long",
            job_name="kb_profile_refresh",
            deduplicate=True,
        )
    except Exception as e:
        frappe.log_error(f"[profiler] Failed to queue KB profile refresh: {e}")


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
