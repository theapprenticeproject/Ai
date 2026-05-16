# tap_ai/services/router.py
"""
TAP AI Router

LLM-based routing (SQL vs RAG vs Direct Chat)
DynamicConfig-compatible user & content context
Robust SQL failure detection (even when SQL "answers")
Automatic fallback with interim message
Resilient chat history cache
Rich metadata
"""

import json
import time
import hashlib
import uuid
import re
from typing import Dict, Any, List, Optional

import frappe
from langchain_openai import ChatOpenAI

from tap_ai.infra.config import get_config
from tap_ai.services.sql_answerer import answer_from_sql
from tap_ai.services.rag_answerer import answer_from_pinecone

from tap_ai.services.direct_response_bank import lookup_direct_response, lookup_exact_direct_response, probe_direct_response_match
from tap_ai.services.single_pass_kb_router import verify_and_respond as verify_kb_and_respond
from tap_ai.services.routing_patterns import match_fast_kb, match_fast_sql


# ======================================================
# LLM INITIALIZATION
# ======================================================

def _llm(
    model: Optional[str] = None,
    temperature: float = 0.0,
    max_tokens: int = 1500,
) -> ChatOpenAI:
    from tap_ai.infra.llm_client import LLMClient
    return LLMClient.get_client(
        model=model or (get_config("primary_llm_model") or "gpt-4o-mini"),
        temperature=temperature,
        max_tokens=max_tokens,
    )


def llm_invoke_cached(
    messages: List,
    model: str = "gpt-4o-mini",
    temperature: float = 0.0,
    cache_ttl: int = 3600,
    max_tokens: int = 700,
) -> str:
    """Invoke LLM with Redis caching; falls back to live invoke on cache issues."""
    try:
        payload = {
            "messages": messages,
            "model": model,
            "temperature": temperature,
        }
        cache_key = "llm_cache:" + hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

        cached = frappe.cache().get(cache_key)
        if cached:
            if isinstance(cached, bytes):
                cached = cached.decode("utf-8", errors="ignore")
            return str(cached)
    except Exception:
        cache_key = None

    llm = _llm(model=model, temperature=temperature, max_tokens=max_tokens)
    start = time.time()
    resp = llm.invoke(messages)
    content = getattr(resp, "content", "")
    if content is None:
        content = ""
    content = str(content).strip()

    try:
        if cache_key and content:
            frappe.cache().set(cache_key, content, ex=cache_ttl)
    except Exception:
        pass

    print(f"> LLM invoke ({model}) took {int((time.time() - start) * 1000)}ms")
    return content


# ======================================================
# ROUTER PROMPT
# ======================================================

ROUTER_PROMPT = """You are a query routing expert.

Choose ONE tool:
1. knowledge_bank - curated TAP response phrases and support snippets from the TAP Response Knowledge doctype
2. text_to_sql - factual, structured data queries (list, count, show, filter)
3. vector_search - conceptual, explanatory, summarization queries

Routing hints:
- Use knowledge_bank for short conversational intents that match curated categories such as greetings, sign-offs, identity questions, TAP program explanations, simple acknowledgements, gibberish or emoji spam, request phrases, help or stuck replies, submission help, and motivational or support replies.
- Use text_to_sql for explicit data lookup from platform tables.
- Use vector_search for semantic/content retrieval and summarization from indexed knowledge.

Return ONLY JSON:
{
    "tool": "knowledge_bank" or "text_to_sql" or "vector_search",
}
"""


def _fast_route(query: str) -> Optional[str]:
    """Fast pattern-based routing without LLM."""
    if match_fast_kb(query):
        return "knowledge_bank"
    
    if match_fast_sql(query):
        return "text_to_sql"
    
    return None


def choose_tool(query: str, user_context: Optional[str] = None) -> str:
    fast_tool = _fast_route(query)
    if fast_tool:
        return fast_tool

    prompt = f"USER QUESTION:\n{query}"
    if user_context:
        prompt = f"USER CONTEXT:\n{user_context}\n\n{prompt}"

    prompt += "\n\nWhich tool should be used?"

    try:
        # Debug: log a short snippet of the prompt so we can reproduce routing decisions
        try:
            print(f"> Router prompt snippet: {prompt[:400]!r}")
        except Exception:
            pass

        messages = [("system", ROUTER_PROMPT), ("user", prompt)]

        content = llm_invoke_cached(
            messages,
            model=get_config("primary_llm_model") or "gpt-4o-mini",
            temperature=0.0,
        )
        # Debug: emit the raw LLM output so we can see why a tool was chosen in logs
        try:
            print(f"> Router LLM output: {str(content)[:1000]!r}")
        except Exception:
            pass
        content = content.replace("```json", "").replace("```", "").strip()
        data = json.loads(content)
        tool = data.get("tool")
        print(f"> Router Reason: {data.get('reason')}")
        if tool in ("knowledge_bank", "text_to_sql", "vector_search"):
            return tool
    except Exception as e:
        frappe.log_error(f"Router failed: {e}")

    print("> Router fallback -> vector_search")
    return "vector_search"


# ======================================================
# FAILURE DETECTION
# ======================================================

def _is_failure(res: dict) -> bool:
    if not res:
        return True

    if res.get("success") is False:
        return True

    answer = (res.get("answer") or "").strip().lower()
    if len(answer) < 10:
        return True

    bad_phrases = (
        "i don't know",
        "unable to",
        "cannot",
        "no answer",
        "failed",
        "error",
        "could not generate",
        "no results found",
        "could not generate a valid sql"
    )

    return any(p in answer for p in bad_phrases)


# ======================================================
# METADATA NORMALIZATION
# ======================================================

def _with_meta(
    res: dict,
    original_query: str,
    primary: str,
    fallback_used: bool,
    timing_ms: Optional[Dict[str, Any]] = None,
) -> dict:
    res.setdefault("metadata", {})
    res["metadata"].update({
        "original_query": original_query,
        "primary_engine": primary,
        "fallback_used": fallback_used,
    })

    if timing_ms:
        res["metadata"].setdefault("timings_ms", {})
        res["metadata"]["timings_ms"].update(timing_ms)

    if "routed_doctypes" in res:
        res["metadata"]["doctypes_used"] = res["routed_doctypes"]

    return res


# ======================================================
# MAIN QUERY PROCESSOR
# ======================================================

def process_query(
    query: str,
    user_profile: Optional[Dict[str, Any]] = None,
    content_details: Optional[Dict[str, Any]] = None,
    chat_history: Optional[List[Dict[str, str]]] = None,
    context: Optional[Dict[str, Any]] = None,
    voice_mode: bool = False,
    primary_tool: Optional[str] = None,
) -> dict:

    chat_history = chat_history or []
    process_start = time.perf_counter()

    # -------- Build user context string (for routing) --------
    user_context = None
    if user_profile:
        parts = [f"User: {user_profile.get('name', 'Unknown')}"]
        if user_profile.get("grade"):
            parts.append(f"Grade: {user_profile['grade']}")
        if user_profile.get("batch"):
            parts.append(f"Batch: {user_profile['batch']}")
        if user_profile.get("current_enrollment"):
            ce = user_profile["current_enrollment"]
            if ce.get("course"):
                parts.append(f"Course: {ce['course']}")
        user_context = " | ".join(parts)

    if content_details:
        content_str = f"Content: {content_details.get('title', 'Unknown')}"
        user_context = f"{user_context}\n{content_str}" if user_context else content_str

    # -------- Query refinement (use history to expand follow-ups) --------
    refined_query = query
    try:
        # Import locally to avoid circular imports at module load
        from tap_ai.services.rag_answerer import _refine_query_with_history

        refined_query = _refine_query_with_history(query, chat_history or []) or query
        if not isinstance(refined_query, str):
            refined_query = str(refined_query)
    except Exception as e:
        frappe.log_error(f"Router: query refiner failed: {e}")

    # -------- Choose tool (routing uses refined query) --------
    routing_ms = 0
    if primary_tool is None:
        routing_start = time.perf_counter()
        primary_tool = choose_tool(refined_query, user_context)
        routing_ms = int((time.perf_counter() - routing_start) * 1000)
        print(f"> Selected Primary Tool: {primary_tool}")

    fallback_used = False
    result = {}

    # -------- Execute --------
    if primary_tool == "knowledge_bank":
        """
        KNOWLEDGE BANK EXECUTION FLOW
        =============================
        
        The knowledge_bank tool follows a two-stage process:
        
        STAGE 1: EXACT MATCH LOOKUP (FAST PATH)
        ──────────────────────────────────────
        lookup_exact_direct_response(query) checks if the user's query matches
        any KB entry after normalization. This includes:
          - student_query field
          - alternate_queries field (all variants)
        
        If found: Returns KB response immediately (~50ms, no LLM)
        
        Examples:
          Query: "Hi"           → Matches "hi" (student_query)
          Query: "Hello there"  → Matches "hello" (alternate_query)
          Query: "Goodbye"      → Matches "bye" (alternate_query)
        
        STAGE 2: LLM FALLBACK (REGEX MATCH BUT NO EXACT MATCH)
        ──────────────────────────────────────────────────────
        If exact match fails, verify_kb_and_respond() is called.
        This loads the ENTIRE active KB and passes it to the LLM to:
          1. Check if the query semantically matches any KB entry
          2. Return the matched KB response if found
          3. Generate a custom answer if no KB match exists
        
        The LLM has full context: match_queries + responses for all KB entries
        This ensures regex-matched queries (e.g., "hello" matching regex) don't
        get dropped if alternate queries weren't explicitly added to the KB.
        
        Examples:
          Regex Match: "heyyyy"      → Exact: NO → LLM: Matches "hey" intent → KB Response
          Regex Match: "gud morning" → Exact: NO → LLM: Matches "good morning" → KB Response
        """
        
        # STAGE 1: Try exact match (fast path, no LLM)
        exact_result = lookup_exact_direct_response(
            query=query,
            user_profile=user_profile,
        )
        if exact_result:
            result = exact_result
        else:
            # STAGE 2: LLM selection with full KB context (fallback)
            result = verify_kb_and_respond(
                query=query,
                user_profile=user_profile,
                chat_history=chat_history,
            )

        processing_ms = int((time.perf_counter() - process_start) * 1000)
        return _with_meta(
            result,
            query,
            "knowledge_bank",
            False,
            timing_ms={"router": routing_ms, "processing_total": processing_ms},
        )

    if primary_tool == "text_to_sql":
        result = answer_from_sql(
            query,
            user_profile=user_profile,
            content_details=content_details,
            chat_history=chat_history
        )

        if _is_failure(result):
            print("> SQL failure detected → Falling back to RAG")
            fallback_used = True
            interim = "Searching, please wait a few more seconds..."
            result = answer_from_pinecone(
                query,
                user_profile=user_profile,
                content_details=content_details,
                chat_history=chat_history,
                refined_query=refined_query,
            )
            result["interim_message"] = interim

    else:
        primary_tool = "vector_search"
        result = answer_from_pinecone(
            query,
            user_profile=user_profile,
            content_details=content_details,
            chat_history=chat_history,
            refined_query=refined_query,
        )

    processing_ms = int((time.perf_counter() - process_start) * 1000)
    return _with_meta(
        result,
        query,
        primary_tool,
        fallback_used,
        timing_ms={"router": routing_ms, "processing_total": processing_ms},
    )


# ======================================================
# RESILIENT CACHE FOR CHAT HISTORY
# ======================================================

CHAT_HISTORY_TABLE = get_config("chat_history_db_table") or "tabAIChatHistory"
_CHAT_HISTORY_TABLE_ENSURED = False


def _cache_key(user_id: str, session_id: Optional[str] = None) -> str:
    return f"chat_history_{user_id}:{session_id}" if session_id else f"chat_history_{user_id}"


def _ensure_chat_history_table_exists():
    global _CHAT_HISTORY_TABLE_ENSURED

    if not get_config("enable_db_history"):
        return

    if _CHAT_HISTORY_TABLE_ENSURED:
        return

    try:
        frappe.db.sql(
            f"""
            CREATE TABLE IF NOT EXISTS `{CHAT_HISTORY_TABLE}` (
                name VARCHAR(255) PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                session_id VARCHAR(255),
                role VARCHAR(50) NOT NULL,
                content TEXT NOT NULL,
                metadata TEXT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        frappe.db.commit()
        _CHAT_HISTORY_TABLE_ENSURED = True
    except Exception as e:
        frappe.log_error(f"Chat history table creation failed: {e}", "tap_ai.services.router")


def _get_history_from_db(
    user_id: str,
    session_id: Optional[str] = None,
    limit: int = 10
) -> List[Dict[str, str]]:
    if not get_config("enable_db_history"):
        return []

    try:
        _ensure_chat_history_table_exists()

        if session_id:
            rows = frappe.db.sql(
                f"SELECT role, content FROM `{CHAT_HISTORY_TABLE}` WHERE user_id = %s AND session_id = %s ORDER BY created_at DESC LIMIT %s",
                (user_id, session_id, limit),
                as_dict=True,
            )
        else:
            rows = frappe.db.sql(
                f"SELECT role, content FROM `{CHAT_HISTORY_TABLE}` WHERE user_id = %s ORDER BY created_at DESC LIMIT %s",
                (user_id, limit),
                as_dict=True,
            )

        return [{"role": row.role, "content": row.content} for row in reversed(rows)]
    except Exception as e:
        print(f"> DB history load failed: {e}")
        return []


def _append_history_to_db(
    user_id: str,
    messages: List[Dict[str, str]],
    session_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
):
    if not get_config("enable_db_history") or not messages:
        return

    metadata = metadata or {}
    try:
        _ensure_chat_history_table_exists()
        for message in messages:
            frappe.db.sql(
                f"INSERT INTO `{CHAT_HISTORY_TABLE}` (name, user_id, session_id, role, content, metadata) VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    uuid.uuid4().hex,
                    user_id,
                    session_id,
                    message.get("role"),
                    message.get("content"),
                    json.dumps(metadata),
                ),
            )
        frappe.db.commit()
    except Exception as e:
        frappe.log_error(f"Chat history save failed: {e}", "tap_ai.services.router")


def _get_history_from_cache(
    user_id: str,
    session_id: Optional[str] = None
) -> List[Dict[str, str]]:
    try:
        key = _cache_key(user_id, session_id)
        cache = frappe.cache()

        # Preferred path: Redis list avoids read-modify-write races.
        if all(hasattr(cache, method) for method in ("lrange",)):
            rows = cache.lrange(key, 0, -1) or []
            history = []
            for row in rows:
                if isinstance(row, bytes):
                    row = row.decode("utf-8", errors="ignore")
                try:
                    item = json.loads(row)
                    if isinstance(item, dict):
                        history.append(item)
                except Exception:
                    continue
            if history:
                return history[-10:]

        # Legacy fallback: JSON blob under same key.
        raw = cache.get(key)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="ignore")
        history = json.loads(raw) if raw else []
        if isinstance(history, list) and history:
            return history[-10:]

        # Fallback: hydrate live cache from durable DB history
        return _get_history_from_db(user_id, session_id=session_id, limit=10)
    except Exception as e:
        print(f"> History load failed: {e}")
        return []


def _save_history_to_cache(
    user_id: str,
    messages: List[Dict[str, str]],
    session_id: Optional[str] = None
):
    try:
        if not messages:
            return

        key = _cache_key(user_id, session_id)
        cache = frappe.cache()

        # Preferred path: append-only list operations are safer under concurrency.
        if all(hasattr(cache, method) for method in ("rpush", "ltrim")):
            for message in messages:
                cache.rpush(key, json.dumps(message))
            cache.ltrim(key, -10, -1)
            return

        # Fallback for cache backends without list operations.
        existing = _get_history_from_cache(user_id, session_id=session_id)
        merged = (existing + messages)[-10:]
        cache.set(key, json.dumps(merged))
    except Exception as e:
        print(f"> History save failed: {e}")


def get_session_transcript(
    session_id: str,
    user_id: Optional[str] = None,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    if not get_config("enable_db_history"):
        return []

    _ensure_chat_history_table_exists()
    sql = f"SELECT role, content, metadata, created_at FROM `{CHAT_HISTORY_TABLE}` WHERE session_id = %s"
    params = [session_id]
    if user_id:
        sql += " AND user_id = %s"
        params.append(user_id)
    sql += " ORDER BY created_at ASC"
    if limit:
        sql += " LIMIT %s"
        params.append(limit)

    rows = frappe.db.sql(sql, tuple(params), as_dict=True)
    return [
        {
            "role": row.role,
            "content": row.content,
            "metadata": json.loads(row.metadata) if row.metadata else {},
            "created_at": row.created_at,
        }
        for row in rows
    ]


def list_sessions_for_user(user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
    if not get_config("enable_db_history"):
        return []

    _ensure_chat_history_table_exists()
    rows = frappe.db.sql(
        f"SELECT session_id, MIN(created_at) AS started_at, MAX(created_at) AS last_activity_at, COUNT(*) AS turns FROM `{CHAT_HISTORY_TABLE}` WHERE user_id = %s GROUP BY session_id ORDER BY last_activity_at DESC LIMIT %s",
        (user_id, limit),
        as_dict=True,
    )

    return [
        {
            "session_id": row.session_id,
            "started_at": row.started_at,
            "last_activity_at": row.last_activity_at,
            "turns": row.turns,
        }
        for row in rows
        if row.session_id
    ]


# ======================================================
# CLI ENTRY POINT
# ======================================================

def cli(q: str, user_id: str = "default_user"):
    """
    Bench CLI with resilient chat memory.

    Examples:

    bench execute tap_ai.services.router.cli --kwargs "{'q':'👋','user_id':'user123'}"

    Turn 1:
    bench execute tap_ai.services.router.cli --kwargs "{'q':'list videos with basic difficulty','user_id':'user123'}"

    Turn 2:
    bench execute tap_ai.services.router.cli --kwargs "{'q':'summarize the first one','user_id':'user123'}"

    bench execute tap_ai.services.router.cli --kwargs "{'q':'list all the videos with easy difficulty','user_id':'user123'}"

    bench execute tap_ai.services.router.cli --kwargs "{'q':'list all the activities present','user_id':'user123'}"

    bench execute tap_ai.services.router.cli --kwargs "{'q':'Find a video about financial literacy and goal setting and summarize its key points','user_id':'user123'}"

    bench execute tap_ai.services.router.cli --kwargs "{'q':'what is budget','user_id':'user123'}"
    
    """

    print("\n" + "=" * 80)
    print("TAP AI ROUTER â€“ CLI")
    print("=" * 80)

    history = _get_history_from_cache(user_id)

    out = process_query(
        query=q,
        user_profile=None,
        content_details=None,
        chat_history=history
    )

    if "interim_message" in out:
        print("\n--- INTERIM MESSAGE ---")
        print(out["interim_message"])

    new_messages = [
        {"role": "user", "content": q},
        {"role": "assistant", "content": out.get("answer", "")},
    ]
    history.extend(new_messages)
    _save_history_to_cache(user_id, new_messages)

    print("\n--- RESULT ---")
    print(json.dumps(out, indent=2, ensure_ascii=False))

    return out

