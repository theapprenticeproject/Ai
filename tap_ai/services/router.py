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
import uuid
from typing import Dict, Any, List, Optional

import frappe
from langchain_openai import ChatOpenAI

from tap_ai.infra.config import get_config
from tap_ai.services.sql_answerer import answer_from_sql
from tap_ai.services.rag_answerer import answer_from_pinecone
from tap_ai.services.direct_answerer import answer_direct


# ======================================================
# LLM INITIALIZATION
# ======================================================

def _llm() -> ChatOpenAI:  
    from tap_ai.infra.llm_client import LLMClient  
    return LLMClient.get_client(  
        model=get_config("primary_llm_model") or "gpt-4o-mini",  
        temperature=0.0  
    )  


# ======================================================
# ROUTER PROMPT
# ======================================================

ROUTER_PROMPT = """You are a query routing expert.

Choose ONE tool:
1. text_to_sql – factual, structured data queries (list, count, show, filter)
2. vector_search – conceptual, explanatory, summarization queries
3. direct_llm – greetings, small talk, wellbeing/motivation guidance, conversational support

Routing hints:
- Use text_to_sql for explicit data lookup from platform tables
- Use vector_search for semantic/content retrieval and summarization from indexed knowledge
- Use direct_llm for social conversation and coaching-style guidance that does not require data retrieval

Return ONLY JSON:
{
    "tool": "text_to_sql" or "vector_search" or "direct_llm",
  "reason": "short explanation (<= 20 words)"
}
"""


def choose_tool(query: str, user_context: Optional[str] = None) -> str:
    llm = _llm()

    prompt = f"USER QUESTION:\n{query}"
    if user_context:
        prompt = f"USER CONTEXT:\n{user_context}\n\n{prompt}"

    prompt += "\n\nWhich tool should be used?"

    try:
        resp = llm.invoke([("system", ROUTER_PROMPT), ("user", prompt)])
        content = getattr(resp, "content", "").strip()
        content = content.replace("```json", "").replace("```", "").strip()
        data = json.loads(content)
        tool = data.get("tool")
        print(f"> Router Reason: {data.get('reason')}")
        if tool in ("text_to_sql", "vector_search", "direct_llm"):
            return tool
    except Exception as e:
        frappe.log_error(f"Router failed: {e}")

    print("> Router fallback → vector_search")
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
    fallback_used: bool
) -> dict:
    res.setdefault("metadata", {})
    res["metadata"].update({
        "original_query": original_query,
        "primary_engine": primary,
        "fallback_used": fallback_used,
    })

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
    context: Optional[Dict[str, Any]] = None
) -> dict:

    chat_history = chat_history or []

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

    # -------- Choose tool --------
    primary_tool = choose_tool(query, user_context)
    print(f"> Selected Primary Tool: {primary_tool}")

    fallback_used = False
    result = {}

    # -------- Execute --------
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
                chat_history=chat_history
            )
            result["interim_message"] = interim

    elif primary_tool == "direct_llm":
        result = answer_direct(
            query=query,
            user_profile=user_profile,
            chat_history=chat_history,
        )

    else:
        primary_tool = "vector_search"
        result = answer_from_pinecone(
            query,
            user_profile=user_profile,
            content_details=content_details,
            chat_history=chat_history
        )

    return _with_meta(result, query, primary_tool, fallback_used)


# ======================================================
# RESILIENT CACHE FOR CHAT HISTORY
# ======================================================

CHAT_HISTORY_TABLE = get_config("chat_history_db_table") or "tabAIChatHistory"


def _cache_key(user_id: str, session_id: Optional[str] = None) -> str:
    return f"chat_history_{user_id}:{session_id}" if session_id else f"chat_history_{user_id}"


def _ensure_chat_history_table_exists():
    if not get_config("enable_db_history"):
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
        raw = frappe.cache().get(key)
        if isinstance(raw, bytes):
            raw = raw.decode()
        history = json.loads(raw) if raw else []
        if history:
            return history

        # Fallback: hydrate live cache from durable DB history
        return _get_history_from_db(user_id, session_id=session_id, limit=10)
    except Exception as e:
        print(f"> History load failed: {e}")
        return []


def _save_history_to_cache(
    user_id: str,
    history: List[Dict[str, str]],
    session_id: Optional[str] = None
):
    try:
        key = _cache_key(user_id, session_id)
        frappe.cache().set(key, json.dumps(history[-10:]))
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

    Turn 1:
    bench execute tap_ai.services.router.cli --kwargs "{'q':'list videos with basic difficulty','user_id':'user123'}"

    Turn 2:
    bench execute tap_ai.services.router.cli --kwargs "{'q':'summarize the first one','user_id':'user123'}"

    bench execute tap_ai.services.router.cli --kwargs "{'q':'list all the videos with easy difficulty','user_id':'user123'}"

    bench execute tap_ai.services.router.cli --kwargs "{'q':'list all the activities present','user_id':'user123'}"

    bench execute tap_ai.services.router.cli --kwargs "{'q':'Find a video about financial literacy and goal setting and summarize its key points','user_id':'user123'}"
    """

    print("\n" + "=" * 80)
    print("TAP AI ROUTER – CLI")
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

    history.append({"role": "user", "content": q})
    history.append({"role": "assistant", "content": out.get("answer", "")})
    _save_history_to_cache(user_id, history)

    print("\n--- RESULT ---")
    print(json.dumps(out, indent=2, ensure_ascii=False))

    return out
