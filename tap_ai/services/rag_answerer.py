"""
Vector RAG Engine for TAP AI

Conversational query refinement
Pinecone routing with optional grade/batch/course filtering
Robust context construction from DB (batched + char budget)
Personalized answer synthesis
Rich metadata for debugging & observability
DynamicConfig compatible
"""

import json
import time
from typing import Dict, Any, List, Optional

import frappe
from langchain_openai import ChatOpenAI

from tap_ai.infra.config import get_config
from tap_ai.services.pinecone_store import (
    search_auto_namespaces,
    get_db_columns_for_doctype,
)

# ======================================================
# LLM INITIALIZATION
# ======================================================

def _llm(model: str = "gpt-4o-mini", temperature: float = 0.2) -> ChatOpenAI:
    api_key = get_config("openai_api_key")
    return ChatOpenAI(
        model_name=model,
        openai_api_key=api_key,
        temperature=temperature,
        max_tokens=1500,
    )


# ======================================================
# QUERY REFINER (FROM EARLIER VERSION – KEPT)
# ======================================================

REFINER_PROMPT = """Given a chat history and a follow-up question, rewrite the follow-up question to be a standalone question that a search engine can understand.

- If already standalone, return as is
- Incorporate relevant context from history
- Do NOT answer the question

Return ONLY the refined question.
"""

def _refine_query_with_history(query: str, history: List[Dict[str, str]]) -> str:
    if not history:
        return query

    llm = _llm(temperature=0.0)
    formatted_history = "\n".join(
        f"{msg['role']}: {msg['content']}" for msg in history
    )

    prompt = (
        f"CHAT HISTORY:\n{formatted_history}\n\n"
        f"FOLLOW-UP QUESTION:\n{query}\n\n"
        f"REFINED STANDALONE QUESTION:"
    )

    try:
        resp = llm.invoke([("system", REFINER_PROMPT), ("user", prompt)])
        refined = getattr(resp, "content", query).strip()
        print(f"> Refined Query: {refined}")
        return refined
    except Exception as e:
        frappe.log_error(f"Query refiner failed: {e}")
        return query


# ======================================================
# METADATA FILTER BUILDER (MENTOR VERSION – KEPT)
# ======================================================

def _build_metadata_filter(
    user_profile: Optional[Dict] = None,
    content_details: Optional[Dict] = None
) -> Optional[Dict[str, Any]]:
    filters = {}

    if user_profile:
        if user_profile.get("grade"):
            filters["grade"] = user_profile["grade"]
        if user_profile.get("batch"):
            filters["batch"] = user_profile["batch"]
        if user_profile.get("current_enrollment", {}).get("course"):
            filters["course"] = user_profile["current_enrollment"]["course"]

    if content_details and content_details.get("type"):
        filters["content_type"] = content_details["type"]

    return filters or None


# ======================================================
# CONTEXT BUILDING (RESTORED FROM EARLIER VERSION)
# ======================================================

def _record_to_text(doctype: str, row: Dict[str, Any]) -> str:
    parts = []
    meta = frappe.get_meta(doctype)

    title_field = meta.title_field
    if title_field and row.get(title_field):
        parts.append(f"{meta.get_field(title_field).label}: {row[title_field]}")

    parts.append(f"DocType: {doctype}")
    parts.append(f"ID: {row.get('name')}")

    for k, v in row.items():
        if k in ("name", title_field) or v in (None, ""):
            continue
        v = v.isoformat() if hasattr(v, "isoformat") else v
        parts.append(f"{k}: {v}")

    return "\n".join(parts)


def _build_context_from_hits(
    hits: List[Dict[str, Any]],
    max_chars: int = 12000
) -> Dict[str, Any]:
    context_chunks: List[str] = []
    sources: List[Dict[str, Any]] = []
    used_chars = 0

    for hit in hits:
        meta = hit.get("metadata") or {}
        doctype = meta.get("doctype")
        record_ids = meta.get("record_ids") or []

        if not doctype or not record_ids:
            continue

        try:
            fields = get_db_columns_for_doctype(doctype)
            rows = frappe.get_all(
                doctype,
                filters={"name": ("in", record_ids)},
                fields=fields,
            )

            for row in rows:
                chunk = _record_to_text(doctype, row)
                if used_chars + len(chunk) > max_chars:
                    break

                context_chunks.append(chunk)
                sources.append({
                    "doctype": doctype,
                    "id": row.get("name"),
                    "score": hit.get("score"),
                })
                used_chars += len(chunk)

        except Exception as e:
            frappe.log_error(f"Context build failed for {doctype}: {e}")

        if used_chars >= max_chars:
            break

    return {
        "context_text": "\n\n---\n\n".join(context_chunks),
        "sources": sources,
    }


# ======================================================
# ANSWER SYNTHESIS
# ======================================================

def _synthesize_answer(
    query: str,
    context_text: str,
    user_profile: Optional[Dict] = None,
    history: Optional[List[Dict[str, str]]] = None
) -> str:
    llm = _llm()
    history = history or []

    if user_profile and user_profile.get("name"):
        system_prompt = f"""You are a helpful educational AI assistant.

The user is {user_profile['name']}.
Grade: {user_profile.get('grade', 'N/A')}

Use friendly, age-appropriate language.
"""
    else:
        system_prompt = """You are a helpful educational AI assistant."""

    messages = [{"role": "system", "content": system_prompt}]
    messages.extend(history[-3:])
    messages.append({
        "role": "user",
        "content": f"CONTEXT:\n{context_text}\n\nAnswer this question:\n{query}"
    })

    try:
        resp = llm.invoke(messages)
        return getattr(resp, "content", "I couldn't generate an answer.").strip()
    except Exception as e:
        frappe.log_error(f"RAG synthesis failed: {e}")
        return "There was an error while generating the answer."


# ======================================================
# MAIN ENTRY POINT
# ======================================================

def answer_from_pinecone(
    query: str,
    user_profile: Optional[Dict[str, Any]] = None,
    content_details: Optional[Dict[str, Any]] = None,
    chat_history: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:

    chat_history = chat_history or []
    start = time.time()

    print("> Starting Vector RAG process...")

    # 1. Refine query
    refined_query = _refine_query_with_history(query, chat_history)

    # 2. Build metadata filters
    metadata_filter = _build_metadata_filter(user_profile, content_details)

    # 3. Pinecone search
    search_result = search_auto_namespaces(
        q=refined_query,
        k=15,
        route_top_n=5,
        filters=metadata_filter,
    )

    matches = search_result.get("matches") or []
    routed_doctypes = search_result.get("routed_doctypes") or []

    if not matches:
        return {
            "question": query,
            "answer": "I couldn't find relevant information for your question.",
            "routed_doctypes": routed_doctypes,
            "results_count": 0,
            "search_time": round(time.time() - start, 2),
        }

    # 4. Build context
    ctx = _build_context_from_hits(matches)
    context_text = ctx["context_text"]

    if not context_text.strip():
        return {
            "question": query,
            "answer": "I found references but not enough details to answer confidently.",
            "routed_doctypes": routed_doctypes,
            "results_count": len(matches),
            "search_time": round(time.time() - start, 2),
        }

    # 5. Synthesize answer
    answer = _synthesize_answer(
        query=query,
        context_text=context_text,
        user_profile=user_profile,
        history=chat_history,
    )

    elapsed = round(time.time() - start, 2)

    return {
        "question": query,
        "answer": answer,
        "routed_doctypes": routed_doctypes,
        "results_count": len(matches),
        "search_time": elapsed,
        "user_context": "personalized" if user_profile else "general",
        "metadata": {
            "refined_query": refined_query,
            "filters_used": metadata_filter,
            "sources": ctx["sources"],
        },
    }



# -------- Bench CLI --------
def cli(q: str, k: int = 8, route_top_n: int = 4):
    """
    Bench command to test the RAG pipeline.

    bench execute tap_ai.services.rag_answerer.cli --kwargs "{'q':'Find a video about financial literacy and goal setting and summarize its key points'}"
    bench execute tap_ai.services.rag_answerer.cli --kwargs "{'q':'Can you provide a summary of the video titled Needs First, Wants Later (2024)'}"
    """
    return answer_from_pinecone(q=q, k=k, route_top_n=route_top_n)