"""
Vector RAG Engine for TAP AI

Conversational query refinement
Pinecone routing with optional grade/batch/course filtering
Context construction from Pinecone metadata (context_preview)
Personalized answer synthesis
Rich metadata for debugging & observability
DynamicConfig compatible
"""

import json
import time
from typing import Any, Dict, List, Optional

import frappe
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from loguru import logger

from tap_ai.infra.config import get_config
from tap_ai.infra.llm_client import llm_invoke_cached
from tap_ai.models import ContentDetails, UserProfile
from tap_ai.utils.prompt_bank import get_system_message_for_context
from tap_ai.utils.query_refiner import refine_query_with_history
from tap_ai.services.rag.pinecone_store import search_auto_namespaces


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


# ======================================================
# METADATA FILTER BUILDER (MENTOR VERSION – KEPT)
# ======================================================

def _build_metadata_filter(
    user_profile: Optional[UserProfile] = None,
    content_details: Optional[ContentDetails] = None,
) -> Optional[Dict[str, Any]]:
    filters = {}

    if user_profile:
        if user_profile.grade:
            filters["grade"] = user_profile.grade
        if user_profile.batch:
            filters["batch"] = user_profile.batch
        if user_profile.current_enrollment and user_profile.current_enrollment.course:
            filters["course"] = user_profile.current_enrollment.course

    if content_details and content_details.type:
        filters["content_type"] = content_details.type

    return filters or None


# ======================================================
# CONTEXT BUILDING
# ======================================================

def _max_context_hits() -> int:
    """Cap context building to top-N vector hits."""
    try:
        configured = int(get_config("rag_max_context_hits") or 6)
        return max(1, min(configured, 6))
    except Exception:
        return 6


def _effective_k(k: int) -> int:
    """Hard-limit top-k to 6 for predictable latency."""
    try:
        parsed = int(k)
    except Exception:
        parsed = 6
    return max(1, min(parsed, 6))


def _build_context_from_hits(
    hits: List[Dict[str, Any]],
    max_chars: int = 12000
) -> Dict[str, Any]:
    """Build context text from Pinecone hit metadata (context_preview is always stored at upsert time)."""
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

    top_hits = (hits or [])[:_max_context_hits()]
    return {
        "context_text": "\n\n---\n\n".join(context_chunks),
        "sources": sources,
        "stats": {
            "total_hits_in": len(hits or []),
            "top_hits_used": len(top_hits),
            "context_chars": used_chars,
        },
    }


def _max_context_chars() -> int:
    """Bound synthesis context size to reduce token latency."""
    return max(1200, _to_int(get_config("rag_max_context_chars") or 6000, 6000))


# ======================================================
# ANSWER SYNTHESIS
# ======================================================

_SYNTHESIS_PROMPT = ChatPromptTemplate.from_messages([
    ("system", "{system_prompt}"),
    MessagesPlaceholder(variable_name="history", optional=True),
    ("human", "CONTEXT:\n{context_text}\n\nAnswer this question:\n{query}"),
])


def _synthesize_answer(
    query: str,
    context_text: str,
    user_profile: Optional[UserProfile] = None,
    history: Optional[List[Dict[str, str]]] = None,
) -> str:
    """
     OPTIMIZATION: Use cached LLM invoke (Phase 1)
    """
    history = history or []
    synthesis_history_turns = max(0, _to_int(get_config("rag_synthesis_history_turns") or 1, 1))
    synthesis_temperature = _to_float(get_config("rag_synthesis_temperature") or 0.0, 0.0)
    synthesis_max_tokens = max(180, _to_int(get_config("rag_synthesis_max_tokens") or 500, 500))
    synthesis_model = get_config("rag_synthesis_model") or "gpt-4o-mini"

    try:
        persona = get_system_message_for_context(user_profile=user_profile)
    except Exception:
        persona = ""

    system_prompt = persona or "You are a helpful educational AI assistant."

    history_msgs = [
        HumanMessage(content=m["content"]) if m["role"] == "user" else AIMessage(content=m["content"])
        for m in history[-synthesis_history_turns:]
    ]
    messages = _SYNTHESIS_PROMPT.format_messages(
        system_prompt=system_prompt,
        history=history_msgs,
        context_text=context_text,
        query=query,
    )

    try:
        answer = llm_invoke_cached(
            [(m.type, m.content) for m in messages],
            model=synthesis_model,
            temperature=synthesis_temperature,
            max_tokens=synthesis_max_tokens,
        )
        return answer.strip() if answer else "I couldn't generate an answer."
    except Exception as e:
        frappe.log_error(f"RAG synthesis failed: {e}")
        return "There was an error while generating the answer."


def retrieve_vector_search(
    query: str,
    k: int = 6,
    route_top_n: int = 5,
    user_profile: Optional[UserProfile] = None,
    content_details: Optional[ContentDetails] = None,
    chat_history: Optional[List[Dict[str, str]]] = None,
    refined_query: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run the vector-search side of the RAG pipeline without answer synthesis.

    This is used when the request needs to expose vector search completion as a
    separate observable phase before the final answer is synthesized.
    """
    chat_history = chat_history or []
    start = time.time()
    timings_ms: Dict[str, int] = {}

    def _stamp(stage_name: str, t0: float):
        timings_ms[stage_name] = int((time.time() - t0) * 1000)

    effective_k = _effective_k(k)

    # Skip refinement if the router already refined it upstream.
    t_refine = time.time()
    if refined_query is None:
        refined_query = refine_query_with_history(query, chat_history)
    _stamp("refine_query", t_refine)

    t_filters = time.time()
    metadata_filter = _build_metadata_filter(user_profile, content_details)
    _stamp("build_filters", t_filters)

    t_search = time.time()
    search_result = search_auto_namespaces(
        q=refined_query,
        k=effective_k,
        route_top_n=route_top_n,
        filters=metadata_filter,
    )
    _stamp("vector_search", t_search)

    matches = search_result.get("matches") or []
    routed_doctypes = search_result.get("routed_doctypes") or []

    if not matches:
        timings_ms["total"] = int((time.time() - start) * 1000)
        return {
            "success": False,
            "question": query,
            "answer": None,
            "routed_doctypes": routed_doctypes,
            "results_count": 0,
            "search_time": round(time.time() - start, 2),
            "user_context": "personalized" if user_profile else "general",
            "error": "I couldn't find relevant information for your question.",
            "metadata": {
                "refined_query": refined_query,
                "filters_used": metadata_filter,
                "effective_k": effective_k,
                "effective_context_hits_cap": _max_context_hits(),
                "sources": [],
                "timings_ms": timings_ms,
                "context_stats": {},
            },
            "context_text": "",
            "context_stats": {},
        }

    t_context = time.time()
    ctx = _build_context_from_hits(matches, max_chars=_max_context_chars())
    _stamp("build_context", t_context)
    context_text = ctx["context_text"]

    if not context_text.strip():
        timings_ms["total"] = int((time.time() - start) * 1000)
        return {
            "success": False,
            "question": query,
            "answer": None,
            "routed_doctypes": routed_doctypes,
            "results_count": len(matches),
            "search_time": round(time.time() - start, 2),
            "user_context": "personalized" if user_profile else "general",
            "error": "I found references but not enough details to answer confidently.",
            "metadata": {
                "refined_query": refined_query,
                "filters_used": metadata_filter,
                "effective_k": effective_k,
                "effective_context_hits_cap": _max_context_hits(),
                "sources": ctx["sources"],
                "timings_ms": timings_ms,
                "context_stats": ctx.get("stats") or {},
            },
            "context_text": "",
            "context_stats": ctx.get("stats") or {},
        }

    timings_ms["total"] = int((time.time() - start) * 1000)

    return {
        "success": True,
        "question": query,
        "answer": None,
        "routed_doctypes": routed_doctypes,
        "results_count": len(matches),
        "search_time": round(time.time() - start, 2),
        "user_context": "personalized" if user_profile else "general",
        "metadata": {
            "refined_query": refined_query,
            "filters_used": metadata_filter,
            "effective_k": effective_k,
            "effective_context_hits_cap": _max_context_hits(),
            "sources": ctx["sources"],
            "timings_ms": timings_ms,
            "context_stats": ctx.get("stats") or {},
        },
        "context_text": context_text,
        "context_stats": ctx.get("stats") or {},
    }


def synthesize_vector_search_answer(
    query: str,
    vector_search_bundle: Dict[str, Any],
    user_profile: Optional[UserProfile] = None,
    chat_history: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """Generate the final answer from a previously prepared vector-search bundle."""
    start = time.time()
    chat_history = chat_history or []

    context_text = vector_search_bundle.get("context_text") or ""
    if not context_text.strip():
        return {
            "question": query,
            "answer": vector_search_bundle.get("error") or "I couldn't find relevant information for your question.",
            "routed_doctypes": vector_search_bundle.get("routed_doctypes") or [],
            "results_count": vector_search_bundle.get("results_count") or 0,
            "search_time": vector_search_bundle.get("search_time"),
            "user_context": vector_search_bundle.get("user_context") or ("personalized" if user_profile else "general"),
            "error": vector_search_bundle.get("error") or "No vector-search context available.",
            "metadata": vector_search_bundle.get("metadata") or {},
        }

    t_synth = time.time()
    answer = _synthesize_answer(
        query=query,
        context_text=context_text,
        user_profile=user_profile,
        history=chat_history,
    )

    metadata = dict(vector_search_bundle.get("metadata") or {})
    timings_ms = dict(metadata.get("timings_ms") or {})
    timings_ms["synthesize_answer"] = int((time.time() - t_synth) * 1000)
    timings_ms["total"] = int((time.time() - start) * 1000) + int((vector_search_bundle.get("metadata") or {}).get("timings_ms", {}).get("total", 0))
    metadata["timings_ms"] = timings_ms

    return {
        "question": query,
        "answer": answer,
        "routed_doctypes": vector_search_bundle.get("routed_doctypes") or [],
        "results_count": vector_search_bundle.get("results_count") or 0,
        "search_time": vector_search_bundle.get("search_time"),
        "user_context": vector_search_bundle.get("user_context") or ("personalized" if user_profile else "general"),
        "metadata": metadata,
        "vector_search": {
            "status": "success" if vector_search_bundle.get("success") else "failed",
            "raw_status": "vector_search_success" if vector_search_bundle.get("success") else "vector_search_failed",
            "results_count": vector_search_bundle.get("results_count") or 0,
            "routed_doctypes": vector_search_bundle.get("routed_doctypes") or [],
            "search_time": vector_search_bundle.get("search_time"),
            "metadata": vector_search_bundle.get("metadata") or {},
        },
    }


# ======================================================
# MAIN ENTRY POINT
# ======================================================

def answer_from_pinecone(
    query: str,
    k: int = 6,
    route_top_n: int = 5,
    user_profile: Optional[UserProfile] = None,
    content_details: Optional[ContentDetails] = None,
    chat_history: Optional[List[Dict[str, str]]] = None,
    refined_query: Optional[str] = None,
) -> Dict[str, Any]:

    chat_history = chat_history or []
    start = time.time()
    timings_ms: Dict[str, int] = {}

    def _stamp(stage_name: str, t0: float):
        timings_ms[stage_name] = int((time.time() - t0) * 1000)

    logger.info("Starting Vector RAG process")

    effective_k = _effective_k(k)

    # 1. Refine query — skip if the router already refined it upstream.
    t_refine = time.time()
    if refined_query is None:
        refined_query = refine_query_with_history(query, chat_history)
    else:
        logger.debug(f"Using pre-refined query from router: {refined_query}")
    _stamp("refine_query", t_refine)

    # 2. Build metadata filters
    t_filters = time.time()
    metadata_filter = _build_metadata_filter(user_profile, content_details)
    _stamp("build_filters", t_filters)

    # 3. Pinecone search
    t_search = time.time()
    search_result = search_auto_namespaces(
        q=refined_query,
        k=effective_k,
        route_top_n=route_top_n,
        filters=metadata_filter,
    )
    _stamp("vector_search", t_search)

    matches = search_result.get("matches") or []
    routed_doctypes = search_result.get("routed_doctypes") or []

    if not matches:
        timings_ms["total"] = int((time.time() - start) * 1000)
        logger.debug(f"RAG timings (ms): {json.dumps(timings_ms)}")
        return {
            "question": query,
            "answer": "I couldn't find relevant information for your question.",
            "routed_doctypes": routed_doctypes,
            "results_count": 0,
            "search_time": round(time.time() - start, 2),
            "timings_ms": timings_ms,
        }

    # 4. Build context
    t_context = time.time()
    ctx = _build_context_from_hits(matches, max_chars=_max_context_chars())
    _stamp("build_context", t_context)
    context_text = ctx["context_text"]

    if not context_text.strip():
        timings_ms["total"] = int((time.time() - start) * 1000)
        logger.debug(f"RAG timings (ms): {json.dumps(timings_ms)}")
        return {
            "question": query,
            "answer": "I found references but not enough details to answer confidently.",
            "routed_doctypes": routed_doctypes,
            "results_count": len(matches),
            "search_time": round(time.time() - start, 2),
            "timings_ms": timings_ms,
            "context_stats": ctx.get("stats") or {},
        }

    # 5. Synthesize answer
    t_synth = time.time()
    answer = _synthesize_answer(
        query=query,
        context_text=context_text,
        user_profile=user_profile,
        history=chat_history,
    )
    _stamp("synthesize_answer", t_synth)

    elapsed = round(time.time() - start, 2)
    timings_ms["total"] = int((time.time() - start) * 1000)
    logger.debug(f"RAG timings (ms): {json.dumps(timings_ms)}")
    logger.debug(f"RAG context stats: {json.dumps(ctx.get('stats') or {})}")

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
            "effective_k": effective_k,
            "effective_context_hits_cap": _max_context_hits(),
            "sources": ctx["sources"],
            "timings_ms": timings_ms,
            "context_stats": ctx.get("stats") or {},
        },
    }



# -------- Bench CLI --------
def cli(q: str, k: int = 6, route_top_n: int = 4):
    """
    Bench command to test the RAG pipeline.

    bench execute tap_ai.services.rag.rag_answerer.cli --kwargs "{'q':'Find a video about financial literacy and goal setting and summarize its key points'}"
    bench execute tap_ai.services.rag.rag_answerer.cli --kwargs "{'q':'Can you provide a summary of the video titled Needs First, Wants Later (2024)'}"
    """
    return answer_from_pinecone(query=q, k=k, route_top_n=route_top_n)