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
    embed_query_cached,
)

# ======================================================
# LLM INITIALIZATION
# ======================================================

def _llm(model: str = "gpt-4o-mini", temperature: float = 0.2) -> ChatOpenAI:  
    from tap_ai.infra.llm_client import LLMClient  
    return LLMClient.get_client(  
        model=model,  
        temperature=temperature,  
        max_tokens=1500  
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

_FOLLOW_UP_MARKERS = (
    "it", "this", "that", "these", "those", "they", "them", "he", "she",
    "first one", "second one", "third one", "the above", "previous", "earlier",
    "same", "that one", "explain more", "summarize that", "what about", "how about",
)


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


def _should_refine_query(query: str, history: List[Dict[str, str]]) -> bool:
    """Refine only likely follow-up queries; skip standalone questions to save ~1-2s."""
    if not history:
        return False

    force_refine = str(get_config("rag_force_query_refine") or "").strip().lower()
    if force_refine in ("1", "true", "yes", "on"):
        return True

    q = (query or "").strip().lower()
    if not q:
        return False

    if any(marker in q for marker in _FOLLOW_UP_MARKERS):
        return True

    if q.startswith(("and ", "then ", "also ", "so ")):
        return True

    # Standalone definition/factual queries usually do not need refinement.
    return False

def _refine_query_with_history(query: str, history: List[Dict[str, str]]) -> str:
    if not _should_refine_query(query, history):
        return query

    history_turns = max(1, _to_int(get_config("rag_refine_history_turns") or 2, 2))
    max_chars_per_msg = max(80, _to_int(get_config("rag_refine_message_chars") or 240, 240))
    recent_history = history[-history_turns:]

    if not recent_history:
        return query

    #  OPTIMIZATION: Use cached LLM invoke (Phase 1)
    from tap_ai.services.router import llm_invoke_cached
    
    formatted_history = "\n".join(
        f"{msg.get('role', 'user')}: {(msg.get('content') or '')[:max_chars_per_msg]}"
        for msg in recent_history
    )

    prompt = (
        f"CHAT HISTORY:\n{formatted_history}\n\n"
        f"FOLLOW-UP QUESTION:\n{query}\n\n"
        f"REFINED STANDALONE QUESTION:"
    )

    try:
        refined = llm_invoke_cached(
            [("system", REFINER_PROMPT), ("user", prompt)],
            model="gpt-4o-mini",
            temperature=0.0,
            max_tokens=120,
        )
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


def _max_context_hits() -> int:
    """Cap DB hydration to top-N vector hits to reduce latency."""
    try:
        return int(get_config("rag_max_context_hits") or 6)
    except Exception:
        return 6


def _context_fields_for_doctype(doctype: str) -> List[str]:
    """Fetch a compact field list for context hydration, with cache."""
    cache_key = f"rag_context_fields:{doctype}"
    cached = frappe.cache().get(cache_key)
    if cached:
        if isinstance(cached, bytes):
            cached = cached.decode("utf-8", errors="ignore")
        try:
            fields = json.loads(cached)
            if isinstance(fields, list) and fields:
                return fields
        except Exception:
            pass

    columns = get_db_columns_for_doctype(doctype) or []
    columns_set = set(columns)

    preferred = [
        "name", "title", "subject", "topic", "description", "content",
        "instructions", "learning_objective", "evaluation_points", "rubric",
        "objective", "summary", "course", "grade", "batch", "modified",
    ]
    selected = [f for f in preferred if f in columns_set]

    # Keep at most 15 columns for context hydration; fallback to first columns.
    if not selected:
        selected = ["name"] + [c for c in columns if c != "name"][:14]

    # Ensure unique, stable order.
    seen = set()
    final_fields = []
    for f in selected:
        if f not in seen:
            seen.add(f)
            final_fields.append(f)

    frappe.cache().set(cache_key, json.dumps(final_fields), ex=86400)
    return final_fields


def _build_context_from_hits(
    hits: List[Dict[str, Any]],
    max_chars: int = 12000
) -> Dict[str, Any]:
    """
     OPTIMIZATION: Batch DB queries by doctype (Phase 2)
    Instead of: 15 hits = 15 DB queries
    Now does: 2-3 doctypes = 2-3 batch queries
    """
    context_chunks: List[str] = []
    sources: List[Dict[str, Any]] = []
    used_chars = 0
    metadata_hits_used = 0
    db_queries = 0

    # Hydrate context only for top-N hits; deeper hits are often low signal.
    top_hits = (hits or [])[: _max_context_hits()]
    
    # Group hits by doctype
    hits_by_doctype: Dict[str, List] = {}
    for hit in top_hits:
        meta = hit.get("metadata") or {}
        doctype = meta.get("doctype")
        record_ids = meta.get("record_ids") or []
        
        if not doctype or not record_ids:
            continue
        
        if doctype not in hits_by_doctype:
            hits_by_doctype[doctype] = []
        
        hits_by_doctype[doctype].append((hit, record_ids))
    
    # Fast path: if Pinecone metadata already carries a preview chunk, avoid DB hit.
    pending_hits_by_doctype: Dict[str, List] = {}
    for doctype, hits_group in hits_by_doctype.items():
        for hit, record_ids in hits_group:
            if used_chars >= max_chars:
                break

            meta = hit.get("metadata") or {}
            preview = (meta.get("context_preview") or "").strip()
            if preview:
                chunk = f"DocType: {doctype}\nID: {record_ids[0]}\n{preview}"
                if used_chars + len(chunk) > max_chars:
                    continue
                context_chunks.append(chunk)
                metadata_hits_used += 1
                sources.append({
                    "doctype": doctype,
                    "id": record_ids[0],
                    "score": hit.get("score"),
                })
                used_chars += len(chunk)
                continue

            pending_hits_by_doctype.setdefault(doctype, []).append((hit, record_ids))

    # Single batch query per doctype for misses
    from tap_ai.utils.remote_db import get_remote_all
    
    for doctype, hits_group in pending_hits_by_doctype.items():
        if used_chars >= max_chars:
            break
        
        try:
            # Collect all unique record IDs for this doctype
            all_record_ids = []
            for hit, record_ids in hits_group:
                all_record_ids.extend(record_ids)
            
            all_record_ids = list(set(all_record_ids))  # Deduplicate
            
            if not all_record_ids:
                continue
            
            # ✅ ONE query per doctype instead of ONE per hit
            fields = _context_fields_for_doctype(doctype)
            rows = get_remote_all(
                doctype,
                fields=fields,
                filters={"name": ["in", all_record_ids]},
            )
            db_queries += 1
            
            # Map rows by name for quick lookup
            rows_dict = {row.get("name"): row for row in rows}
            
            # Build context from batched results
            for hit, record_ids in hits_group:
                if used_chars >= max_chars:
                    break
                
                for record_id in record_ids:
                    if used_chars >= max_chars:
                        break
                    
                    if record_id not in rows_dict:
                        continue
                    
                    row = rows_dict[record_id]
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

    return {
        "context_text": "\n\n---\n\n".join(context_chunks),
        "sources": sources,
        "stats": {
            "total_hits_in": len(hits or []),
            "top_hits_used": len(top_hits),
            "metadata_hits_used": metadata_hits_used,
            "db_queries": db_queries,
            "context_chars": used_chars,
        },
    }


def _max_context_chars() -> int:
    """Bound synthesis context size to reduce token latency."""
    return max(1200, _to_int(get_config("rag_max_context_chars") or 6000, 6000))


# ======================================================
# ANSWER SYNTHESIS
# ======================================================

def _synthesize_answer(
    query: str,
    context_text: str,
    user_profile: Optional[Dict] = None,
    history: Optional[List[Dict[str, str]]] = None
) -> str:
    """
     OPTIMIZATION: Use cached LLM invoke (Phase 1)
    """
    from tap_ai.services.router import llm_invoke_cached
    
    history = history or []
    synthesis_history_turns = max(0, _to_int(get_config("rag_synthesis_history_turns") or 1, 1))
    synthesis_temperature = _to_float(get_config("rag_synthesis_temperature") or 0.0, 0.0)
    synthesis_max_tokens = max(180, _to_int(get_config("rag_synthesis_max_tokens") or 500, 500))
    synthesis_model = get_config("rag_synthesis_model") or "gpt-4o-mini"

    if user_profile and user_profile.get("name"):
        system_prompt = f"""You are a helpful educational AI assistant.

The user is {user_profile['name']}.
Grade: {user_profile.get('grade', 'N/A')}

Use friendly, age-appropriate language.
"""
    else:
        system_prompt = """You are a helpful educational AI assistant."""

    messages = [["system", system_prompt]]
    for msg in history[-synthesis_history_turns:]:
        messages.append([msg["role"], msg["content"]])
    messages.append(["user", f"CONTEXT:\n{context_text}\n\nAnswer this question:\n{query}"])

    try:
        answer = llm_invoke_cached(
            messages,
            model=synthesis_model,
            temperature=synthesis_temperature,
            max_tokens=synthesis_max_tokens,
        )
        return answer.strip() if answer else "I couldn't generate an answer."
    except Exception as e:
        frappe.log_error(f"RAG synthesis failed: {e}")
        return "There was an error while generating the answer."


# ======================================================
# MAIN ENTRY POINT
# ======================================================

def answer_from_pinecone(
    query: str,
    k: int = 6,
    route_top_n: int = 5,
    user_profile: Optional[Dict[str, Any]] = None,
    content_details: Optional[Dict[str, Any]] = None,
    chat_history: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:

    chat_history = chat_history or []
    start = time.time()
    timings_ms: Dict[str, int] = {}

    def _stamp(stage_name: str, t0: float):
        timings_ms[stage_name] = int((time.time() - t0) * 1000)

    print("> Starting Vector RAG process...")

    # 1. Refine query
    t_refine = time.time()
    refined_query = _refine_query_with_history(query, chat_history)
    _stamp("refine_query", t_refine)

    # 2. Build metadata filters
    t_filters = time.time()
    metadata_filter = _build_metadata_filter(user_profile, content_details)
    _stamp("build_filters", t_filters)

    # 3. Pinecone search
    t_search = time.time()
    search_result = search_auto_namespaces(
        q=refined_query,
        k=k,
        route_top_n=route_top_n,
        filters=metadata_filter,
    )
    _stamp("vector_search", t_search)

    matches = search_result.get("matches") or []
    routed_doctypes = search_result.get("routed_doctypes") or []

    if not matches:
        timings_ms["total"] = int((time.time() - start) * 1000)
        print(f"> RAG timings (ms): {json.dumps(timings_ms)}")
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
        print(f"> RAG timings (ms): {json.dumps(timings_ms)}")
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
    print(f"> RAG timings (ms): {json.dumps(timings_ms)}")
    print(f"> RAG context stats: {json.dumps(ctx.get('stats') or {})}")

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
            "timings_ms": timings_ms,
            "context_stats": ctx.get("stats") or {},
        },
    }



# -------- Bench CLI --------
def cli(q: str, k: int = 6, route_top_n: int = 4):
    """
    Bench command to test the RAG pipeline.

    bench execute tap_ai.services.rag_answerer.cli --kwargs "{'q':'Find a video about financial literacy and goal setting and summarize its key points'}"
    bench execute tap_ai.services.rag_answerer.cli --kwargs "{'q':'Can you provide a summary of the video titled Needs First, Wants Later (2024)'}"
    """
    return answer_from_pinecone(query=q, k=k, route_top_n=route_top_n)