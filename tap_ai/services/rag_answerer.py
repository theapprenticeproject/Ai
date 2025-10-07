import json
import time
from typing import Dict, Any, List, Optional

import frappe
from langchain_openai import ChatOpenAI

from tap_ai.infra.config import get_config
# We no longer need the filter extractor
from tap_ai.services.pinecone_store import search_auto_namespaces, get_db_columns_for_doctype
from tap_ai.services.doctype_selector import pick_doctypes

# --- NEW: LLM-based Query Refiner for Conversational Context ---

REFINER_PROMPT = """Given a chat history and a follow-up question, rewrite the follow-up question to be a standalone question that a search engine can understand, incorporating the necessary context from the history.

- If the follow-up is already a complete question, return it as is.
- Incorporate relevant context from the history (like names of items mentioned) into the new question.
- Do NOT answer the question, just reformulate it.

Return ONLY the refined, standalone question.
"""

def _llm(model: str = "gpt-4o-mini", temperature: float = 0.2) -> ChatOpenAI:
    """Initializes the Language Model client."""
    api_key = get_config("openai_api_key")
    return ChatOpenAI(model_name=model, openai_api_key=api_key, temperature=temperature, max_tokens=1500)

def _refine_query_with_history(query: str, history: List[Dict[str, str]]) -> str:
    """Uses an LLM to create a standalone query from a follow-up question and history."""
    if not history:
        return query

    llm = _llm(temperature=0.0)
    # Format the history for the prompt
    formatted_history = "\n".join([f"{msg['role']}: {msg['content']}" for msg in history])
    
    user_prompt = (
        f"CHAT HISTORY:\n{formatted_history}\n\n"
        f"FOLLOW-UP QUESTION: \"{query}\"\n\n"
        f"REFINED STANDALONE QUESTION:"
    )
    
    try:
        resp = llm.invoke([("system", REFINER_PROMPT), ("user", user_prompt)])
        refined_query = getattr(resp, "content", query).strip()
        print(f"> Refined Query for Search: {refined_query}")
        return refined_query
    except Exception as e:
        frappe.log_error(f"Query refiner failed: {e}")
        return query


# --- Core RAG Logic ---

def _record_to_text(doctype: str, row: Dict[str, Any]) -> str:
    """Flattens a record to a text block, giving weight to the title field."""
    parts = []
    meta = frappe.get_meta(doctype)
    title_field, title_value = None, None
    official_title_field = meta.title_field
    if official_title_field and official_title_field in row and row[official_title_field]:
        title_field, title_value = official_title_field, row[official_title_field]
    else:
        fallback_fields = ['title', 'name1', 'video_name', 'assignment_name', 'project_name', 'quiz_name', 'objective_name', 'unit_name', 'comp_name', 'note_name']
        for field in fallback_fields:
            if field in row and row[field]:
                title_field, title_value = field, row[field]
                break
    if title_field and title_value:
        title_label = meta.get_field(title_field).label or title_field.replace("_", " ").title()
        parts.append(f"{title_label}: {title_value}")
    parts.append(f"DocType: {doctype}")
    parts.append(f"ID: {row.get('name','')}")
    for k, v in row.items():
        if k in ('name', title_field) or v in (None, ""): continue
        v_str = v.isoformat() if hasattr(v, 'isoformat') else str(v)
        parts.append(f"{k}: {v_str}")
    return "\n".join(parts)


def _build_context_from_hits(hits: List[Dict[str, Any]], max_chars: int = 12000) -> Dict[str, Any]:
    """Builds context by fetching full records from Frappe DB based on Pinecone hit metadata."""
    context_chunks: List[str] = []
    sources: List[Dict[str, Any]] = []
    used_chars = 0
    
    for h in hits:
        meta = h.get("metadata") or {}
        doctype = meta.get("doctype")
        record_ids = meta.get("record_ids", [])
        
        if not doctype or not record_ids: continue

        try:
            fields = get_db_columns_for_doctype(doctype)
            rows = frappe.get_all(doctype, filters={"name": ("in", record_ids)}, fields=fields) or []
            
            for row in rows:
                text_chunk = _record_to_text(doctype, row)
                if used_chars + len(text_chunk) > max_chars:
                    break
                
                context_chunks.append(text_chunk)
                sources.append({"doctype": doctype, "id": row.get("name"), "score": h.get("score")})
                used_chars += len(text_chunk)
        
        except Exception as e:
            frappe.log_error(f"Failed to fetch records for context building: {e}")

        if used_chars >= max_chars: break
            
    return {"context_text": "\n\n---\n\n".join(context_chunks), "sources": sources}

def answer_from_pinecone(
    q: str,
    k: int = 8,
    route_top_n: int = 4,
    chat_history: Optional[List[Dict[str, str]]] = None
) -> Dict[str, Any]:
    t0 = time.time()
    history = chat_history or []

    # 1. Refine the query to be context-aware before doing anything else.
    refined_query_for_search = _refine_query_with_history(q, history)

    # 2. Use the refined query for both routing and searching (NO FILTERS).
    routed = search_auto_namespaces(q=refined_query_for_search, k=k, route_top_n=route_top_n)
    matches = routed.get("matches") or []

    # 3. Build context from DB hits (two-step fetch).
    ctx = _build_context_from_hits(matches)
    context_text = ctx["context_text"]
    sources = ctx["sources"]

    if not context_text.strip():
        return {
            "question": q,
            "answer": "I don't have enough context in the knowledge base to answer that.",
            "success": True,
            "engine": "rag-pinecone",
            "execution_time": time.time() - t0,
            "metadata": {
                "refined_query_for_search": refined_query_for_search,
                "routed_doctypes": routed.get("routed_doctypes"),
                "sources": [],
            },
        }

    # 4. Use the original query and full history for the final answer synthesis.
    synthesis_llm = _llm()
    system_prompt = (
        "You are a helpful assistant. Answer the user's FINAL question based ONLY on the provided CONTEXT and CHAT HISTORY.\n"
        #"If the context is insufficient, say you don't know. Be concise."
    )
    
    messages = [{"role": "system", "content": system_prompt}]
    messages.extend(history)
    messages.append({
        "role": "user",
        "content": f"CONTEXT:\n{context_text}\n\nBased on the context, answer this question:\n{q}"
    })

    resp = synthesis_llm.invoke(messages)
    answer = getattr(resp, "content", "Could not synthesize an answer.").strip()

    answer = answer.replace('\\u20b9', '₹')
    
    out = {
        "question": q,
        "answer": answer,
        "success": True,
        "engine": "rag-pinecone",
        "execution_time": time.time() - t0,
        "metadata": {
            "refined_query_for_search": refined_query_for_search,
            "routed_doctypes": routed.get("routed_doctypes"),
            "sources": sources,
        },
    }
    return out


# -------- Bench CLI --------
def cli(q: str, k: int = 8, route_top_n: int = 4):
    """
    Bench command to test the RAG pipeline.

    bench execute tap_ai.services.rag_answerer.cli --kwargs "{'q':'Find a video about financial literacy and goal setting and summarize its key points'}"
    bench execute tap_ai.services.rag_answerer.cli --kwargs "{'q':'Can you provide a summary of the video titled Needs First, Wants Later (2024)'}"
    """
    return answer_from_pinecone(q=q, k=k, route_top_n=route_top_n)