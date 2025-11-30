# tap_ai/services/rag_answerer.py
"""
Vector RAG Engine - Enhanced with User Context
Performs semantic search with grade/batch filtering for personalized results
FINAL FIX: Correct search_auto_namespaces call signature
"""

import json
import time
from typing import Dict, Any, List, Optional

import frappe
from langchain_openai import ChatOpenAI

from tap_ai.infra.config import get_config
from tap_ai.services.pinecone_store import search_auto_namespaces, get_db_columns_for_doctype


# --- LLM-based Query Refiner for Conversational Context ---

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
    all_cols = get_db_columns_for_doctype(doctype)
    title_field = None
    for candidate in ["title", "name1", "video_name", "quiz_name", "assignment_name", "subject"]:
        if candidate in all_cols:
            title_field = candidate
            break
    
    parts = []
    if title_field and row.get(title_field):
        parts.append(f"{title_field.upper()}: {row[title_field]}")
    
    for k, v in row.items():
        if k == title_field:
            continue
        if v and str(v).strip():
            parts.append(f"{k}: {v}")
    
    return "\n".join(parts)


def _build_metadata_filter(
    user_profile: Optional[Dict] = None,
    content_details: Optional[Dict] = None
) -> Optional[Dict[str, Any]]:
    """
    Builds Pinecone metadata filter based on user context.
    
    NOTE: Currently building filter but search_auto_namespaces may not fully support it.
    This is prepared for future enhancement.
    """
    filters = {}
    
    if user_profile and user_profile.get('grade'):
        filters['grade'] = user_profile['grade']
        print(f"> Grade filter prepared: {user_profile['grade']}")
    
    if user_profile and user_profile.get('batch'):
        filters['batch'] = user_profile['batch']
        print(f"> Batch filter prepared: {user_profile['batch']}")
    
    if user_profile and user_profile.get('current_enrollment'):
        enrollment = user_profile['current_enrollment']
        if enrollment.get('course'):
            filters['course'] = enrollment['course']
            print(f"> Course filter prepared: {enrollment['course']}")
    
    if content_details and content_details.get('type'):
        filters['content_type'] = content_details['type']
        print(f"> Content type filter prepared: {content_details['type']}")
    
    return filters if filters else None


def _synthesize_answer_with_context(
    query: str,
    context_records: List[str],
    user_profile: Optional[Dict] = None,
    content_details: Optional[Dict] = None,
    history: Optional[List[Dict[str, str]]] = None
) -> str:
    """Synthesizes a final answer using retrieved context with user personalization."""
    llm = _llm()
    
    # Build user context string
    user_context_str = ""
    if user_profile:
        context_parts = [f"User: {user_profile.get('name', 'Student')}"]
        if user_profile.get('grade'):
            context_parts.append(f"Grade {user_profile['grade']}")
        if user_profile.get('batch'):
            context_parts.append(f"Batch {user_profile['batch']}")
        if user_profile.get('current_enrollment'):
            enrollment = user_profile['current_enrollment']
            if enrollment.get('course'):
                context_parts.append(f"Course: {enrollment['course']}")
        user_context_str = " | ".join(context_parts)
    
    # Build content context string
    content_context_str = ""
    if content_details:
        content_context_str = f"Content: {content_details.get('title')} (Type: {content_details.get('type')})"
    
    system_prompt = """You are a helpful educational assistant.
    
You will be given:
1. A user's question
2. User context (name, grade, batch, course)
3. Relevant educational content retrieved from a database
4. Optional conversation history

Your job is to synthesize a clear, friendly, and educational answer that:
- Directly answers the user's question
- Is appropriate for the user's grade level
- Uses the retrieved content as supporting evidence
- References specific content when relevant
- Is personalized for the user

Keep your answer concise but informative."""
    
    # Build context records string (safe slicing)
    safe_records = context_records[:min(10, len(context_records))] if isinstance(context_records, list) else []
    context_str = "\n\n---\n\n".join(safe_records)
    
    # Build user prompt
    user_prompt_parts = []
    
    if history:
        history_str = "\n".join([f"{msg['role'].title()}: {msg['content']}" for msg in history])
        user_prompt_parts.append(f"CONVERSATION HISTORY:\n{history_str}\n")
    
    if user_context_str:
        user_prompt_parts.append(f"USER CONTEXT: {user_context_str}\n")
    
    if content_context_str:
        user_prompt_parts.append(f"CONTENT CONTEXT: {content_context_str}\n")
    
    user_prompt_parts.append(f"USER'S QUESTION: {query}\n")
    user_prompt_parts.append(f"RETRIEVED CONTEXT (from database):\n{context_str}\n")
    user_prompt_parts.append(
        "Please provide a clear, friendly answer that's personalized for this user's grade level. "
        "Reference specific content from the context when relevant."
    )
    
    user_prompt = "\n".join(user_prompt_parts)
    
    try:
        resp = llm.invoke([("system", system_prompt), ("user", user_prompt)])
        return getattr(resp, "content", "I couldn't find a good answer.").strip()
    except Exception as e:
        frappe.log_error(f"RAG answer synthesis failed: {e}")
        return "There was an error while formulating an answer."


# --- Main Function (ENHANCED) ---

def answer_from_pinecone(
    query: str,
    user_profile: Optional[Dict[str, Any]] = None,
    content_details: Optional[Dict[str, Any]] = None,
    chat_history: Optional[List[Dict[str, str]]] = None
) -> Dict[str, Any]:
    """
    Main Vector RAG entry point with user context and grade filtering.
    
    Args:
        query: Natural language question
        user_profile: User profile from DynamicConfig.get_user_profile()
            Contains: name, batch, grade, enrollments, current_enrollment
        content_details: Content details for content-specific queries
        chat_history: Conversation history
    
    Returns:
        Dictionary with answer, context used, and success flag
    """
    chat_history = chat_history or []
    start_time = time.time()
    
    print("> Starting Vector RAG process...")
    if user_profile:
        print(f"> User Context: {user_profile.get('name')} | Grade {user_profile.get('grade')} | Batch {user_profile.get('batch')}")
    
    # Step 1: Refine query with conversation history
    refined_query = _refine_query_with_history(query, chat_history)
    
    # Step 2: Build metadata filter for grade/batch
    metadata_filter = _build_metadata_filter(user_profile, content_details)
    
    # Step 3: Search Pinecone
    # NOTE: search_auto_namespaces already calls pick_doctypes internally!
    # Signature: search_auto_namespaces(q: str, k: int = 8, route_top_n: int = 4, filters: Optional[Dict] = None)
    try:
        print("> Calling Pinecone search...")
        search_result = search_auto_namespaces(
            q=refined_query,
            k=15,  # Return top 15 results
            route_top_n=5,  # Route to top 5 doctypes
            filters=metadata_filter  # Optional metadata filters
        )
        
        # search_auto_namespaces returns a dict with structure:
        # {"q": str, "routed_doctypes": list, "k": int, "matches": list}
        routed_doctypes = search_result.get("routed_doctypes", [])
        all_matches = search_result.get("matches", [])
        
        print(f"> Routed to DocTypes: {routed_doctypes}")
        print(f"> Retrieved {len(all_matches)} results from Pinecone.")
        
        if not all_matches:
            return {
                "question": query,
                "answer": "I couldn't find relevant information in the database.",
                "context_used": [],
                "success": False
            }
        
        # Step 4: Manual grade filtering on results
        filtered_results = list(all_matches)  # Start with all results
        
        if user_profile and user_profile.get('grade'):
            user_grade = str(user_profile['grade'])
            temp_filtered = []
            
            for result in all_matches:
                if not isinstance(result, dict):
                    continue
                    
                metadata = result.get('metadata', {})
                result_grade = metadata.get('grade')
                
                # Include if no grade specified (general content) or if grade matches
                if not result_grade or str(result_grade) == user_grade:
                    temp_filtered.append(result)
            
            if temp_filtered:
                filtered_results = temp_filtered
                print(f"> Filtered to {len(filtered_results)} grade-appropriate results (Grade {user_grade}).")
            else:
                print(f"> No exact grade match, using all {len(all_matches)} results.")
        
        # Step 5: Extract text records (safe iteration)
        context_records = []
        max_records = min(10, len(filtered_results))
        
        for i in range(max_records):
            result = filtered_results[i]
            if not isinstance(result, dict):
                continue
                
            metadata = result.get("metadata", {})
            doctype = metadata.get("doctype", "Unknown")
            doc_name = metadata.get("doc_name", "")
            
            try:
                if doctype and doc_name:
                    # Try to get the actual record
                    record = frappe.get_doc(doctype, doc_name).as_dict()
                    text_block = _record_to_text(doctype, record)
                    context_records.append(text_block)
                else:
                    # Use metadata directly
                    text_block = "\n".join([f"{k}: {v}" for k, v in metadata.items()])
                    context_records.append(text_block)
            except Exception as e:
                # If error fetching record, use metadata
                text_block = "\n".join([f"{k}: {v}" for k, v in metadata.items()])
                context_records.append(text_block)
        
        if not context_records:
            return {
                "question": query,
                "answer": "I found results but couldn't extract meaningful content.",
                "context_used": [],
                "success": False
            }
        
        # Step 6: Synthesize answer with user context
        final_answer = _synthesize_answer_with_context(
            query,
            context_records,
            user_profile,
            content_details,
            chat_history
        )
        
        elapsed = time.time() - start_time
        print(f"> RAG completed in {elapsed:.2f}s")
        
        # Safe return
        safe_context_used = context_records[:min(5, len(context_records))]
        
        return {
            "question": query,
            "answer": final_answer,
            "context_used": safe_context_used,
            "num_results": len(filtered_results),
            "routed_doctypes": routed_doctypes,
            "success": True,
            "user_context": {
                "batch": user_profile.get('batch') if user_profile else None,
                "grade": user_profile.get('grade') if user_profile else None,
                "filtered_by_grade": len(filtered_results) != len(all_matches) if user_profile and user_profile.get('grade') else False
            }
        }
        
    except Exception as e:
        frappe.log_error(f"Pinecone search failed: {e}")
        import traceback
        traceback.print_exc()
        return {
            "question": query,
            "answer": f"An error occurred while searching: {str(e)}",
            "context_used": [],
            "success": False
        }
