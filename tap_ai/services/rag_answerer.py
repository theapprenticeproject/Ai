# tap_ai/services/rag_answerer.py
"""
Vector RAG Engine - Enhanced with Optional User Context
Performs semantic search with optional grade/batch filtering for personalized results
"""

import json
import time
from typing import Dict, Any, List, Optional

import frappe
from langchain_openai import ChatOpenAI

from tap_ai.infra.config import get_config
from tap_ai.services.pinecone_store import search_auto_namespaces, get_db_columns_for_doctype


# --- LLM Initialization ---

def _llm(model: str = "gpt-4o-mini", temperature: float = 0.2) -> ChatOpenAI:
    """Initializes the Language Model client."""
    api_key = get_config("openai_api_key")
    return ChatOpenAI(
        model_name=model,
        openai_api_key=api_key,
        temperature=temperature,
        max_tokens=1500
    )


# --- Query Refiner for Conversational Context ---

REFINER_PROMPT = """Given a chat history and a follow-up question, rewrite the follow-up question to be a standalone question that a search engine can understand, incorporating the necessary context from the history.

- If the follow-up is already a complete question, return it as is.
- Incorporate relevant context from the history (like names of items mentioned) into the new question.
- Do NOT answer the question, just reformulate it.

Return ONLY the refined, standalone question.
"""


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


# --- Helper Functions ---

def _record_to_text(doctype: str, row: Dict[str, Any]) -> str:
    """Flattens a record to a text block, giving weight to the title field."""
    all_cols = get_db_columns_for_doctype(doctype)
    title_field = None
    
    # Try to find a title-like field
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
    Now handles None user_profile gracefully.
    
    Args:
        user_profile: Optional user profile dict (can be None)
        content_details: Optional content details dict
    
    Returns:
        Dict of metadata filters or None
    """
    filters = {}
    
    # Only add user-specific filters if user_profile exists and has data
    if user_profile:
        if user_profile.get('grade'):
            filters['grade'] = user_profile['grade']
            print(f"> Grade filter prepared: {user_profile['grade']}")
        
        if user_profile.get('batch'):
            filters['batch'] = user_profile['batch']
            print(f"> Batch filter prepared: {user_profile['batch']}")
        
        if user_profile.get('current_enrollment'):
            enrollment = user_profile['current_enrollment']
            if enrollment.get('course'):
                filters['course'] = enrollment['course']
                print(f"> Course filter prepared: {enrollment['course']}")
    else:
        print("> No user profile - using general search (no grade/batch filtering)")
    
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
    """
    Synthesizes a final answer using retrieved context with optional user personalization.
    
    Args:
        query: User's question
        context_records: Retrieved context from Pinecone/DB
        user_profile: Optional user profile (can be None)
        content_details: Optional content details
        history: Optional conversation history
    
    Returns:
        Generated answer string
    """
    llm = _llm()
    
    # Build system prompt based on available context
    if user_profile and user_profile.get('name'):
        # Personalized system prompt
        system_prompt = f"""You are a helpful educational AI assistant for TAP (Teaching and Learning Platform).

The user is {user_profile.get('name', 'a learner')}.
"""
        if user_profile.get('type'):
            system_prompt += f"User type: {user_profile['type'].title()}. "
        
        if user_profile.get('grade'):
            system_prompt += f"They are in Grade {user_profile['grade']}. "
            system_prompt += "Use age-appropriate language and examples for their grade level. "
        
        system_prompt += """
Provide clear, accurate, and helpful answers using the context below.
Address the user by name when appropriate.
Be encouraging and supportive in your responses.
"""
    else:
        # Generic system prompt for anonymous queries
        system_prompt = """You are a helpful educational AI assistant for TAP (Teaching and Learning Platform).

Provide clear, accurate, and helpful answers using the context below.
Use appropriate language for a general educational audience.
Be encouraging and supportive in your responses.
"""
    
    # Build user prompt
    user_prompt_parts = [f"Question: {query}"]
    
    if context_records:
        user_prompt_parts.append(f"\nRelevant Context:\n{chr(10).join(context_records)}")
    
    if history:
        formatted_history = "\n".join([f"{msg['role']}: {msg['content']}" for msg in history[-3:]])
        user_prompt_parts.append(f"\nConversation History:\n{formatted_history}")
    
    user_prompt_parts.append(
        "\nProvide a helpful, accurate answer based on the context. "
        "Reference specific content from the context when relevant. "
        "If the context doesn't contain enough information, say so."
    )
    
    user_prompt = "\n".join(user_prompt_parts)
    
    try:
        resp = llm.invoke([("system", system_prompt), ("user", user_prompt)])
        return getattr(resp, "content", "I couldn't find a good answer.").strip()
    except Exception as e:
        frappe.log_error(f"RAG answer synthesis failed: {e}")
        return "There was an error while formulating an answer."


# --- Main Function ---

def answer_from_pinecone(
    query: str,
    user_profile: Optional[Dict[str, Any]] = None,
    content_details: Optional[Dict[str, Any]] = None,
    chat_history: Optional[List[Dict[str, str]]] = None
) -> Dict[str, Any]:
    """
    Main Vector RAG entry point with optional user context and grade filtering.
    
    Args:
        query: Natural language question
        user_profile: Optional user profile from DynamicConfig.get_user_profile()
            Contains: name, batch, grade, enrollments, current_enrollment
            Can be None for anonymous queries
        content_details: Optional content details for content-specific queries
        chat_history: Optional conversation history
    
    Returns:
        Dictionary with answer, context used, and success flag
    """
    chat_history = chat_history or []
    start_time = time.time()
    
    print("> Starting Vector RAG process...")
    if user_profile:
        user_name = user_profile.get('name', 'Unknown')
        user_grade = user_profile.get('grade', 'N/A')
        user_batch = user_profile.get('batch', 'N/A')
        print(f"> User Context: {user_name} | Grade {user_grade} | Batch {user_batch}")
    else:
        print("> No user context - anonymous query")
    
    # Step 1: Refine query with conversation history
    refined_query = _refine_query_with_history(query, chat_history)
    
    # Step 2: Build metadata filter for grade/batch (optional)
    metadata_filter = _build_metadata_filter(user_profile, content_details)
    
    # Step 3: Search Pinecone
    try:
        print("> Calling Pinecone search...")
        search_result = search_auto_namespaces(
            q=refined_query,
            k=15,  # Return top 15 results
            route_top_n=5,  # Route to top 5 doctypes
            filters=metadata_filter  # Optional metadata filters
        )
        
        routed_doctypes = search_result.get("routed_doctypes", [])
        all_matches = search_result.get("matches", [])
        
        print(f"> Routed to DocTypes: {routed_doctypes}")
        print(f"> Retrieved {len(all_matches)} results from Pinecone.")
        
        if not all_matches:
            no_results_msg = "I couldn't find relevant information in the database."
            if user_profile and user_profile.get('grade'):
                no_results_msg += f" (searched for Grade {user_profile['grade']} content)"
            
            return {
                "question": query,
                "answer": no_results_msg,
                "routed_doctypes": routed_doctypes,
                "results_count": 0,
                "search_time": round(time.time() - start_time, 2)
            }
        
        # Step 4: Fetch full records from MariaDB
        print("> Fetching full records from database...")
        full_records = []
        
        for match in all_matches[:10]:  # Limit to top 10 for performance
            meta = match.get("metadata", {})
            doctype = meta.get("doctype")
            record_ids = meta.get("record_ids", [])
            
            if not doctype or not record_ids:
                continue
            
            try:
                # Fetch from Frappe DB
                for rid in record_ids[:3]:  # Max 3 records per match
                    doc = frappe.get_doc(doctype, rid)
                    record_text = _record_to_text(doctype, doc.as_dict())
                    full_records.append(record_text)
            except Exception as e:
                frappe.log_error(f"Failed to fetch {doctype}/{record_ids}: {e}")
                # Use embedded text as fallback
                if meta.get("text"):
                    full_records.append(meta["text"])
        
        print(f"> Fetched {len(full_records)} full records")
        
        # Step 5: Synthesize answer with LLM
        print("> Synthesizing answer with LLM...")
        answer = _synthesize_answer_with_context(
            query=query,
            context_records=full_records,
            user_profile=user_profile,
            content_details=content_details,
            history=chat_history
        )
        
        elapsed = round(time.time() - start_time, 2)
        print(f"> RAG process completed in {elapsed}s")
        
        return {
            "question": query,
            "answer": answer,
            "routed_doctypes": routed_doctypes,
            "results_count": len(all_matches),
            "records_used": len(full_records),
            "search_time": elapsed,
            "user_context": "personalized" if user_profile else "general"
        }
        
    except Exception as e:
        frappe.log_error(f"Pinecone RAG failed: {e}")
        return {
            "question": query,
            "answer": "There was an error searching for information. Please try again.",
            "error": str(e),
            "search_time": round(time.time() - start_time, 2)
        }
