# tap_ai/services/rag_answerer.py
"""
Vector RAG Engine - Enhanced with User Context
Performs semantic search with grade/batch filtering for personalized results
"""

import json
import time
from typing import Dict, Any, List, Optional

import frappe
from langchain_openai import ChatOpenAI

from tap_ai.infra.config import get_config
from tap_ai.services.pinecone_store import search_auto_namespaces, get_db_columns_for_doctype
from tap_ai.services.doctype_selector import pick_doctypes


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
    
    Args:
        user_profile: User profile with grade, batch, etc.
        content_details: Content details
    
    Returns:
        Metadata filter dictionary for Pinecone query
    """
    filters = {}
    
    # Add grade filter
    if user_profile and user_profile.get('grade'):
        filters['grade'] = user_profile['grade']
        print(f"> Applying grade filter: {user_profile['grade']}")
    
    # Add batch filter
    if user_profile and user_profile.get('batch'):
        filters['batch'] = user_profile['batch']
        print(f"> Applying batch filter: {user_profile['batch']}")
    
    # Add course filter from enrollment
    if user_profile and user_profile.get('current_enrollment'):
        enrollment = user_profile['current_enrollment']
        if enrollment.get('course'):
            filters['course'] = enrollment['course']
            print(f"> Applying course filter: {enrollment['course']}")
    
    # Add content type filter
    if content_details and content_details.get('type'):
        filters['content_type'] = content_details['type']
        print(f"> Applying content type filter: {content_details['type']}")
    
    return filters if filters else None


def _synthesize_answer_with_context(
    query: str,
    context_records: List[str],
    user_profile: Optional[Dict] = None,
    content_details: Optional[Dict] = None,
    history: Optional[List[Dict[str, str]]] = None
) -> str:
    """
    Synthesizes a final answer using retrieved context with user personalization.
    
    Args:
        query: User's question
        context_records: Retrieved text records from Pinecone
        user_profile: User profile for personalization
        content_details: Content details
        history: Conversation history
    
    Returns:
        Synthesized natural language answer
    """
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
    
    # Build context records string
    context_str = "\n\n---\n\n".join(context_records[:10])  # Use top 10 results
    
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
    
    # Step 2: Select relevant DocTypes
    try:
        selected_doctypes = pick_doctypes(refined_query, top_n=5)
        print(f"> Selected DocTypes: {selected_doctypes}")
    except Exception as e:
        frappe.log_error(f"DocType selection failed: {e}")
        selected_doctypes = ["VideoClass", "Quiz", "Assignment"]  # Fallback
    
    # Step 3: Build metadata filter for grade/batch
    metadata_filter = _build_metadata_filter(user_profile, content_details)
    
    # Step 4: Search Pinecone with filters
    try:
        # Note: search_auto_namespaces needs to be updated to accept metadata_filter
        # For now, we'll search without filter and filter results manually if needed
        raw_results = search_auto_namespaces(
            refined_query,
            selected_doctypes,
            top_k=15  # Get more results for filtering
        )
        
        if not raw_results:
            return {
                "question": query,
                "answer": "I couldn't find relevant information in the database.",
                "context_used": [],
                "success": False
            }
        
        print(f"> Retrieved {len(raw_results)} results from Pinecone.")
        
        # Step 5: Manual grade filtering if needed
        # (This is a temporary solution until search_auto_namespaces supports metadata filtering)
        filtered_results = raw_results
        if user_profile and user_profile.get('grade'):
            user_grade = str(user_profile['grade'])
            # Filter results that match user's grade or are grade-agnostic
            filtered_results = []
            for result in raw_results:
                result_grade = result.get('metadata', {}).get('grade')
                # Include if no grade specified (general content) or if grade matches
                if not result_grade or str(result_grade) == user_grade:
                    filtered_results.append(result)
            
            if filtered_results:
                print(f"> Filtered to {len(filtered_results)} grade-appropriate results (Grade {user_grade}).")
            else:
                # If no exact matches, use all results but note this in response
                filtered_results = raw_results
                print(f"> No exact grade match, using all {len(raw_results)} results.")
        
        # Step 6: Extract text records
        context_records = []
        for res in filtered_results[:10]:  # Use top 10
            metadata = res.get("metadata", {})
            doctype = metadata.get("doctype", "Unknown")
            doc_name = metadata.get("doc_name", "")
            
            # Get the actual record if available
            if doctype and doc_name:
                try:
                    record = frappe.get_doc(doctype, doc_name).as_dict()
                    text_block = _record_to_text(doctype, record)
                    context_records.append(text_block)
                except Exception as e:
                    # If can't fetch record, use metadata
                    text_block = "\n".join([f"{k}: {v}" for k, v in metadata.items()])
                    context_records.append(text_block)
            else:
                # Use metadata directly
                text_block = "\n".join([f"{k}: {v}" for k, v in metadata.items()])
                context_records.append(text_block)
        
        if not context_records:
            return {
                "question": query,
                "answer": "I found results but couldn't extract meaningful content.",
                "context_used": [],
                "success": False
            }
        
        # Step 7: Synthesize answer with user context
        final_answer = _synthesize_answer_with_context(
            query,
            context_records,
            user_profile,
            content_details,
            chat_history
        )
        
        elapsed = time.time() - start_time
        print(f"> RAG completed in {elapsed:.2f}s")
        
        return {
            "question": query,
            "answer": final_answer,
            "context_used": context_records[:5],  # Return top 5 for reference
            "num_results": len(filtered_results),
            "success": True,
            "user_context": {
                "batch": user_profile.get('batch') if user_profile else None,
                "grade": user_profile.get('grade') if user_profile else None,
                "filtered_by_grade": len(filtered_results) != len(raw_results)
            }
        }
        
    except Exception as e:
        frappe.log_error(f"Pinecone search failed: {e}")
        return {
            "question": query,
            "answer": f"An error occurred while searching: {str(e)}",
            "context_used": [],
            "success": False
        }
