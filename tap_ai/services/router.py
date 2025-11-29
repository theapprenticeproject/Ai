# tap_ai/services/router.py
"""
TAP AI Router - Enhanced with Dynamic Configuration Support
Routes queries to Text-to-SQL or Vector RAG with full user context
"""

import json
from typing import Dict, Any, List, Optional

import frappe
from langchain_openai import ChatOpenAI

# --- Tool Imports ---
from tap_ai.infra.config import get_config
from tap_ai.services.sql_answerer import answer_from_sql
from tap_ai.services.rag_answerer import answer_from_pinecone


# --- LLM-based Tool Chooser ---

ROUTER_PROMPT = """You are a query routing expert. Your job is to determine the best tool to answer a user's question based on its intent.

You have the following tools available:
1. `text_to_sql`: Best for factual, specific questions that can be answered by querying a structured database. Use this for questions like "list all...", "count...", "how many...", or questions that ask for specific data points with filters (e.g., "list videos with basic difficulty").
2. `vector_search`: Best for conceptual, open-ended, or summarization questions that require understanding unstructured text. Use this for questions like "summarize...", "explain...", "what is...", or "tell me about...".

Based on the user's question, decide which single tool is most appropriate.

Return ONLY a JSON object with this structure:
{
  "tool": "text_to_sql" or "vector_search",
  "reason": "A short explanation for your choice (<= 20 words)."
}
"""

def _llm() -> ChatOpenAI:
    """Initializes the Language Model client."""
    api_key = get_config("openai_api_key")
    model = get_config("primary_llm_model") or "gpt-4o-mini"
    return ChatOpenAI(model_name=model, openai_api_key=api_key, temperature=0.0)


def choose_tool(query: str, user_context: Optional[str] = None) -> str:
    """
    Uses an LLM to decide which tool (SQL or Vector Search) is best for the query.
    
    Args:
        query: The user's question
        user_context: Optional context about the user (grade, batch, etc.)
    
    Returns:
        Tool name: 'text_to_sql' or 'vector_search'
    """
    llm = _llm()
    
    user_prompt = f"USER QUESTION:\n{query}"
    if user_context:
        user_prompt = f"USER CONTEXT:\n{user_context}\n\n{user_prompt}"
    
    user_prompt += "\n\nWhich tool should be used to answer this?"
    
    try:
        resp = llm.invoke([("system", ROUTER_PROMPT), ("user", user_prompt)])
        content = getattr(resp, "content", "")
        if content.startswith("```json"):
            content = content[7:-3].strip()
        data = json.loads(content)
        tool_choice = data.get("tool")
        print(f"> Router Reason: {data.get('reason')}")
        if tool_choice in ["text_to_sql", "vector_search"]:
            return tool_choice
    except Exception as e:
        frappe.log_error(f"Tool router failed: {e}")
    
    print("> Router failed, defaulting to vector_search.")
    return "vector_search"


# --- Main Answer Function (ENHANCED with User Context) ---

def process_query(
    query: str,
    user_profile: Optional[Dict[str, Any]] = None,
    content_details: Optional[Dict[str, Any]] = None,
    context: Optional[Dict[str, Any]] = None,
    history: Optional[List[Dict[str, str]]] = None
) -> dict:
    """
    Main entry point for processing queries with full user context.
    
    Args:
        query: The user's question
        user_profile: User profile from DynamicConfig.get_user_profile()
            Contains: name, phone, batch, grade, enrollments, current_enrollment
        content_details: Content details from get_content_details()
            Contains: id, title, duration, difficulty, etc.
        context: Additional context dictionary
        history: Conversation history
    
    Returns:
        Dictionary with answer, metadata, and tool used
    """
    current_query = query
    chat_history = history or []
    
    # Build enriched user context for better routing and responses
    user_context_str = None
    if user_profile:
        context_parts = [f"User: {user_profile.get('name', 'Unknown')}"]
        
        if user_profile.get('grade'):
            context_parts.append(f"Grade: {user_profile['grade']}")
        
        if user_profile.get('batch'):
            context_parts.append(f"Batch: {user_profile['batch']}")
        
        # Add enrollment info if available
        if user_profile.get('current_enrollment'):
            enrollment = user_profile['current_enrollment']
            if enrollment.get('course'):
                context_parts.append(f"Course: {enrollment['course']}")
            if enrollment.get('school'):
                context_parts.append(f"School: {enrollment['school']}")
        
        user_context_str = " | ".join(context_parts)
        print(f"> User Context: {user_context_str}")
    
    # Add content context if available
    if content_details:
        content_context = f"Content: {content_details.get('title', 'Unknown')}"
        if content_details.get('type'):
            content_context += f" (Type: {content_details['type']})"
        print(f"> Content Context: {content_context}")
        if user_context_str:
            user_context_str += f"\n{content_context}"
        else:
            user_context_str = content_context
    
    # Choose tool with enhanced context
    primary_tool = choose_tool(current_query, user_context_str)
    print(f"> Selected Primary Tool: {primary_tool}")

    result = {}
    fallback_used = False

    if primary_tool == "text_to_sql":
        result = answer_from_sql(
            current_query,
            user_profile=user_profile,
            content_details=content_details,
            chat_history=chat_history
        )
        
        if _is_failure(result):
            print("> Text-to-SQL failed. Falling back to Vector Search...")
            fallback_used = True
            interim_message = "Searching, please wait a few more seconds..."
            result = answer_from_pinecone(
                current_query,
                user_profile=user_profile,
                content_details=content_details,
                chat_history=chat_history
            )
            result['interim_message'] = interim_message
    else:
        primary_tool = "vector_search"
        result = answer_from_pinecone(
            current_query,
            user_profile=user_profile,
            content_details=content_details,
            chat_history=chat_history
        )

    return _with_meta(result, current_query, primary=primary_tool, fallback=fallback_used)


# --- Backward Compatibility Wrapper ---

def answer(q: str, history: Optional[List[Dict[str, str]]] = None) -> dict:
    """
    Backward compatibility wrapper for old function signature.
    
    This maintains compatibility with existing code that calls answer()
    without user context. New code should use process_query() instead.
    
    Args:
        q: Query string
        history: Optional conversation history
    
    Returns:
        Query result dictionary
    """
    print("> WARNING: Using legacy answer() function without user context.")
    print("> Consider updating to process_query() for personalized responses.")
    return process_query(q, user_profile=None, content_details=None, history=history)


# --- Helper functions ---

def _is_failure(res: dict) -> bool:
    """Robust failure detector."""
    if not res:
        return True
    if res.get("success") is False:
        return True
    answer = res.get("answer", "")
    if not answer or len(answer.strip()) < 10:
        return True
    # Check for explicit error indicators
    error_phrases = [
        "could not generate",
        "failed to execute",
        "no results",
        "error occurred",
        "unable to",
    ]
    answer_lower = answer.lower()
    return any(phrase in answer_lower for phrase in error_phrases)


def _with_meta(result: dict, query: str, primary: str, fallback: bool) -> dict:
    """Adds metadata to the result."""
    result["query"] = query
    result["tool_used"] = primary
    result["fallback_used"] = fallback
    return result


# --- CLI for Testing ---

def cli(q: str = None, user_id: str = "test_user", **kwargs):
    """
    Command-line interface for testing with automatic history management.
    
    Usage:
        # Simple query
        bench execute tap_ai.services.router.cli --kwargs "{'q':'list videos', 'user_id':'test_user_1'}"
        
        # With user context
        bench execute tap_ai.services.router.cli --kwargs "{'q':'list videos', 'user_id':'student123', 'user_type':'student', 'glific_id':'12345'}"
    
    Args:
        q: Query string
        user_id: Unique user ID for conversation tracking
        **kwargs: Additional parameters like user_type, glific_id, etc.
    """
    if not q:
        print("ERROR: No query provided. Use: --kwargs \"{'q':'your question', 'user_id':'user123'}\"")
        return None

    import hashlib
    cache_key = f"chat_history:{hashlib.md5(user_id.encode()).hexdigest()}"

    # Load conversation history
    history = frappe.cache().get_value(cache_key) or []
    print(f"\n> User: {user_id}")
    print(f"> Loaded {len(history)} previous turn(s) from cache.")
    
    # Get user profile if user_type and glific_id provided
    user_profile = None
    if kwargs.get('user_type') and kwargs.get('glific_id'):
        from tap_ai.utils.dynamic_config import DynamicConfig
        user_profile = DynamicConfig.get_user_profile(
            kwargs['user_type'],
            kwargs['glific_id'],
            kwargs.get('batch_id')
        )
        if user_profile:
            print(f"> User Profile: {user_profile.get('name')} (Grade {user_profile.get('grade')}, Batch {user_profile.get('batch')})")

    # Process query with context
    result = process_query(q, user_profile=user_profile, history=history)

    # Update history
    history.append({"role": "user", "content": q})
    history.append({"role": "assistant", "content": result.get("answer", "")})
    frappe.cache().set_value(cache_key, history, expires_in_sec=3600)

    # Display results
    print("\n" + "="*70)
    print(f"QUESTION: {result.get('query')}")
    print(f"TOOL USED: {result.get('tool_used')}")
    if result.get('fallback_used'):
        print("(Fallback was used)")
    print("-"*70)
    print(f"ANSWER:\n{result.get('answer')}")
    print("="*70 + "\n")

    return result
