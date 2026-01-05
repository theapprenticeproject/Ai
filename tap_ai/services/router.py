# tap_ai/services/router.py
"""
TAP AI Router - Enhanced with Optional User Context
Intelligently routes queries to the best tool (SQL or RAG) with optional user context
"""

import json
from typing import Dict, Any, List, Optional

import frappe
from langchain_openai import ChatOpenAI

from tap_ai.infra.config import get_config


# --- LLM Initialization ---

def _llm() -> ChatOpenAI:
    """Initialize LLM for routing decisions."""
    api_key = get_config("openai_api_key")
    model = get_config("primary_llm_model") or "gpt-4o-mini"
    return ChatOpenAI(
        model_name=model,
        openai_api_key=api_key,
        temperature=0.0,
        max_tokens=200
    )


# --- Routing Decision ---

ROUTER_PROMPT = """You are a query routing assistant for an educational platform.

Given a user's query, decide which tool to use:
- text_to_sql: For factual, specific queries about data (list, show, find, how many, which, what are)
- vector_search: For conceptual, explanatory queries (explain, why, how does, summarize, tell me about)

Return ONLY a JSON object:
{
  "tool": "text_to_sql" or "vector_search",
  "reason": "brief explanation"
}

Example:
Query: "list all videos about budgeting"
Response: {"tool": "text_to_sql", "reason": "Listing specific content from database"}

Query: "explain how budgeting works"
Response: {"tool": "vector_search", "reason": "Conceptual explanation needed"}
"""


def _choose_tool(query: str, user_profile: Optional[Dict] = None) -> Dict[str, str]:
    """
    Uses LLM to choose the best tool for the query.
    
    Args:
        query: User's natural language question
        user_profile: Optional user profile (for context)
    
    Returns:
        Dict with 'tool' and 'reason'
    """
    llm = _llm()
    
    # Build context string
    context_parts = [f"QUERY: {query}"]
    
    if user_profile:
        if user_profile.get('type'):
            context_parts.append(f"USER TYPE: {user_profile['type']}")
        if user_profile.get('grade'):
            context_parts.append(f"GRADE: {user_profile['grade']}")
    else:
        context_parts.append("USER: Anonymous")
    
    user_prompt = "\n".join(context_parts)
    
    try:
        resp = llm.invoke([
            ("system", ROUTER_PROMPT),
            ("user", user_prompt)
        ])
        
        content = getattr(resp, "content", "").strip()
        
        # Clean up response
        content = content.replace("```json", "").replace("```", "").strip()
        
        # Parse JSON
        decision = json.loads(content)
        tool = decision.get("tool", "vector_search")
        reason = decision.get("reason", "Default routing")
        
        print(f"> Router Decision: {tool} - {reason}")
        
        return {
            "tool": tool,
            "reason": reason
        }
        
    except Exception as e:
        frappe.log_error(f"Router decision failed: {e}")
        # Default to vector_search on error
        return {
            "tool": "vector_search",
            "reason": "Fallback due to routing error"
        }


# --- Main Processing Function ---

def process_query(
    query: str,
    user_profile: Optional[Dict[str, Any]] = None,
    content_details: Optional[Dict[str, Any]] = None,
    chat_history: Optional[List[Dict[str, str]]] = None,
    context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Main query processing function with optional user context.
    Routes to appropriate tool and returns answer.
    
    Args:
        query: Natural language question
        user_profile: Optional user profile (can be None)
        content_details: Optional content details
        chat_history: Optional conversation history
        context: Optional additional context
    
    Returns:
        Dict with answer and metadata
    """
    print(f"\n{'='*60}")
    print(f"PROCESSING QUERY: {query}")
    if user_profile:
        print(f"User: {user_profile.get('name', 'Unknown')} ({user_profile.get('type', 'unknown')})")
    else:
        print("User: Anonymous")
    print(f"{'='*60}\n")
    
    # Import tools
    from tap_ai.services.sql_answerer import answer_from_sql
    from tap_ai.services.rag_answerer import answer_from_pinecone
    
    # Step 1: Choose tool
    routing_decision = _choose_tool(query, user_profile)
    chosen_tool = routing_decision["tool"]
    
    # Step 2: Try primary tool
    try:
        if chosen_tool == "text_to_sql":
            print("> Using Text-to-SQL engine...")
            result = answer_from_sql(
                query=query,
                user_profile=user_profile,
                content_details=content_details,
                chat_history=chat_history or []
            )
            
            # Check if SQL succeeded
            if result.get("results_count", 0) > 0:
                return {
                    "answer": result["answer"],
                    "tool_used": "text_to_sql",
                    "sql_query": result.get("sql_query"),
                    "results_count": result["results_count"],
                    "execution_time": result.get("execution_time"),
                    "routing_reason": routing_decision["reason"]
                }
            else:
                # SQL returned no results, try RAG as fallback
                print("> SQL returned no results, falling back to RAG...")
                result = answer_from_pinecone(
                    query=query,
                    user_profile=user_profile,
                    content_details=content_details,
                    chat_history=chat_history or []
                )
                
                return {
                    "answer": result["answer"],
                    "tool_used": "vector_search",
                    "fallback_from": "text_to_sql",
                    "routed_doctypes": result.get("routed_doctypes", []),
                    "results_count": result.get("results_count", 0),
                    "search_time": result.get("search_time"),
                    "routing_reason": routing_decision["reason"]
                }
        
        else:  # vector_search
            print("> Using Vector RAG engine...")
            result = answer_from_pinecone(
                query=query,
                user_profile=user_profile,
                content_details=content_details,
                chat_history=chat_history or []
            )
            
            return {
                "answer": result["answer"],
                "tool_used": "vector_search",
                "routed_doctypes": result.get("routed_doctypes", []),
                "results_count": result.get("results_count", 0),
                "search_time": result.get("search_time"),
                "routing_reason": routing_decision["reason"]
            }
    
    except Exception as e:
        error_msg = str(e)
        frappe.log_error(f"Query processing failed: {error_msg}\nQuery: {query}")
        
        # Try fallback tool on error
        print(f"> Error with {chosen_tool}, trying fallback...")
        
        try:
            if chosen_tool == "text_to_sql":
                # Fallback to RAG
                result = answer_from_pinecone(
                    query=query,
                    user_profile=user_profile,
                    content_details=content_details,
                    chat_history=chat_history or []
                )
                
                return {
                    "answer": result["answer"],
                    "tool_used": "vector_search",
                    "fallback_from": "text_to_sql",
                    "fallback_reason": "Error in primary tool",
                    "routed_doctypes": result.get("routed_doctypes", []),
                    "results_count": result.get("results_count", 0)
                }
            else:
                # Fallback to SQL
                result = answer_from_sql(
                    query=query,
                    user_profile=user_profile,
                    content_details=content_details,
                    chat_history=chat_history or []
                )
                
                return {
                    "answer": result["answer"],
                    "tool_used": "text_to_sql",
                    "fallback_from": "vector_search",
                    "fallback_reason": "Error in primary tool",
                    "sql_query": result.get("sql_query"),
                    "results_count": result.get("results_count", 0)
                }
        
        except Exception as fallback_error:
            frappe.log_error(f"Fallback also failed: {str(fallback_error)}")
            
            # Return generic error message
            return {
                "answer": "I'm sorry, I encountered an error processing your query. Please try rephrasing your question or try again later.",
                "tool_used": "error",
                "error": error_msg,
                "fallback_error": str(fallback_error)
            }


# --- CLI Function for Testing ---

def cli(q: str, user_id: str = "test_user"):
    """
    Command-line interface for testing the router.
    
    Usage:
        bench execute tap_ai.services.router.cli --kwargs "{'q': 'your question', 'user_id': 'test'}"
    """
    print(f"\n{'='*80}")
    print(f"TAP AI ROUTER - CLI TEST")
    print(f"{'='*80}\n")
    print(f"Query: {q}")
    print(f"User ID: {user_id}")
    print()
    
    # For CLI testing, use anonymous context
    result = process_query(
        query=q,
        user_profile=None,  # Anonymous for CLI
        content_details=None,
        chat_history=[],
        context={'user_id': user_id}
    )
    
    print(f"\n{'='*80}")
    print(f"RESULT:")
    print(f"{'='*80}")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print()
    
    return result
