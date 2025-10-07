# tap_ai/services/router.py
# Final version with automatic, resilient chat history management.
# MODIFIED to include a user-friendly message during the fallback process.

import json
from typing import Dict, Any, List, Optional

import frappe
from langchain_openai import ChatOpenAI

# --- Tool Imports ---
from tap_ai.infra.config import get_config
from tap_ai.services.sql_answerer import answer_from_sql
from tap_ai.services.rag_answerer import answer_from_pinecone


# --- LLM-based Tool Chooser (Unchanged) ---

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

def choose_tool(query: str) -> str:
    """Uses an LLM to decide which tool (SQL or Vector Search) is best for the query."""
    llm = _llm()
    user_prompt = f"USER QUESTION:\n{query}\n\nWhich tool should be used to answer this?"
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


# --- Main Answer Function (MODIFIED) ---

def answer(q: str, history: Optional[List[Dict[str, str]]] = None) -> dict:
    current_query = q
    primary_tool = choose_tool(current_query)
    print(f"> Selected Primary Tool: {primary_tool}")

    result = {}
    fallback_used = False
    chat_history = history or []

    if primary_tool == "text_to_sql":
        result = answer_from_sql(current_query, chat_history=chat_history)
        if _is_failure(result):
            print("> Text-to-SQL failed. Falling back to Vector Search...")
            fallback_used = True
            # Set an interim message to be sent to the user by the client.
            interim_message = "Searching, please wait a few more seconds..."
            result = answer_from_pinecone(current_query, chat_history=chat_history)
            # Add the message to the final result dictionary.
            result['interim_message'] = interim_message
    else:
        primary_tool = "vector_search"
        result = answer_from_pinecone(current_query, chat_history=chat_history)

    return _with_meta(result, current_query, primary=primary_tool, fallback=fallback_used)


# --- Helper functions (Unchanged) ---

def _is_failure(res: dict) -> bool:
    """Robust failure detector."""
    if not res: return True
    if res.get("success") is False: return True
    text = (res.get("answer") or "").strip().lower()
    bad_phrases = ("i don't know", "unable to", "cannot", "no answer", "failed", "error", "could not generate a valid sql")
    if any(p in text for p in bad_phrases): return True
    return False

def _with_meta(res: dict, original_query: str, primary: str, fallback: bool) -> dict:
    res.setdefault("metadata", {})
    res["metadata"].update({
        "original_query": original_query,
        "primary_engine": primary,
        "fallback_used": fallback,
    })
    if "routed_doctypes" in (res.get("metadata") or {}):
        res["metadata"]["doctypes_used"] = res["metadata"]["routed_doctypes"]
    return res

# --- Resilient Cache & CLI ---

def _get_history_from_cache(user_id: str) -> List[Dict[str, str]]:
    try:
        cache_key = f"chat_history_{user_id}"
        cached_data = frappe.cache().get(cache_key)
        if isinstance(cached_data, bytes):
            cached_data = cached_data.decode('utf-8')
        if isinstance(cached_data, str) and cached_data:
            return json.loads(cached_data)
        return []
    except Exception as e:
        print(f"> [Warning] Failed to retrieve or parse chat history from cache: {e}")
        return []

def _save_history_to_cache(user_id: str, history: List[Dict[str, str]]):
    try:
        cache_key = f"chat_history_{user_id}"
        history_to_save = history[-10:]
        frappe.cache().set(cache_key, json.dumps(history_to_save))
    except Exception as e:
        print(f"\n> [Warning] Failed to save chat history to cache: {e}")
        print("> Conversation memory will not be available for the next turn.")


# --- Bench CLI (HAVING RESILIENT HISTORY) ---
def cli(q: str, user_id: str = "default_user"):
    '''
    Automatically manages conversation history in the Frappe cache with error handling.

    Turn 1:
    bench execute tap_ai.services.router.cli --kwargs "{'q':'list videos with basic difficulty', 'user_id':'user123'}"

    Turn 2 (Follow-up, no history needed):
    bench execute tap_ai.services.router.cli --kwargs "{'q':'summarize the first one', 'user_id':'user123'}"

    bench execute tap_ai.services.router.cli --kwargs "{'q':'list all the videos with easy difficulty', 'user_id':'user123'}"

    bench execute tap_ai.services.router.cli --kwargs "{'q':'list all the activities present', 'user_id':'user123'}"

    bench execute tap_ai.services.router.cli --kwargs "{'q':'Find a video about financial literacy and goal setting and summarize its key points', 'user_id':'user123'}"
    
    '''
    # 1. Get history safely
    history = _get_history_from_cache(user_id)
    
    # 2. Call the main answer function
    out = answer(q, history=history)
    # Handle the interim message for the CLI demo to simulate the bot's behavior.
    if "interim_message" in out:
        print("\n--- INTERIM MESSAGE (Simulating Bot Message) ---")
        print(out['interim_message'])    
    
    # 3. Update the history list
    history.append({"role": "user", "content": q})
    history.append({"role": "assistant", "content": out.get("answer", "")})
    
    # 4. Save history safely
    _save_history_to_cache(user_id, history)

    # Final, user-friendly print
    json_output_with_unicode = json.dumps(out, indent=2, default=str)
    final_output = json_output_with_unicode.replace('\\u20b9', '₹')
    
    print("\n--- CONVERSATION TURN ---")
    print(final_output)
    
    return out
