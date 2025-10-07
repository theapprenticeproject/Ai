# tap_ai/api/query.py

import frappe
import json
from typing import Dict, Any, List

# Main entry point for the entire answering pipeline
from tap_ai.services.router import answer as route_query
from tap_ai.services.ratelimit import check_rate_limit

# --- Resilient Cache Helper Functions ---
def _get_history_from_cache(user_id: str) -> List[Dict[str, str]]:
    """Safely retrieves and decodes chat history from the cache for a given user."""
    try:
        cache_key = f"chat_history_{user_id}"
        cached_data = frappe.cache().get(cache_key)
        if isinstance(cached_data, bytes):
            cached_data = cached_data.decode('utf-8')
        if isinstance(cached_data, str) and cached_data:
            return json.loads(cached_data)
        return []
    except Exception as e:
        frappe.log_error(f"Failed to retrieve chat history for {user_id}: {e}")
        return []

def _save_history_to_cache(user_id: str, history: List[Dict[str, str]]):
    """Safely serializes and saves chat history to the cache."""
    try:
        cache_key = f"chat_history_{user_id}"
        history_to_save = history[-10:]
        frappe.cache().set(cache_key, json.dumps(history_to_save))
    except Exception as e:
        frappe.log_error(f"Failed to save chat history for {user_id}: {e}")
        print(f"> [Warning] Failed to save chat history for user {user_id}")

# --- API Endpoint ---
@frappe.whitelist(methods=["POST"], allow_guest=True)
def query():
    """
    Public API endpoint for the conversational assistant.
    Accepts a POST request with a JSON body containing 'q' and optional 'user_id'.
    """
    user_id = frappe.session.user

    data = frappe.local.form_dict or {}
    q = data.get("q")
    
    if data.get("user_id"):
        user_id = data.get("user_id")

    if not q:
        frappe.throw("Missing required parameter in POST body: q (the user's question)")

    # --- Rate Limiting ---
    auth = frappe.get_request_header("Authorization") or ""
    api_key = None
    if auth.lower().startswith("token "):
        try:
            api_key = auth.split()[1].split(":")[0]
        except Exception:
            api_key = None

    ok, remaining, reset = check_rate_limit(
        api_key=api_key,
        scope=f"query_api_{user_id}", 
        limit=60, 
        window_sec=60
    )
    if not ok:
        frappe.throw(
            f"Rate limit exceeded. Try again in {reset} seconds.",
            frappe.TooManyRequestsError,
        )

    # --- Main Conversational Logic ---
    chat_history = _get_history_from_cache(user_id)
    out = route_query(q, history=chat_history)
    
    chat_history.append({"role": "user", "content": q})
    chat_history.append({"role": "assistant", "content": out.get("answer", "")})
    
    _save_history_to_cache(user_id, chat_history)

    # --- Format and Return Response ---
    if hasattr(frappe.local, "response") and isinstance(frappe.local.response.headers, dict):
        frappe.local.response.headers["X-RateLimit-Limit"] = 60
        frappe.local.response.headers["X-RateLimit-Remaining"] = remaining
    
    return out