# tap_ai/api/history.py
"""
Conversation history management endpoints.

POST /api/method/tap_ai.api.history.clear
  Body: { "user_id": "...", "session_id": "..." (optional) }

Clears the Redis chat history for a user/session so the next query
starts a fresh conversation. Useful for testing, support, and
explicit "new conversation" actions on the client side.
"""

import frappe
from loguru import logger
from tap_ai.services.routing.router import _cache_key


@frappe.whitelist(methods=["POST"], allow_guest=True)
def clear():
    """
    Clear chat history for a user (and optionally a specific session).

    Deletes both the Redis list (preferred path) and the legacy JSON blob
    under the same key so both cache backends are cleaned up.
    """
    data = frappe.local.form_dict or {}
    user_id = (data.get("user_id") or "").strip()
    session_id = (data.get("session_id") or "").strip() or None

    if not user_id:
        frappe.throw("user_id is required.")

    key = _cache_key(user_id, session_id)
    cache = frappe.cache()

    try:
        cache.delete(key)
        logger.info(f"Chat history cleared: user_id={user_id} session_id={session_id} key={key}")
    except Exception as e:
        logger.error(f"Failed to clear chat history for {user_id}: {e}")
        frappe.local.response["http_status_code"] = 500
        frappe.throw(f"Failed to clear history: {e}")

    return {
        "status": "ok",
        "user_id": user_id,
        "session_id": session_id,
        "cleared_key": key,
    }
