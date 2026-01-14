# tap_ai/api/query.py

import frappe
import json
import uuid
from tap_ai.services.router import process_query as route_query
from tap_ai.services.ratelimit import check_rate_limit

@frappe.whitelist(methods=["POST"], allow_guest=True)
def query():
    """
    Query API: Accepts 'q' and optional 'user_id'.
    Returns: request_id immediately. Actual answer is computed in the background.
    """
    user_id = frappe.session.user
    data = frappe.local.form_dict or {}
    q = data.get("q")
    
    if data.get("user_id"):
        user_id = data.get("user_id")

    if not q:
        frappe.throw("Missing required parameter in POST body: q (the user's question)")

    # Rate limiting 
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

    # Generate request ID 
    request_id = f"REQ_{uuid.uuid4().hex[:8]}"

    # Save request in cache as 'pending'
    frappe.cache().set(request_id, json.dumps({
        "status": "pending",
        "answer": None,
        "query": q,
        "user_id": user_id,
        "history": []
    }))  

    # Trigger background processing
    frappe.enqueue("tap_ai.api.query._process_query",
                   queue="long",
                   timeout=300,
                   request_id=request_id,
                   query=q,
                   user_id=user_id)

    return {"request_id": request_id}


def _process_query(request_id: str, query: str, user_id: str):
    """Background job to compute the answer and update cache."""
    import frappe, json
    from tap_ai.services.router import process_query as route_query

    chat_history_key = f"chat_history_{user_id}"
    chat_history = []

    try:
        # --- Fetch previous chat history safely ---
        cached_history = frappe.cache().get(chat_history_key)
        if cached_history:
            if isinstance(cached_history, bytes):
                cached_history = cached_history.decode("utf-8")
            try:
                chat_history = json.loads(cached_history)
                if not isinstance(chat_history, list):
                    chat_history = []
            except Exception:
                chat_history = []

        # Call router/LLM logic 
        out = route_query(query, chat_history=chat_history)

        # Update history (last 10 messages) 
        chat_history.append({"role": "user", "content": query})
        chat_history.append({"role": "assistant", "content": out.get("answer", "")})
        frappe.cache().set(chat_history_key, json.dumps(chat_history[-10:]))

        # Store routed_doctypes in cache with request
        metadata = out.get("metadata", {})
        routed_doctypes = metadata.get("routed_doctypes", [])

        # Update request in cache as 'success'
        frappe.cache().set(request_id, json.dumps({
            "status": "success",
            "answer": out.get("answer"),
            "query": query,
            "user_id": user_id,
            "history": chat_history[-10:],  # last 10
            "routed_doctypes": routed_doctypes
        }))

        frappe.log_error(f"_process_query success for {request_id}", "Query Debug")

    except Exception as e:
        frappe.cache().set(request_id, json.dumps({
            "status": "failed",
            "answer": None,
            "query": query,
            "user_id": user_id,
            "error": str(e),
            "history": chat_history[-10:]
        }))
        frappe.log_error(f"_process_query failed for {request_id}: {e}", "Query Debug")