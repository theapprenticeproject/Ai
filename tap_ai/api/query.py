import frappe
import json
import uuid
from tap_ai.services.ratelimit import check_rate_limit
from tap_ai.utils.mq import publish_to_queue

@frappe.whitelist(methods=["POST"], allow_guest=True)
def query():
    """
    Query API: Accepts 'q' and optional 'user_id'.
    Returns: request_id immediately. Actual answer is computed by a RabbitMQ worker.
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
        "history": [] # The worker will handle populating the actual history
    }))  

    # Construct payload for the RabbitMQ worker
    payload = {
        "request_id": request_id,
        "query": q,
        "user_id": user_id
    }

    # Trigger background processing via RabbitMQ
    publish_to_queue("text_query_queue", payload)

    return {"request_id": request_id}