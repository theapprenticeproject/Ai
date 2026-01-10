# tap_ai/api/query.py

import frappe
import json
import uuid

from tap_ai.utils.dynamic_config import DynamicConfig, get_content_details
from tap_ai.services.router import process_query
from tap_ai.services.ratelimit import check_rate_limit


# -------------------------------------------------------------------
# QUERY API (ASYNC ENTRY POINT)
# -------------------------------------------------------------------

@frappe.whitelist(methods=["POST"], allow_guest=True)
def query():
    """
    Async Query API.
    Accepts query + optional user context.
    Returns request_id immediately.
    """
    data = frappe.local.form_dict or {}
    q = data.get("q")

    if not q:
        frappe.throw("Missing required parameter: q")

    # ------------------------------
    # Rate limiting
    # ------------------------------
    user_id = data.get("user_id") or frappe.session.user

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

    # ------------------------------
    # Generate request_id
    # ------------------------------
    request_id = f"REQ_{uuid.uuid4().hex[:10]}"

    # Save pending state
    frappe.cache().set(
        request_id,
        json.dumps({
            "status": "pending",
            "query": q,
            "answer": None,
            "user_id": user_id
        })
       
    )

    # ------------------------------
    # Enqueue background job
    # ------------------------------
    frappe.enqueue(
        "tap_ai.api.query._process_query",
        queue="long",
        timeout=300,
        request_id=request_id,
        payload=data
    )

    return {
        "success": True,
        "request_id": request_id,
        "status": "pending"
    }


# -------------------------------------------------------------------
# BACKGROUND WORKER
# -------------------------------------------------------------------

def _process_query(request_id: str, payload: dict):
    """
    Background worker that:
    - builds user context
    - calls router.process_query
    - stores result in cache
    """
    try:
        # ------------------------------
        # Extract inputs
        # ------------------------------
        query_text = payload.get("q")
        user_type = payload.get("user_type")
        glific_id = payload.get("glific_id")
        phone = payload.get("phone")
        name = payload.get("name")
        batch_id = payload.get("batch_id")
        context = payload.get("context", {})

        # ------------------------------
        # Build user profile (graceful)
        # ------------------------------
        user_profile = None
        user_context_level = "none"

        if user_type and glific_id:
            try:
                user_profile = DynamicConfig.get_user_profile(
                    user_type, glific_id, batch_id
                )
                user_context_level = "full" if user_profile else "partial"
            except Exception:
                user_context_level = "partial"

        if not user_profile and user_type:
            user_profile = {
                "type": user_type,
                "name": name or "there",
                "phone": phone,
                "glific_id": glific_id,
                "grade": None,
                "batch": None,
            }

        # ------------------------------
        # Content details (optional)
        # ------------------------------
        content_details = None
        if context.get("content_type") and context.get("content_id"):
            try:
                content_details = get_content_details(
                    context["content_type"],
                    context["content_id"]
                )
            except Exception:
                content_details = None

        # ------------------------------
        # Call router (CORE)
        # ------------------------------
        result = process_query(
            query=query_text,
            user_profile=user_profile,
            content_details=content_details,
            chat_history=[]
        )

        # ------------------------------
        # Save success result
        # ------------------------------
        frappe.cache().set(
            request_id,
            json.dumps({
                "status": "success",
                "answer": result.get("answer"),
                "tool_used": result.get("tool_used"),
                "metadata": result,
                "user_context_level": user_context_level
            })
        )

    except Exception as e:
        frappe.cache().set(
            request_id,
            json.dumps({
                "status": "failed",
                "error": str(e)
            })
        )

        frappe.log_error(
            f"_process_query failed: {e}",
            "TAP AI Query Worker"
        )


# -------------------------------------------------------------------
# RESULT API
# -------------------------------------------------------------------

@frappe.whitelist(allow_guest=True)
def result(request_id: str):
    """
    Result API.
    Client polls this using request_id.
    """
    if not request_id:
        return {"success": False, "error": "request_id required"}

    cached = frappe.cache().get(request_id)
    if not cached:
        return {"success": False, "error": "Invalid or expired request_id"}

    if isinstance(cached, bytes):
        cached = cached.decode("utf-8")

    data = json.loads(cached)
    return {
        "success": True,
        **data
    }
