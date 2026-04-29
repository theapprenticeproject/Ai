import frappe
import json
import uuid
from tap_ai.services.ratelimit import check_rate_limit
from tap_ai.utils.mq import publish_to_queue


def _extract_api_key() -> str | None:
    auth = frappe.get_request_header("Authorization") or ""
    if not auth.lower().startswith("token "):
        return None
    try:
        return auth.split()[1].split(":")[0]
    except Exception:
        return None


def _resolve_user_id(data: dict) -> str:
    # Keep compatibility with existing clients, but avoid trusting Guest placeholders.
    user_id = data.get("user_id")
    if not user_id or user_id == "Guest":
        user_id = frappe.session.user
    return user_id


@frappe.whitelist(methods=["POST"], allow_guest=True)
def query():
    """
    Unified Query API.
    Accepts either:
    - q (text input), or
    - audio_url (voice input)

    Returns request_id immediately. Processing is handled by RabbitMQ workers.
    """
    try:
        data = frappe.local.form_dict or {}
        q = (data.get("q") or "").strip()
        audio_url = (data.get("audio_url") or "").strip()
        user_id = _resolve_user_id(data)
        session_id = data.get("session_id")

        if not q and not audio_url:
            frappe.throw("Provide one input in POST body: q (text) or audio_url (voice).")
        if q and audio_url:
            frappe.throw("Provide only one input per request: q or audio_url, not both.")

        is_voice = bool(audio_url)

        # Voice requests are costlier, so use a lower limit.
        api_key = _extract_api_key()
        scope = f"voice_api_{user_id}" if is_voice else f"query_api_{user_id}"
        limit = 30 if is_voice else 60

        ok, remaining, reset = check_rate_limit(
            api_key=api_key,
            scope=scope,
            limit=limit,
            window_sec=60
        )
        if not ok:
            if is_voice:
                message = f"Voice query rate limit exceeded. Try again in {reset} seconds."
            else:
                message = f"Rate limit exceeded. Try again in {reset} seconds."
            frappe.throw(
                message,
                frappe.TooManyRequestsError,
            )

        request_prefix = "VREQ" if is_voice else "REQ"

        # Create a unique request id for every incoming request
        request_id = f"{request_prefix}_{uuid.uuid4().hex[:8]}"

        state = {
            "status": "pending",
            "user_id": user_id,
            "mode": "voice" if is_voice else "text",
        }

        if is_voice:
            state["audio_url"] = audio_url
        else:
            state.update({
                "answer": None,
                "query": q,
                "history": [],
            })

        # Keep a bounded TTL for both request types.
        frappe.cache().set(request_id, json.dumps(state), ex=3600)

        if is_voice:
            payload = {
                "request_id": request_id,
                "audio_url": audio_url,
                "user_id": user_id,
            }
            if session_id:
                payload["session_id"] = session_id
            publish_to_queue("audio_stt_queue", payload)
        else:
            payload = {
                "request_id": request_id,
                "query": q,
                "user_id": user_id,
            }
            if session_id:
                payload["session_id"] = session_id
            publish_to_queue("text_query_queue", payload)

        return {"request_id": request_id}
    except frappe.TooManyRequestsError:
        # Re-raise rate limit errors so they propagate with proper status
        raise
    except Exception as e:
        # Log and return a safe non-empty response
        try:
            frappe.log_error(str(e), "Query API Error")
        except Exception:
            pass
        return {"error": str(e), "status": "failed"}