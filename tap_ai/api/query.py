import frappe
import json
import uuid
import time
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
    user_id = data.get("user_id")
    if not user_id or user_id == "Guest":
        user_id = frappe.session.user
    return user_id


@frappe.whitelist(methods=["POST"], allow_guest=True)
def query():
    """
    Unified Query API with backend polling.

    Accepts:
    - q (text input)
    - audio_url (voice input)

    Returns:
    - success (with answer) if ready within wait window
    - processing (with request_id) if not ready
    """

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

    # Rate limiting
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
        message = (
            f"Voice query rate limit exceeded. Try again in {reset} seconds."
            if is_voice
            else f"Rate limit exceeded. Try again in {reset} seconds."
        )
        frappe.throw(message, frappe.TooManyRequestsError)

    # Generate request_id
    request_prefix = "VREQ" if is_voice else "REQ"
    request_id = f"{request_prefix}_{uuid.uuid4().hex[:8]}"

    # Initial state
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

    frappe.cache().set(request_id, json.dumps(state), ex=3600)

    # Publish to queue
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

    # -------------------------------
    # Backend Polling (Key Upgrade)
    # -------------------------------

    MAX_WAIT_SECONDS = 9
    POLL_INTERVAL = 0.3

    start = time.time()

    while time.time() - start < MAX_WAIT_SECONDS:
        cached = frappe.cache().get(request_id)
        if cached:
            try:
                result = json.loads(cached)
            except Exception:
                break

            status = result.get("status")

            if status == "success":
                return {
                    "message": {
                        "status": "success",
                        "answer": result.get("answer") or result.get("answer_text"),
                        "audio_url": result.get("audio_url"),
                        "request_id": request_id,
                    }
                }

            if status == "failed":
                return {
                    "message": {
                        "status": "failed",
                        "error": result.get("error"),
                        "request_id": request_id,
                    }
                }

        time.sleep(POLL_INTERVAL)

    # Fallback if not ready
    return {
        "message": {
            "status": "processing",
            "request_id": request_id,
        }
    }