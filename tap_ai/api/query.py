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


def _to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

@frappe.whitelist(methods=["POST"], allow_guest=True)
def query():
    """
    Unified Query API.
    Accepts one of:
    - q (text input)
    - transcribed_text (already transcribed voice input)
    - audio_url (voice input for STT pipeline)

    Returns request_id immediately. Processing is handled by RabbitMQ workers.
    """
    data = frappe.local.form_dict or {}
    q = (data.get("q") or "").strip()
    transcribed_text = (data.get("transcribed_text") or "").strip()
    audio_url = (data.get("audio_url") or "").strip()
    language = (data.get("language") or "en").strip().lower()
    user_id = _resolve_user_id(data)
    session_id = data.get("session_id")
    force_voice = _to_bool(data.get("is_voice"))

    provided_inputs = [bool(q), bool(transcribed_text), bool(audio_url)]
    if sum(provided_inputs) == 0:
        frappe.throw("Provide one input in POST body: q, transcribed_text, or audio_url.")
    if sum(provided_inputs) > 1:
        frappe.throw("Provide only one input per request: q, transcribed_text, or audio_url.")

    has_transcribed_text = bool(transcribed_text)
    query_text = q or transcribed_text

    # transcribed_text is treated as voice input so it can continue to TTS when needed.
    is_voice = bool(audio_url) or has_transcribed_text or force_voice

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
    request_id = f"{request_prefix}_{uuid.uuid4().hex[:8]}"

    state = {
        "status": "pending",
        "user_id": user_id,
        "mode": "voice" if is_voice else "text",
    }

    if bool(audio_url):
        state["audio_url"] = audio_url
    elif has_transcribed_text:
        state["transcribed_text"] = transcribed_text
        state["language"] = language
    else:
        state.update({
            "answer": None,
            "query": query_text,
            "history": [],
        })

    # Keep a bounded TTL for both request types.
    frappe.cache().set(request_id, json.dumps(state), ex=3600)

    if bool(audio_url):
        payload = {
            "request_id": request_id,
            "audio_url": audio_url,
            "user_id": user_id,
        }
        if session_id:
            payload["session_id"] = session_id
        publish_to_queue("audio_stt_queue", payload)
    elif has_transcribed_text:
        payload = {
            "request_id": request_id,
            "query": transcribed_text,
            "transcribed_text": transcribed_text,
            "user_id": user_id,
            "is_voice": True,
            "language": language,
        }
        if session_id:
            payload["session_id"] = session_id
        publish_to_queue("text_query_queue", payload)
    else:
        payload = {
            "request_id": request_id,
            "query": query_text,
            "user_id": user_id,
        }
        if session_id:
            payload["session_id"] = session_id
        publish_to_queue("text_query_queue", payload)

    return {"request_id": request_id}