import frappe
import json
import uuid
import hashlib
import time
from tap_ai.services.ratelimit import check_rate_limit
from tap_ai.utils.mq import publish_to_queue

#  OPTIMIZATION: Request deduplication 
DEDUP_WINDOW_SEC = 3  # 3-second window for dedup

# Polling constants
MAX_WAIT_SECONDS = 55
MIN_POLL_INTERVAL_MS = 100
MAX_POLL_INTERVAL_MS = 2000

AUTO_TEXT_WAIT_SECONDS = 8
AUTO_VOICE_WAIT_SECONDS = 25
AUTO_TEXT_POLL_INTERVAL_MS = 300
AUTO_VOICE_POLL_INTERVAL_MS = 500


def _to_int(value, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(min(parsed, max_value), min_value)


def _resolve_wait_seconds(wait_seconds, is_voice: bool) -> int:
    if wait_seconds is None or wait_seconds == "":
        auto_wait = AUTO_VOICE_WAIT_SECONDS if is_voice else AUTO_TEXT_WAIT_SECONDS
        return _to_int(auto_wait, default=0, min_value=0, max_value=MAX_WAIT_SECONDS)
    return _to_int(wait_seconds, default=0, min_value=0, max_value=MAX_WAIT_SECONDS)


def _resolve_poll_interval_ms(poll_interval_ms, is_voice: bool) -> int:
    if poll_interval_ms is None or poll_interval_ms == "":
        auto_interval = AUTO_VOICE_POLL_INTERVAL_MS if is_voice else AUTO_TEXT_POLL_INTERVAL_MS
        return _to_int(
            auto_interval,
            default=500,
            min_value=MIN_POLL_INTERVAL_MS,
            max_value=MAX_POLL_INTERVAL_MS,
        )
    return _to_int(
        poll_interval_ms,
        default=500,
        min_value=MIN_POLL_INTERVAL_MS,
        max_value=MAX_POLL_INTERVAL_MS,
    )


def _safe_load_cache_payload(cached) -> tuple[dict | None, str | None]:
    if cached is None:
        return None, "No cached payload found"
    if isinstance(cached, dict):
        return cached, None
    if isinstance(cached, bytes):
        cached = cached.decode("utf-8", errors="replace")
    if not isinstance(cached, str):
        return None, f"Unsupported cached payload type: {type(cached).__name__}"
    if not cached.strip():
        return None, "Cached payload is empty"
    try:
        data = json.loads(cached)
    except Exception as exc:
        return None, f"Invalid cached payload JSON: {exc}"
    if not isinstance(data, dict):
        return None, "Cached payload JSON is not an object"
    return data, None


def _normalize_result(data: dict, request_id: str) -> dict:
    """
    Returns a simplified result for the query API.
    """
    mode = "voice" if request_id.startswith("VREQ_") else "text"
    status = data.get("status")
    if status == "success":
        status = "success"
    elif status == "failed":
        status = "failed"
    else:
        status = "processing"

    answer = data.get("answer") or data.get("answer_text")
    query = data.get("query") or data.get("transcribed_text")

    return {
        "request_id": request_id,
        "mode": mode,
        "status": status,
        "answer": answer,
        "query": query,
        "error": data.get("error"),
    }


def _wait_for_result(request_id: str, is_voice: bool, wait_seconds: int | None = None, poll_interval_ms: int | None = None) -> dict | None:
    """
    Polls the cache for a result until status changes from 'processing' or timeout.
    Returns the normalized result dict if found, or None if timeout.
    """
    wait_seconds = _resolve_wait_seconds(wait_seconds, is_voice=is_voice)
    poll_interval_ms = _resolve_poll_interval_ms(poll_interval_ms, is_voice=is_voice)

    if wait_seconds == 0:
        return None

    deadline = time.monotonic() + wait_seconds

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None

        time.sleep(min(poll_interval_ms / 1000.0, remaining))

        cached = frappe.cache().get(request_id)
        data, error = _safe_load_cache_payload(cached)
        if error:
            return None

        out = _normalize_result(data, request_id)
        if out.get("status") != "processing":
            return out


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


def _get_or_create_request(q: str, user_id: str, window_sec: int = DEDUP_WINDOW_SEC) -> dict:
    """
     OPTIMIZATION: Request deduplication 
    Return existing request if identical query in progress, else create new.
    """
    dedup_key = f"dedup_{user_id}:{hashlib.md5(q.encode()).hexdigest()}"
    cached_req = frappe.cache().get(dedup_key)
    
    if cached_req:
        try:
            existing = json.loads(cached_req)
            print(f"✓ Request dedup hit: reusing {existing['request_id']}")
            return {"request_id": existing["request_id"], "deduplicated": True}
        except Exception:
            pass
    
    # Create new request
    request_id = f"REQ_{uuid.uuid4().hex[:8]}"
    frappe.cache().set(dedup_key, json.dumps({"request_id": request_id}), ex=window_sec)
    return {"request_id": request_id, "deduplicated": False}

@frappe.whitelist(methods=["POST"], allow_guest=True)
def query(
    wait_seconds: int | None = None,
    poll_interval_ms: int | None = None,
):
    """
    Unified Query API.
    Accepts either:
    - q (text input), or
    - audio_url (voice input)

    Optional long-polling:
    - wait_seconds: 0-55, or omit for auto (text: 8s, voice: 25s)
    - poll_interval_ms: 100-2000, or omit for auto (text: 300ms, voice: 500ms)

    Returns request_id immediately if wait_seconds=0, otherwise waits for result.
    Processing is handled by RabbitMQ workers.
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
    
    #  OPTIMIZATION: Request deduplication for text queries
    if not is_voice and q:
        dedup_result = _get_or_create_request(q, user_id)
        request_id = dedup_result["request_id"]
        if dedup_result.get("deduplicated"):
            # Return existing request ID immediately
            return {"request_id": request_id, "deduplicated": True}
    else:
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

    # Wait for result if requested
    result = _wait_for_result(request_id, is_voice, wait_seconds, poll_interval_ms)
    if result:
        return result

    return {"request_id": request_id}