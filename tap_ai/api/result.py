# tap_ai/api/result.py

import frappe
import json
import time


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


def _is_voice_response(data: dict, request_id: str) -> bool:
    if data.get("mode") == "voice":
        return True
    if request_id.startswith("VREQ_"):
        return True
    return any(
        key in data
        for key in ("audio_url", "transcribed_text", "answer_text", "language")
    )


def _canonical_status(raw_status: str | None) -> str:
    if raw_status == "success":
        return "success"
    if raw_status == "failed":
        return "failed"
    return "processing"


def _as_public_url(url: str | None) -> str | None:
    if not url:
        return None
    if str(url).startswith("http://") or str(url).startswith("https://"):
        return url
    return frappe.utils.get_url(url)


def _empty_result(request_id: str, status: str = "failed", error: str | None = None) -> dict:
    mode = "voice" if str(request_id or "").startswith("VREQ_") else "text"
    return {
        "request_id": request_id,
        "mode": mode,
        "status": status,
        "raw_status": None,
        "answer": None,
        "answer_text": None,
        "query": None,
        "transcribed_text": None,
        "audio_url": None,
        "language": None,
        "error": error,
    }


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
    Returns a stable response contract for both text and voice.
    """
    mode = "voice" if _is_voice_response(data, request_id) else "text"
    raw_status = data.get("status")
    status = _canonical_status(raw_status)

    answer = data.get("answer") or data.get("answer_text")
    query = data.get("query") or data.get("transcribed_text")

    audio_url = _as_public_url(data.get("audio_url"))

    out = {
        "request_id": request_id,
        "mode": mode,
        "status": status,
        "raw_status": raw_status,
        "answer": answer,
        "answer_text": answer,
        "query": query,
        "transcribed_text": data.get("transcribed_text"),
        "audio_url": audio_url,
        "language": data.get("language"),
        "error": data.get("error"),
    }

    # Preserve useful existing fields when available.
    if "history" in data:
        out["history"] = data.get("history")
    if "metadata" in data:
        out["metadata"] = data.get("metadata")
    if "session_id" in data:
        out["session_id"] = data.get("session_id")
    if "user_id" in data:
        out["user_id"] = data.get("user_id")

    return out

@frappe.whitelist(methods=["GET"], allow_guest=True)
def result(request_id: str, wait_seconds: int | None = None, poll_interval_ms: int | None = None):
    """
    Result API: Fetch answer by request_id.
    Optional long-polling:
    - wait_seconds: 0-55, or omit for auto (text: 8s, voice: 25s)
    - poll_interval_ms: 100-2000, or omit for auto (text: 300ms, voice: 500ms)
    """
    request_id = (request_id or "").strip()
    if not request_id:
        return _empty_result(request_id, error="Missing request_id")

    cached = frappe.cache().get(request_id)
    data, error = _safe_load_cache_payload(cached)
    if error:
        return _empty_result(request_id, error=f"No such request_id or unavailable state: {request_id}")

    out = _normalize_result(data, request_id)

    if out.get("status") != "processing":
        return out

    is_voice = _is_voice_response(data, request_id)
    wait_seconds = _resolve_wait_seconds(wait_seconds, is_voice=is_voice)
    poll_interval_ms = _resolve_poll_interval_ms(poll_interval_ms, is_voice=is_voice)

    if wait_seconds == 0:
        return out

    deadline = time.monotonic() + wait_seconds

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return out

        time.sleep(min(poll_interval_ms / 1000.0, remaining))

        cached = frappe.cache().get(request_id)
        data, error = _safe_load_cache_payload(cached)
        if error:
            return _empty_result(request_id, error=f"No such request_id or unavailable state: {request_id}")

        out = _normalize_result(data, request_id)
        if out.get("status") != "processing":
            return out
