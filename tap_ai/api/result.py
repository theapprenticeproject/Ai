# tap_ai/api/result.py

import frappe
import json
import time


VOICE_PROCESSING_STATES = {
    "pending",
    "processing",
    "transcribing",
    "transcribed",
    "generating_answer",
    "text_generated",
    "generating_audio",
}

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


def _format_result(data: dict, request_id: str) -> dict:
    # Keep voice clients aligned with old voice_result semantics while using one endpoint.
    if _is_voice_response(data, request_id) and data.get("status") in VOICE_PROCESSING_STATES:
        return {
            "status": "processing",
            "request_id": request_id,
            "transcribed_text": data.get("transcribed_text"),
            "answer_text": data.get("answer_text") or data.get("answer"),
            "language": data.get("language"),
            "audio_url": data.get("audio_url"),
        }
    return data

@frappe.whitelist(methods=["GET"], allow_guest=True)
def result(request_id: str, wait_seconds: int | None = None, poll_interval_ms: int | None = None):
    """
    Result API: Fetch answer by request_id.
    Optional long-polling:
    - wait_seconds: 0-55, or omit for auto (text: 8s, voice: 25s)
    - poll_interval_ms: 100-2000, or omit for auto (text: 300ms, voice: 500ms)
    """
    cached = frappe.cache().get(request_id)
    if not cached:
        frappe.throw(f"No such request_id: {request_id}")

    data = json.loads(cached)
    out = _format_result(data, request_id)

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
        if not cached:
            frappe.throw(f"No such request_id: {request_id}")

        data = json.loads(cached)
        out = _format_result(data, request_id)
        if out.get("status") != "processing":
            return out
