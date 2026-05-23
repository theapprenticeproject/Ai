# tap_ai/api/voice_result.py
"""
Voice-specific result polling endpoint.

Extends the three-phase result API with an additional `transcribe` phase
that lets the caller detect when speech-to-text is complete before waiting
for the LLM answer. Phases in order:

    phase=transcribe → waits until Whisper transcription is done (~12 s)
    phase=router     → delegates to result.result(phase="router")
    phase=search     → delegates to result.result(phase="search")
    phase=answer     → delegates to result.result(phase="answer")

This four-step sequence maps to Glific's chained webhook flow:
    audio_url → [transcribe poll] → [router poll] → [search poll] → [answer poll]

Endpoint:
    GET /api/method/tap_ai.api.voice_result.voice_result
    Params: request_id, phase (transcribe|router|search|answer), wait_seconds, poll_interval_ms
"""

import time
import frappe

from tap_ai.api.result import result


TRANSCRIBE_DONE_STATES = {
    "transcribed",
    "vector_search_success",
    "vector_search_failed",
    "generating_answer",
    "text_generated",
    "generating_audio",
    "success",
    "failed",
}

MAX_WAIT_SECONDS = 55
MIN_POLL_INTERVAL_MS = 100
MAX_POLL_INTERVAL_MS = 2000

AUTO_TRANSCRIBE_WAIT_SECONDS = 12
AUTO_TRANSCRIBE_POLL_INTERVAL_MS = 300


def _close_http_connection() -> None:
    try:
        response_headers = getattr(frappe.local, "response_headers", None)
        if response_headers is None:
            frappe.local.response_headers = []
            response_headers = frappe.local.response_headers

        header = ("Connection", "close")
        if header not in response_headers:
            response_headers.append(header)
    except Exception:
        pass


def _to_int(value, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(min(parsed, max_value), min_value)


@frappe.whitelist(methods=["GET"], allow_guest=True)
def voice_result(
    request_id: str,
    phase: str = "answer",
    wait_seconds: int | None = None,
    poll_interval_ms: int | None = None,
):
    """
    Audio-focused result endpoint with 2-phase polling:
    - phase=router: wait until routing is completed after transcription
    - phase=transcribe: wait until transcription is completed
    - phase=answer: reuse unified result polling until final answer
    """
    try:
        phase = (phase or "answer").strip().lower()

        # Phase 2: final answer (reuses existing unified long-poll logic)
        if phase == "answer":
            return result(
                request_id=request_id,
                phase=phase,
                wait_seconds=wait_seconds,
                poll_interval_ms=poll_interval_ms,
            )

        if phase == "router":
            return result(
                request_id=request_id,
                phase=phase,
                wait_seconds=wait_seconds,
                poll_interval_ms=poll_interval_ms,
            )

        if phase == "search":
            return result(
                request_id=request_id,
                phase=phase,
                wait_seconds=wait_seconds,
                poll_interval_ms=poll_interval_ms,
            )

        # Phase 1: wait only for transcription completion
        resolved_wait_seconds = _to_int(
            AUTO_TRANSCRIBE_WAIT_SECONDS if wait_seconds in (None, "") else wait_seconds,
            default=AUTO_TRANSCRIBE_WAIT_SECONDS,
            min_value=0,
            max_value=MAX_WAIT_SECONDS,
        )
        resolved_poll_interval_ms = _to_int(
            AUTO_TRANSCRIBE_POLL_INTERVAL_MS if poll_interval_ms in (None, "") else poll_interval_ms,
            default=AUTO_TRANSCRIBE_POLL_INTERVAL_MS,
            min_value=MIN_POLL_INTERVAL_MS,
            max_value=MAX_POLL_INTERVAL_MS,
        )

        deadline = time.monotonic() + resolved_wait_seconds

        while True:
            out = result(request_id=request_id, phase="answer", wait_seconds=0, poll_interval_ms=resolved_poll_interval_ms)
            raw_status = (out.get("raw_status") or "").strip().lower()
            canonical_status = (out.get("status") or "").strip().lower()

            if raw_status in TRANSCRIBE_DONE_STATES or canonical_status == "failed":
                out["phase"] = "transcribe"
                out["phase_complete"] = True
                out["next_phase"] = "answer"
                return out

            if resolved_wait_seconds == 0:
                out["phase"] = "transcribe"
                out["phase_complete"] = False
                out["next_phase"] = "answer"
                return out

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                out["phase"] = "transcribe"
                out["phase_complete"] = False
                out["next_phase"] = "answer"
                return out

            time.sleep(min(resolved_poll_interval_ms / 1000.0, remaining))
    finally:
        _close_http_connection()