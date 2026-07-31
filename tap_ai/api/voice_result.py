# tap_ai/api/voice_result.py
"""
Voice-specific result polling endpoint.

Extends the result API with an additional `transcribe` phase that lets the
caller detect when speech-to-text is complete before waiting for the LLM
answer. Phases in order:

    phase=transcribe → waits until Whisper transcription is done
    phase=router     → delegates to result.result(phase="router")
    phase=search     → delegates to result.result(phase="search")
    phase=answer     → delegates to result.result(phase="answer")

Each call blocks internally (fixed wait window) and always returns a
conclusive status — success or failed. There is no "processing" status and
no wait_seconds/poll_interval_ms client controls, matching result.result.

Endpoint:
    GET /api/method/tap_ai.api.voice_result.voice_result
    Params: request_id, phase (transcribe|router|search|answer)
"""

import time
import frappe

from tap_ai.api.result import result, get_snapshot, POLL_INTERVAL_MS, TEXT_WAIT_SECONDS


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

TRANSCRIBE_WAIT_SECONDS = TEXT_WAIT_SECONDS


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


@frappe.whitelist(methods=["GET"], allow_guest=True)
def voice_result(request_id: str, phase: str = "answer"):
    """
    Audio-focused result endpoint.
    - phase=transcribe: wait until transcription is completed
    - phase=router: wait until routing is completed after transcription
    - phase=search: wait until vector search is completed
    - phase=answer: reuse unified result polling until final answer
    """
    try:
        phase = (phase or "answer").strip().lower()

        if phase in ("answer", "router", "search"):
            return result(request_id=request_id, phase=phase)

        # phase == "transcribe": wait only for transcription completion
        deadline = time.monotonic() + TRANSCRIBE_WAIT_SECONDS

        while True:
            out = get_snapshot(request_id)
            raw_status = (out.get("raw_status") or "").strip().lower()
            canonical_status = (out.get("status") or "").strip().lower()

            if raw_status in TRANSCRIBE_DONE_STATES or canonical_status in ("success", "failed"):
                out["phase"] = "transcribe"
                out["phase_complete"] = True
                out["next_phase"] = "answer"
                return out

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                out["phase"] = "transcribe"
                out["phase_complete"] = True
                out["status"] = "failed"
                out["error"] = out.get("error") or "Timed out waiting for transcribe to complete"
                out["next_phase"] = "answer"
                return out

            time.sleep(min(POLL_INTERVAL_MS / 1000.0, remaining))
    finally:
        _close_http_connection()
