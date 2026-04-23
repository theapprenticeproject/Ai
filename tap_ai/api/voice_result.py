# tap_ai/api/voice_result.py

import time
import frappe

from tap_ai.api.result import result


TRANSCRIBE_DONE_STATES = {
    "transcribed",
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
    - phase=transcribe: wait until transcription is completed
    - phase=answer: reuse unified result polling until final answer
    """
    phase = (phase or "answer").strip().lower()

    # Phase 2: final answer (reuses existing unified long-poll logic)
    if phase != "transcribe":
        return result(
            request_id=request_id,
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
        out = result(request_id=request_id, wait_seconds=0, poll_interval_ms=resolved_poll_interval_ms)
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