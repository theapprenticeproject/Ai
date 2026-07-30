# tap_ai/api/result.py
"""
Three-phase result polling endpoint.

Glific workflows have a hard 10-second HTTP timeout, so a single long-poll
is not viable. Instead, the client polls in three sequential phases, each
with a ~9-second wait and 200 ms poll interval:

    phase=router  → returns once routing decision is written to Redis
    phase=search  → returns once pgvector vector search completes
    phase=answer  → returns once the final answer is synthesized

Each phase response includes `phase_complete`, `next_phase`, and the
full normalized result envelope so the caller can display intermediate
progress (e.g. "thinking…", "searching knowledge…").

Endpoint:
    GET /api/method/tap_ai.api.result.result
    Params: request_id, phase (router|search|answer), wait_seconds, poll_interval_ms
"""

import frappe
import json
import time
from loguru import logger


MAX_WAIT_SECONDS = 55
MIN_POLL_INTERVAL_MS = 100
MAX_POLL_INTERVAL_MS = 2000

AUTO_TEXT_WAIT_SECONDS = 8
AUTO_VOICE_WAIT_SECONDS = 9
AUTO_TEXT_POLL_INTERVAL_MS = 300
AUTO_VOICE_POLL_INTERVAL_MS = 500
VECTOR_SEARCH_DONE_STATES = {
    "vector_search_success",
    "vector_search_failed",
}

# Legacy status-based fallback for router phase detection.
# In practice, _normalize_router_result() detects completion via the
# router_decision dict being present in the state — this set is a secondary
# safety net and is not currently emitted by the worker.
ROUTER_DONE_STATES = {
    "router_complete",
}


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
        "timing_ms": None,
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
    metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    timings_ms = metadata.get("timings_ms") if isinstance(metadata.get("timings_ms"), dict) else {}
    timing_ms = data.get("timing_ms")
    if timing_ms is None:
        timing_ms = (
            timings_ms.get("stt")
            or data.get("stt_timing_ms")
            or timings_ms.get("direct_llm")
            or timings_ms.get("total")
        )

    fallback_reason = data.get("fallback_reason")
    if fallback_reason is None:
        fallback_reason = metadata.get("fallback_reason")

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
        "timing_ms": timing_ms,
        "fallback_reason": fallback_reason,
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
    if "vector_search" in data:
        out["vector_search"] = data.get("vector_search")
    if "tool" in data:
        out["tool"] = data.get("tool")
    if "fallback_reason" in metadata and out.get("fallback_reason") is None:
        out["fallback_reason"] = metadata.get("fallback_reason")

    return out


def _normalize_router_result(data: dict, request_id: str) -> dict | None:
    """
    Normalize router decision checkpoint.
    Returns None if router phase is not complete.
    """
    router_info = data.get("router_decision") if isinstance(data.get("router_decision"), dict) else None
    raw_status = data.get("status")

    # Router is complete if router_decision info is present or status indicates router is done
    if not router_info and raw_status not in ROUTER_DONE_STATES:
        return None

    if not router_info:
        router_info = {
            "status": "success",
            "raw_status": raw_status,
            "tool": data.get("tool"),
        }

    out = _normalize_result(data, request_id)
    out["phase"] = "router"
    out["phase_complete"] = True
    # Next phase depends on tool chosen
    tool = router_info.get("tool")
    if tool == "vector_search":
        out["next_phase"] = "search"
    else:
        out["next_phase"] = "answer"
    out["status"] = "processing"
    out["answer"] = None
    out["answer_text"] = None
    out["router_decision"] = router_info
    
    # Override timing_ms with router-specific timing from metadata.
    # Prefer the explicit router timing, but fall back to alternate fields when needed.
    metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    timings_ms = metadata.get("timings_ms") if isinstance(metadata.get("timings_ms"), dict) else {}
    router_timing = timings_ms.get("router")
    if router_timing is None:
        router_timing = metadata.get("router_timing_ms")
    if router_timing is None:
        router_timing = data.get("router_timing_ms")
    if router_timing is not None:
        out["timing_ms"] = router_timing
    
    return out


def _normalize_vector_search_result(data: dict, request_id: str) -> dict | None:
    vector_search = data.get("vector_search") if isinstance(data.get("vector_search"), dict) else None
    raw_status = data.get("status")

    if not vector_search and raw_status not in VECTOR_SEARCH_DONE_STATES:
        return None

    if not vector_search:
        vector_search = {
            "status": "success" if raw_status == "vector_search_success" else "failed",
            "raw_status": raw_status,
            "results_count": data.get("results_count"),
            "routed_doctypes": data.get("routed_doctypes") or [],
            "metadata": data.get("metadata") or {},
        }

    out = _normalize_result(data, request_id)
    out["phase"] = "search"
    out["phase_complete"] = True
    out["next_phase"] = "answer" if vector_search.get("status") == "success" else None
    out["status"] = vector_search.get("status") or out.get("status")
    out["raw_status"] = vector_search.get("raw_status") or raw_status
    out["answer"] = None
    out["answer_text"] = None
    out["vector_search"] = vector_search
    return out

@frappe.whitelist(methods=["GET"], allow_guest=True)
def result(
    request_id: str,
    phase: str = "answer",
    wait_seconds: int | None = None,
    poll_interval_ms: int | None = None,
):
    """
    Result API: Fetch answer by request_id.
    Optional long-polling:
    - phase=router: return routing decision state (text: after router, voice: after transcription + router)
    - phase=search: return vector-search completion state
    - phase=answer: return the final synthesized answer
    - wait_seconds: 0-55, or omit for auto (text: 8s, voice: 25s)
    - poll_interval_ms: 100-2000, or omit for auto (text: 300ms, voice: 500ms)
    """
    try:
        request_id = (request_id or "").strip()
        phase = (phase or "answer").strip().lower()
        if not request_id:
            return _empty_result(request_id, error="Missing request_id")

        # Read cache and log payload for diagnostics
        try:
            cached = frappe.cache().get(request_id)
        except Exception as e:
            logger.error(f"Cache read error for {request_id}: {e}")
            return _empty_result(request_id, error=f"Cache read error for {request_id}: {e}")

        logger.debug(f"cache_lookup: request_id={request_id} cached_type={type(cached).__name__}")
        data, error = _safe_load_cache_payload(cached)
        if error:
            logger.warning(f"Cache payload invalid or missing for {request_id}: {error}")
            return _empty_result(request_id, error=f"No such request_id or unavailable state: {request_id}")

        if phase == "router":
            is_voice = _is_voice_response(data, request_id)
            wait_seconds = _resolve_wait_seconds(wait_seconds, is_voice=is_voice)
            poll_interval_ms = _resolve_poll_interval_ms(poll_interval_ms, is_voice=is_voice)

            if wait_seconds == 0:
                router_out = _normalize_router_result(data, request_id)
                if router_out:
                    return router_out
                out = _normalize_result(data, request_id)
                out["phase"] = "router"
                out["phase_complete"] = False
                out["next_phase"] = "search" if data.get("tool") == "vector_search" else "answer"
                return out

            deadline = time.monotonic() + wait_seconds

            while True:
                router_out = _normalize_router_result(data, request_id)
                if router_out:
                    return router_out

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    out = _normalize_result(data, request_id)
                    out["phase"] = "router"
                    out["phase_complete"] = False
                    out["next_phase"] = "search" if data.get("tool") == "vector_search" else "answer"
                    return out

                time.sleep(min(poll_interval_ms / 1000.0, remaining))

                cached = frappe.cache().get(request_id)
                data, error = _safe_load_cache_payload(cached)
                if error:
                    return _empty_result(request_id, error=f"No such request_id or unavailable state: {request_id}")

        if phase == "search":
            is_voice = _is_voice_response(data, request_id)
            wait_seconds = _resolve_wait_seconds(wait_seconds, is_voice=is_voice)
            poll_interval_ms = _resolve_poll_interval_ms(poll_interval_ms, is_voice=is_voice)

            if wait_seconds == 0:
                search_out = _normalize_vector_search_result(data, request_id)
                if search_out:
                    return search_out
                out = _normalize_result(data, request_id)
                out["phase"] = "search"
                out["phase_complete"] = False
                out["next_phase"] = "answer"
                return out

            deadline = time.monotonic() + wait_seconds

            while True:
                search_out = _normalize_vector_search_result(data, request_id)
                if search_out:
                    return search_out

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    out = _normalize_result(data, request_id)
                    out["phase"] = "search"
                    out["phase_complete"] = False
                    out["next_phase"] = "answer"
                    return out

                time.sleep(min(poll_interval_ms / 1000.0, remaining))

                cached = frappe.cache().get(request_id)
                data, error = _safe_load_cache_payload(cached)
                if error:
                    return _empty_result(request_id, error=f"No such request_id or unavailable state: {request_id}")

        out = _normalize_result(data, request_id)

        if data.get("status") == "vector_search_failed":
            return _empty_result(request_id, status="failed", error=data.get("error") or "Vector search failed")

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
    finally:
        _close_http_connection()