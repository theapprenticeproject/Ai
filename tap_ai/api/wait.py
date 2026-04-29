import frappe
import json
import time


# Wait constants
MAX_TIMEOUT_SECONDS = 120  # Hard limit (Glific default is ~10s)
DEFAULT_TIMEOUT_SECONDS = 30  # Smart default for most queries
POLL_INTERVAL_MS = 500  # Check every 500ms

MAX_DELAY_SECONDS = 300  # Hard limit for delay (5 minutes)


def _to_int(value, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(min(parsed, max_value), min_value)


def _safe_load_cache_payload(cached) -> tuple[dict | None, str | None]:
    if cached is None:
        return None, "Request not found"
    if isinstance(cached, dict):
        return cached, None
    if isinstance(cached, bytes):
        cached = cached.decode("utf-8", errors="replace")
    if not isinstance(cached, str):
        return None, f"Invalid cached payload type: {type(cached).__name__}"
    if not cached.strip():
        return None, "Cached payload is empty"
    try:
        data = json.loads(cached)
    except Exception as exc:
        return None, f"Invalid cached payload JSON: {exc}"
    if not isinstance(data, dict):
        return None, "Cached payload is not an object"
    return data, None


def _normalize_wait_result(data: dict, request_id: str) -> dict:
    """
    Returns the full result object for the wait endpoint.
    """
    mode = "voice" if request_id.startswith("VREQ_") else "text"
    status = data.get("status")
    
    # Map to canonical status
    if status == "success":
        status = "success"
    elif status == "failed":
        status = "failed"
    else:
        status = "processing"

    answer = data.get("answer") or data.get("answer_text")
    query = data.get("query") or data.get("transcribed_text")
    
    result = {
        "request_id": request_id,
        "mode": mode,
        "status": status,
        "raw_status": data.get("status"),
        "answer": answer,
        "answer_text": answer,
        "query": query,
        "transcribed_text": data.get("transcribed_text"),
        "audio_url": data.get("audio_url"),
        "language": data.get("language"),
        "error": data.get("error"),
    }
    
    # Preserve optional fields
    if "tool" in data:
        result["tool"] = data.get("tool")
    if "metadata" in data:
        result["metadata"] = data.get("metadata")
    if "session_id" in data:
        result["session_id"] = data.get("session_id")
    if "user_id" in data:
        result["user_id"] = data.get("user_id")
    if "vector_search" in data:
        result["vector_search"] = data.get("vector_search")
    if "history" in data:
        result["history"] = data.get("history")
    
    return result


@frappe.whitelist(methods=["GET"], allow_guest=True)
def wait_for_result(
    request_id: str,
    timeout_seconds: int | None = None,
):
    """
    Wait for a query request to complete (polling endpoint).
    
    Instead of using Glific's wait node, call this endpoint from your workflow.
    It will block (via polling) until the request completes or timeout occurs.
    
    Parameters:
    - request_id: The ID returned by query() API
    - timeout_seconds: Max wait time (0-120s). Defaults to 30s.
                      Set to 0 for immediate return.
    
    Returns:
    - Full result object with status, answer, tool, metadata, etc.
    - On timeout: Returns current state (may still be processing)
    
    Use cases:
    - Replace Glific wait node: call this instead
    - Custom workflows: control wait times from backend
    - Mobile apps: simple blocking wait endpoint
    
    Example:
    GET /api/method/tap_ai.api.wait.wait_for_result?request_id=REQ_abc123&timeout_seconds=45
    """
    request_id = (request_id or "").strip()
    if not request_id:
        return {
            "error": "Missing request_id",
            "status": "failed",
        }
    
    # Resolve timeout
    timeout_seconds = _to_int(timeout_seconds, default=DEFAULT_TIMEOUT_SECONDS, min_value=0, max_value=MAX_TIMEOUT_SECONDS)
    
    # Immediate return if no wait requested
    if timeout_seconds == 0:
        cached = frappe.cache().get(request_id)
        data, error = _safe_load_cache_payload(cached)
        if error:
            return {
                "request_id": request_id,
                "error": error,
                "status": "failed",
            }
        return _normalize_wait_result(data, request_id)
    
    # Poll until completion or timeout
    deadline = time.monotonic() + timeout_seconds
    
    while True:
        remaining = deadline - time.monotonic()
        
        # Check current state
        cached = frappe.cache().get(request_id)
        data, error = _safe_load_cache_payload(cached)
        if error:
            return {
                "request_id": request_id,
                "error": error,
                "status": "failed",
            }
        
        result = _normalize_wait_result(data, request_id)
        
        # Return if request completed
        if result.get("status") != "processing":
            return result
        
        # Check timeout
        if remaining <= 0:
            # Return current state (still processing but timeout reached)
            return result
        
        # Sleep before next poll
        sleep_time = min(POLL_INTERVAL_MS / 1000.0, remaining)
        time.sleep(sleep_time)


@frappe.whitelist(methods=["GET", "POST"], allow_guest=True)
def delay(delay_seconds: int | None = None):
    """
    Simple delay/wait endpoint for Glific workflows.
    
    Use this between two workflow steps to add a pause.
    For example, wait 10 seconds before asking the next question.
    
    Parameters:
    - delay_seconds: How long to wait (0-300s). Defaults to 10s.
    
    Returns:
    - {"success": true, "waited_seconds": N}
    
    Example Glific usage:
    GET /api/method/tap_ai.api.wait.delay?delay_seconds=10
    
    Use case:
    1. User asks question
    2. Get response
    3. Call this delay endpoint (pause 10 seconds)
    4. Ask next question
    """
    delay_seconds = _to_int(delay_seconds, default=10, min_value=0, max_value=MAX_DELAY_SECONDS)
    
    if delay_seconds > 0:
        time.sleep(delay_seconds)
    
    return {
        "success": True,
        "waited_seconds": delay_seconds,
    }

