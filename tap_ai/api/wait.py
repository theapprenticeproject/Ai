import frappe
import time


# Wait constants
MAX_DELAY_SECONDS = 300  # Hard limit for delay (5 minutes)


def _to_int(value, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(min(parsed, max_value), min_value)


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


