"""
Glific workflow delay endpoint.

Provides a simple HTTP-triggered sleep so Glific automation flows can pause
between steps without building timer logic into the flow itself. The delay
is synchronous and capped at 5 minutes to prevent runaway holds.

Endpoint:
    GET  /api/method/tap_ai.api.wait.delay?delay_seconds=10
    POST /api/method/tap_ai.api.wait.delay  (delay_seconds in body)

Typical Glific usage: call this between the query step and the result step
to give workers time to process before the client polls for an answer.
"""

import frappe
import time
from loguru import logger


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

    # Logging for webhook diagnostics (caller, params, timestamp)
    try:
        caller = getattr(frappe.local, "request_ip", None) or (frappe.local.request.environ.get('REMOTE_ADDR') if getattr(frappe.local, 'request', None) else None)
    except Exception:
        caller = None
    logger.debug(f"Webhook delay called: caller={caller} delay_seconds={delay_seconds}")

    waited = 0
    try:
        if delay_seconds > 0:
            time.sleep(delay_seconds)
            waited = delay_seconds
    except Exception as e:
        logger.warning(f"Webhook delay interrupted for caller={caller}: {e}")

    resp = {"success": True, "waited_seconds": waited}
    logger.debug(f"Webhook delay returning: caller={caller} waited={waited}s")
    return resp


