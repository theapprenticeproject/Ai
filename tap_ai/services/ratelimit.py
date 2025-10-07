# File: tap_ai/services/ratelimit.py
import time
from typing import Optional
import frappe

def _cache():
    # Frappe’s Redis cache
    return frappe.cache()

def _key(api_key: str, scope: str) -> str:
    return f"tap_ai:ratelimit:{scope}:{api_key}"

def check_rate_limit(
    api_key: Optional[str],
    scope: str,
    limit: int = 60,          # requests
    window_sec: int = 60,     # per 60s
) -> tuple[bool, int, int]:
    """
    Returns (allowed, remaining, reset_epoch).
    If api_key is None (e.g., session auth), we rate-limit by session/user id instead.
    """
    if not api_key:
        # Try to use session or user as identity
        user = frappe.session.user if frappe.session else "guest"
        api_key = f"user:{user}"

    cache = _cache()
    now = int(time.time())
    bucket = now // window_sec
    k = f"{_key(api_key, scope)}:{bucket}"

    # Increment
    new_count = cache.incr(k)
    if new_count == 1:
        cache.expire(k, window_sec + 2)  # small pad

    remaining = max(0, limit - new_count)
    reset = (bucket + 1) * window_sec

    return (new_count <= limit, remaining, reset)
