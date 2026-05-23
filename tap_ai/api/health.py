# tap_ai/api/health.py
"""
Health check endpoint.

GET /api/method/tap_ai.api.health.health

Returns the connectivity status of every external dependency so that
load balancers, uptime monitors, and on-call engineers have a single
place to verify the system is operational.
"""

import time
import frappe
from loguru import logger


def _check_redis() -> dict:
    start = time.monotonic()
    try:
        frappe.cache().set("tap_ai:health_ping", "1", ex=10)
        val = frappe.cache().get("tap_ai:health_ping")
        ok = val is not None
    except Exception as e:
        logger.warning(f"Health check: Redis failed: {e}")
        return {"status": "fail", "error": str(e)}
    return {"status": "ok" if ok else "fail", "latency_ms": int((time.monotonic() - start) * 1000)}


def _check_postgres() -> dict:
    start = time.monotonic()
    try:
        from tap_ai.utils.remote_db import execute_remote_query
        execute_remote_query("SELECT 1")
    except Exception as e:
        logger.warning(f"Health check: PostgreSQL failed: {e}")
        return {"status": "fail", "error": str(e)}
    return {"status": "ok", "latency_ms": int((time.monotonic() - start) * 1000)}


def _check_rabbitmq() -> dict:
    start = time.monotonic()
    try:
        import pika
        rabbitmq_url = frappe.conf.get("rabbitmq_url") or "amqp://guest:guest@localhost:5672/"
        params = pika.URLParameters(rabbitmq_url)
        params.socket_timeout = 3
        conn = pika.BlockingConnection(params)
        conn.close()
    except Exception as e:
        logger.warning(f"Health check: RabbitMQ failed: {e}")
        return {"status": "fail", "error": str(e)}
    return {"status": "ok", "latency_ms": int((time.monotonic() - start) * 1000)}


def _check_openai_configured() -> dict:
    from tap_ai.infra.config import get_config
    key = get_config("openai_api_key")
    if not key:
        return {"status": "fail", "error": "openai_api_key not configured"}
    return {"status": "ok"}


@frappe.whitelist(methods=["GET"], allow_guest=True)
def health():
    """
    Returns overall system health and per-dependency status.
    HTTP 200 when all checks pass; HTTP 503 when any dependency is down.
    """
    checks = {
        "redis": _check_redis(),
        "postgres": _check_postgres(),
        "rabbitmq": _check_rabbitmq(),
        "openai": _check_openai_configured(),
    }

    overall = "ok" if all(c["status"] == "ok" for c in checks.values()) else "degraded"

    if overall != "ok":
        frappe.local.response["http_status_code"] = 503

    return {
        "status": overall,
        "timestamp": int(time.time()),
        "checks": checks,
    }
