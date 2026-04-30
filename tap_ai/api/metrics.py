import frappe
from tap_ai.utils.mq import get_queue_metrics


@frappe.whitelist(methods=["GET"], allow_guest=True)
def queue_metrics():
    """
    Endpoint to monitor RabbitMQ queue health and performance.
    
    Returns metrics on:
    - Total messages published
    - Average publish latency
    - Error rate
    - Pool status
    
    Example:
    GET /api/method/tap_ai.api.metrics.queue_metrics
    
    Returns:
    {
        "total_publishes": 1500,
        "avg_latency_ms": 2.45,
        "publish_errors": 3,
        "error_rate": 0.2,
        "pool_size": 10,
        "last_update": 1234567890.123
    }
    """
    metrics = get_queue_metrics()
    return {
        "status": "ok" if metrics["error_rate"] < 5 else "degraded",
        "metrics": metrics,
    }
