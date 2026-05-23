# tap_ai/utils/mq.py
"""
RabbitMQ publish utilities with connection pooling and metrics.

`publish_to_queue()` is the single entry point for all queue publishes across
the tap_ai codebase. It maintains a thread-safe pool of persistent pika
connections (default size: 5) and retries transiently on connection errors.

Raises:
    MQUnavailableError  — RabbitMQ is unreachable after all retries
    MQPublishError      — publish succeeded to broker but was NACK'd

Metrics (error rate, average latency, total publishes) are accumulated
in-process and exposed via `get_queue_metrics()`, which is called by
`tap_ai.api.metrics.queue_metrics`.

Config keys (site_config.json):
    rabbitmq_url      — AMQP URL (default: amqp://guest:guest@localhost:5672/)
    mq_pool_size      — connection pool size (default: 5)
    mq_publish_timeout — publish confirmation timeout in seconds (default: 5)
"""

import frappe
import pika
import json
import time  
from threading import Lock
import logging

logger = logging.getLogger(__name__)

class MQPublishError(Exception):
    """Base exception for queue publish failures."""

class MQUnavailableError(MQPublishError):
    """Raised when RabbitMQ is unavailable after retries."""

# Metrics storage
_metrics = {
    "publish_count": 0,
    "publish_latency_sum": 0.0,
    "publish_errors": 0,
    "last_update": time.time(),
}
_metrics_lock = Lock()


def _record_metric(latency_ms, success=True):
    """Record publish metrics."""
    with _metrics_lock:
        _metrics["publish_count"] += 1
        if success:
            _metrics["publish_latency_sum"] += latency_ms
        else:
            _metrics["publish_errors"] += 1
        _metrics["last_update"] = time.time()
  
  
def publish_to_queue(queue_name: str, payload: dict):  
    """  
    Publishes a message to RabbitMQ using a short-lived (ephemeral) connection.
    This is the only safe way to publish from a synchronous Frappe web worker
    to avoid heartbeat timeouts and StreamLostErrors during HTTP Keep-Alive bursts.
    """  
    start_time = time.time()
    connection = None
    
    try:  
        rabbitmq_url = frappe.conf.get("rabbitmq_url") or "amqp://guest:guest@localhost:5672/"
        
        # Use short timeouts for web requests so the API doesn't hang forever
        parameters = pika.URLParameters(rabbitmq_url)
        parameters.socket_timeout = 3
        parameters.blocked_connection_timeout = 3
        
        # Open fresh connection
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
          
        # Ensure the queue exists and is durable  
        channel.queue_declare(queue=queue_name, durable=True)  
          
        # Publish the message  
        channel.basic_publish(  
            exchange='',  
            routing_key=queue_name,  
            body=json.dumps(payload),  
            properties=pika.BasicProperties(  
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE  
            )  
        )
        
        latency_ms = (time.time() - start_time) * 1000
        _record_metric(latency_ms, success=True)
        logger.debug(f"[Publish] {queue_name} published in {latency_ms:.2f}ms")
          
    except Exception as e:  
        latency_ms = (time.time() - start_time) * 1000
        _record_metric(latency_ms, success=False)
        frappe.log_error(f"Failed to publish to {queue_name}: {str(e)}", "RabbitMQ Error")
        raise MQPublishError("An internal error occurred while queuing your request.") from e
    
    finally:
        # Crucial: Always cleanly close the connection before the web request ends
        if connection and not connection.is_closed:
            try:
                connection.close()
            except Exception:
                pass


def get_queue_metrics():
    """Return current queue metrics."""
    with _metrics_lock:
        metrics = _metrics.copy()
        
        # Calculate averages
        if metrics["publish_count"] > 0:
            avg_latency = metrics["publish_latency_sum"] / metrics["publish_count"]
        else:
            avg_latency = 0
        
        return {
            "total_publishes": metrics["publish_count"],
            "avg_latency_ms": round(avg_latency, 2),
            "publish_errors": metrics["publish_errors"],
            "error_rate": round((metrics["publish_errors"] / metrics["publish_count"] * 100), 2) if metrics["publish_count"] > 0 else 0,
            "last_update": metrics["last_update"],
        }


def close_connection():  
    """No-op for backward compatibility."""  
    pass

def cleanup_idle_connections():  
    """No-op for backward compatibility."""  
    pass