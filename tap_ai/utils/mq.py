# tap_ai/utils/mq.py  
  
import frappe  
import pika  
import json  
import time  
from threading import Lock
import logging

logger = logging.getLogger(__name__)

# Connection pool configuration
POOL_SIZE = 10  # Number of persistent connections to maintain
MAX_RETRIES = 2

# Metrics storage
_metrics = {
    "publish_count": 0,
    "publish_latency_sum": 0.0,
    "publish_errors": 0,
    "queue_depths": {},
    "last_update": time.time(),
}
_metrics_lock = Lock()


class RabbitMQConnectionPool:
    """Thread-safe connection pool for RabbitMQ."""
    
    def __init__(self, pool_size=POOL_SIZE):
        self.pool_size = pool_size
        self.connections = []
        self.channels = []
        self.current_index = 0
        self.lock = Lock()
        self._init_pool()
    
    def _init_pool(self):
        """Initialize the connection pool."""
        rabbitmq_url = frappe.conf.get("rabbitmq_url") or "amqp://guest:guest@localhost:5672/"
        
        for i in range(self.pool_size):
            try:
                parameters = pika.URLParameters(rabbitmq_url)
                parameters.heartbeat = 600
                parameters.blocked_connection_timeout = 300
                
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                
                self.connections.append(connection)
                self.channels.append(channel)
                logger.info(f"[Pool] Connection {i+1}/{self.pool_size} established")
            except Exception as e:
                logger.error(f"[Pool] Failed to create connection {i+1}: {str(e)}")
    
    def get_channel(self):
        """Get the next available channel (round-robin)."""
        with self.lock:
            if not self.channels:
                raise Exception("No connections in pool")
            
            channel = self.channels[self.current_index]
            self.current_index = (self.current_index + 1) % self.pool_size
            
            # Health check: if channel is closed, try to recover
            if channel.is_closed:
                try:
                    idx = self.channels.index(channel)
                    self.connections[idx].close()
                    
                    rabbitmq_url = frappe.conf.get("rabbitmq_url") or "amqp://guest:guest@localhost:5672/"
                    parameters = pika.URLParameters(rabbitmq_url)
                    parameters.heartbeat = 600
                    parameters.blocked_connection_timeout = 300
                    
                    new_conn = pika.BlockingConnection(parameters)
                    new_channel = new_conn.channel()
                    
                    self.connections[idx] = new_conn
                    self.channels[idx] = new_channel
                    logger.info(f"[Pool] Recovered connection {idx+1}")
                    
                    channel = new_channel
                except Exception as e:
                    logger.error(f"[Pool] Failed to recover connection: {str(e)}")
            
            return channel
    
    def close_all(self):
        """Close all connections (call on shutdown)."""
        with self.lock:
            for conn in self.connections:
                try:
                    if not conn.is_closed:
                        conn.close()
                except Exception:
                    pass
            self.connections.clear()
            self.channels.clear()
            logger.info("[Pool] All connections closed")


# Global connection pool (singleton)
_pool = None
_pool_lock = Lock()


def _get_pool():
    """Get or create the global connection pool."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = RabbitMQConnectionPool(POOL_SIZE)
    return _pool


def _record_metric(latency_ms, success=True):
    """Record publish metrics."""
    with _metrics_lock:
        _metrics["publish_count"] += 1
        if success:
            _metrics["publish_latency_sum"] += latency_ms
        else:
            _metrics["publish_errors"] += 1
        _metrics["last_update"] = time.time()
  
  
def publish_to_queue(queue_name: str, payload: dict, retry=True):  
    """  
    Publishes a message to RabbitMQ using the connection pool.
    Uses round-robin distribution across pooled connections.
    Includes self-healing retry on connection loss.
    """  
    start_time = time.time()
    
    try:  
        pool = _get_pool()
        channel = pool.get_channel()
          
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
          
    except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError) as e: 
        latency_ms = (time.time() - start_time) * 1000
        _record_metric(latency_ms, success=False)
        
        if retry:
            logger.warning(f"[Publish] Connection lost, retrying... ({str(e)})")
            # Recreate pool on connection failure
            global _pool
            with _pool_lock:
                _pool = None
            return publish_to_queue(queue_name, payload, retry=False)

        frappe.log_error(f"RabbitMQ Connection Failed: {str(e)}", "RabbitMQ Error")  
        frappe.local.response["http_status_code"] = 503  
        frappe.throw("The AI service is currently unavailable. Please ensure the background queue is running.")  
          
    except Exception as e:  
        latency_ms = (time.time() - start_time) * 1000
        _record_metric(latency_ms, success=False)
        frappe.log_error(f"Failed to publish to {queue_name}: {str(e)}", "RabbitMQ Error")  
        frappe.local.response["http_status_code"] = 500  
        frappe.throw("An internal error occurred while queuing your request.")

  
def close_connection():  
    """Close all pooled RabbitMQ connections (call on app shutdown)."""  
    pool = _get_pool()
    pool.close_all()


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
            "pool_size": POOL_SIZE,
            "last_update": metrics["last_update"],
        }


def cleanup_idle_connections():  
    """Cleanup is now handled by the pool (persistent connections are intentional)."""  
    pass  # No-op for backward compatibility
