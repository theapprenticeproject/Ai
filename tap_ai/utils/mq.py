# tap_ai/utils/mq.py  
  
import frappe  
import pika  
import json  
from threading import local  
import time  
  
# Thread-local storage for connection pooling  
_thread_local = local()  
  
def get_rabbitmq_connection():  
    """Get or create a persistent RabbitMQ connection"""  
    # Check if we have a valid connection  
    if not hasattr(_thread_local, 'connection') or _thread_local.connection.is_closed:  
        rabbitmq_url = frappe.conf.get("rabbitmq_url") or "amqp://guest:guest@localhost:5672/"  
        parameters = pika.URLParameters(rabbitmq_url)  
        parameters.heartbeat = 600  # 10 minutes heartbeat  
        parameters.blocked_connection_timeout = 300  
          
        _thread_local.connection = pika.BlockingConnection(parameters)  
        _thread_local.channel = _thread_local.connection.channel()  
        _thread_local.last_used = time.time()  
      
    # Refresh channel if needed  
    elif hasattr(_thread_local, 'channel') and _thread_local.channel.is_closed:  
        _thread_local.channel = _thread_local.connection.channel()  
      
    _thread_local.last_used = time.time()  
    return _thread_local.connection, _thread_local.channel  
  
def publish_to_queue(queue_name: str, payload: dict, retry=True):  
    """  
    Connects to RabbitMQ, ensures the queue exists, and publishes the payload.  
    Includes a self-healing retry mechanism for dropped cloud connections.
    """  
    try:  
        connection, channel = get_rabbitmq_connection()  
          
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
          
    except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError) as e: 
        if retry:
            # The connection was likely dropped by a firewall or idle timeout.
            # Destroy the zombie connection from the thread local storage.
            if hasattr(_thread_local, 'connection'):
                try:
                    _thread_local.connection.close()
                except Exception:
                    pass
                delattr(_thread_local, 'connection')
            
            # Immediately try one more time with a fresh connection
            return publish_to_queue(queue_name, payload, retry=False)

        # If it fails twice, the queue is actually down
        frappe.log_error(f"RabbitMQ Connection Refused: {str(e)}", "RabbitMQ Error")  
        frappe.local.response["http_status_code"] = 503  
        frappe.throw("The AI service is currently unavailable. Please ensure the background queue is running.")  
          
    except Exception as e:  
        frappe.log_error(f"Failed to publish to {queue_name}: {str(e)}", "RabbitMQ Error")  
        frappe.local.response["http_status_code"] = 500  
        frappe.throw("An internal error occurred while queuing your request.")
  
def close_connection():  
    """Close the RabbitMQ connection (call on app shutdown)"""  
    if hasattr(_thread_local, 'connection') and not _thread_local.connection.is_closed:  
        _thread_local.connection.close()  
  
def cleanup_idle_connections():  
    """Clean up idle connections (call periodically)"""  
    if hasattr(_thread_local, 'last_used'):  
        if time.time() - _thread_local.last_used > 300:  # 5 minutes  
            close_connection()