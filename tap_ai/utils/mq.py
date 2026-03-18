# tap_ai/utils/mq.py

import frappe
import pika
import json

def publish_to_queue(queue_name: str, payload: dict):
    """
    Connects to RabbitMQ, ensures the queue exists, and publishes the payload.
    """
    # In production, set this in your site_config.json
    rabbitmq_url = frappe.conf.get("rabbitmq_url") or "amqp://guest:guest@localhost:5672/"
    
    try:
        parameters = pika.URLParameters(rabbitmq_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Ensure the queue exists and is durable (survives broker restarts)
        channel.queue_declare(queue=queue_name, durable=True)
        
        # Publish the message
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(payload),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE # Persist message to disk
            )
        )
        connection.close()
        
    except pika.exceptions.AMQPConnectionError as e:
        frappe.log_error(f"RabbitMQ Connection Refused: {str(e)}", "RabbitMQ Error")
        frappe.local.response["http_status_code"] = 503
        frappe.throw("The AI service is currently unavailable. Please ensure the background queue is running.")
        
    except Exception as e:
        frappe.log_error(f"Failed to publish to {queue_name}: {str(e)}", "RabbitMQ Error")
        frappe.local.response["http_status_code"] = 500
        frappe.throw("An internal error occurred while queuing your request.")