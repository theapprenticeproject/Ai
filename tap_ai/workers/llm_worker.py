# tap_ai/workers/llm_worker.py

import frappe
import json
import pika
from tap_ai.services.router import (
    process_query,
    _get_history_from_cache,
    _save_history_to_cache,
    _append_history_to_db,
)
from tap_ai.utils.mq import publish_to_queue

def process_message(ch, method, properties, body):
    """Callback triggered when a message is pulled from text_query_queue."""
    payload = json.loads(body)
    request_id = payload.get("request_id")
    query = payload.get("query")
    user_id = payload.get("user_id")
    
    # Flags passed by the STT worker for voice queries
    is_voice = payload.get("is_voice", False)
    language = payload.get("language", "en")
    session_id = payload.get("session_id") or user_id

    print(f"\n[*] [LLM Worker] Picked up task: {request_id} | Query: '{query}' | Session: {session_id}")

    try:
        # 1. Update status to provide real-time UI feedback
        current_state = frappe.cache().get(request_id)
        state_dict = json.loads(current_state) if current_state else {}
        state_dict["status"] = "generating_answer"
        state_dict["session_id"] = session_id
        frappe.cache().set(request_id, json.dumps(state_dict))

        # 2. Fetch history using your existing router helper
        chat_history = _get_history_from_cache(user_id, session_id=session_id)

        # 3. Run the Dual-Engine Router logic
        out = process_query(query=query, chat_history=chat_history)
        answer = out.get("answer", "")

        # 4. Update and save history
        chat_history.append({"role": "user", "content": query})
        chat_history.append({"role": "assistant", "content": answer})
        _save_history_to_cache(user_id, chat_history, session_id=session_id)
        _append_history_to_db(
            user_id,
            [{"role": "user", "content": query}, {"role": "assistant", "content": answer}],
            session_id=session_id,
            metadata={"source": "llm_worker"},
        )

        # 5. Routing Logic (Voice vs Text)
        if is_voice:
            # Update state so the frontend knows text is done, audio is next
            state_dict.update({
                "status": "text_generated",
                "answer_text": answer,
                "language": language,
                "transcribed_text": query,
                "session_id": session_id,
            })
            frappe.cache().set(request_id, json.dumps(state_dict))

            # Publish to TTS queue for the final voice step
            publish_to_queue("audio_tts_queue", {
                "request_id": request_id,
                "answer": answer,
                "user_id": user_id,
                "session_id": session_id,
                "language": language,
                "transcribed_text": query
            })
            print(f"[>] Voice detected: Routed {request_id} to audio_tts_queue")

        else:
            # Standard Text Query - Finish and save to Redis
            state_dict.update({
                "status": "success",
                "answer": answer,
                "query": query,
                "user_id": user_id,
                "session_id": session_id,
                "history": chat_history[-10:],
                "metadata": out.get("metadata", {})
            })
            frappe.cache().set(request_id, json.dumps(state_dict))
            print(f"[✓] Task {request_id} completed successfully.")

    except Exception as e:
        print(f"[x] Task {request_id} failed: {str(e)}")
        frappe.log_error(f"LLM Worker Error: {str(e)}", "RabbitMQ Worker")
        
        # Save failure state to Redis
        frappe.cache().set(request_id, json.dumps({
            "status": "failed",
            "error": str(e),
            "query": query,
            "user_id": user_id
        }))

    # 6. Acknowledge the message (Removes it from RabbitMQ)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def start():
    """Initializes RabbitMQ connection and starts consuming."""
    rabbitmq_url = frappe.conf.get("rabbitmq_url") or "amqp://guest:guest@localhost:5672/"
    
    try:
        parameters = pika.URLParameters(rabbitmq_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        channel.queue_declare(queue="text_query_queue", durable=True)
        channel.basic_qos(prefetch_count=1) # Process one message at a time
        channel.basic_consume(queue="text_query_queue", on_message_callback=process_message)

        print(" [*] LLM Worker running. Waiting for messages. (CTRL+C to exit)")
        channel.start_consuming()

    except Exception as e:
        print(f"[!] Worker crashed: {str(e)}")