# tap_ai/workers/tts_worker.py

import frappe
import json
import pika
import os
import uuid
from openai import OpenAI

def get_openai_client():
    api_key = frappe.conf.get("openai_api_key")
    if not api_key:
        frappe.throw("OpenAI API key not found")
    return OpenAI(api_key=api_key)

def process_message(ch, method, properties, body):
    payload = json.loads(body)
    request_id = payload.get("request_id")
    answer = payload.get("answer")
    language = payload.get("language", "en")
    transcribed_text = payload.get("transcribed_text", "")

    print(f"\n[*] [TTS Worker] Generating audio for {request_id}")

    try:
        # Update state
        current_state = frappe.cache().get(request_id)
        state_dict = json.loads(current_state) if current_state else {}
        state_dict["status"] = "generating_audio"
        frappe.cache().set(request_id, json.dumps(state_dict))

        client = get_openai_client()
        output_path = f"/tmp/{uuid.uuid4().hex}.mp3"

        # Generate Speech
        with client.audio.speech.with_streaming_response.create(
            model="tts-1",
            voice="alloy",
            input=answer
        ) as r:
            r.stream_to_file(output_path)

        # Save to Frappe File Manager
        with open(output_path, "rb") as f:
            file_doc = frappe.get_doc({
                "doctype": "File",
                "file_name": os.path.basename(output_path),
                "is_private": 0,
                "content": f.read()
            })
            file_doc.insert(ignore_permissions=True)

        # Cleanup temp file
        if os.path.exists(output_path):
            os.remove(output_path)

        # Final Success State
        state_dict.update({
            "status": "success",
            "audio_url": file_doc.file_url,
            "answer_text": answer,
            "transcribed_text": transcribed_text,
            "language": language
        })
        frappe.cache().set(request_id, json.dumps(state_dict))
        
        print(f"[✓] {request_id} audio generated: {file_doc.file_url}")

    except Exception as e:
        print(f"[x] TTS failed for {request_id}: {str(e)}")
        frappe.cache().set(request_id, json.dumps({"status": "failed", "error": str(e)}))

    ch.basic_ack(delivery_tag=method.delivery_tag)

def start():
    rabbitmq_url = frappe.conf.get("rabbitmq_url") or "amqp://guest:guest@localhost:5672/"
    try:
        parameters = pika.URLParameters(rabbitmq_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue="audio_tts_queue", durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue="audio_tts_queue", on_message_callback=process_message)
        print(" [*] TTS Worker running. Waiting for messages...")
        channel.start_consuming()
    except Exception as e:
        print(f"[!] TTS Worker crashed: {str(e)}")