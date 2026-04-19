# tap_ai/workers/stt_worker.py

import frappe
import json
import pika
import requests
import os
import uuid
import traceback
from urllib.parse import urlparse
from openai import OpenAI
from tap_ai.utils.mq import publish_to_queue

SUPPORTED_AUDIO_EXTENSIONS = {"mp3", "wav", "m4a", "ogg", "webm", "flac", "mp4", "mpeg", "mpga"}

def get_openai_client():
    api_key = frappe.conf.get("openai_api_key")
    if not api_key:
        frappe.throw("OpenAI API key not found")
    return OpenAI(api_key=api_key)

def get_audio_extension(audio_url, content_type):
    path = urlparse(audio_url).path
    ext = os.path.splitext(path)[1].replace(".", "").lower()
    if ext in SUPPORTED_AUDIO_EXTENSIONS:
        return ext
    if content_type and "audio/" in content_type:
        guessed = content_type.split("/")[-1].lower()
        if guessed in SUPPORTED_AUDIO_EXTENSIONS:
            return guessed
    return "mp3"

def detect_intent_language(client, text: str) -> str:
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Determine the language the user intended to speak. Ignore the script. Reply ONLY with ISO code like en, hi."},
            {"role": "user", "content": text}
        ],
        temperature=0
    )
    return completion.choices[0].message.content.strip().lower()

def process_message(ch, method, properties, body):
    payload = json.loads(body)
    request_id = payload.get("request_id")
    audio_url = payload.get("audio_url")
    user_id = payload.get("user_id")
    input_path = None
    response = None

    print(f"\n[*] [STT Worker] Processing {request_id} from {audio_url}")

    try:
        # Update state
        current_state = frappe.cache().get(request_id)
        state_dict = json.loads(current_state) if current_state else {}
        state_dict["status"] = "transcribing"
        frappe.cache().set(request_id, json.dumps(state_dict))

        client = get_openai_client()

        # Download audio
        response = requests.get(audio_url, timeout=20)
        response.raise_for_status()
        ext = get_audio_extension(audio_url, response.headers.get("Content-Type"))
        input_path = f"/tmp/{uuid.uuid4().hex}.{ext}"

        with open(input_path, "wb") as f:
            f.write(response.content)

        # STT via OpenAI
        with open(input_path, "rb") as f:
            transcript = client.audio.transcriptions.create(
                model="whisper-1", # Adjust if you strictly need gpt-4o-transcribe
                file=f
            )

        text = transcript.text.strip()
        language = detect_intent_language(client, text)
        
        print(f"[>] Transcribed: '{text}' (Language: {language})")

        # Update intermediate state
        state_dict.update({
            "status": "transcribed",
            "transcribed_text": text,
            "language": language
        })
        frappe.cache().set(request_id, json.dumps(state_dict))

        # Forward to LLM Worker
        publish_to_queue("text_query_queue", {
            "request_id": request_id,
            "query": text,
            "user_id": user_id,
            "is_voice": True, # Crucial flag so LLM knows to send it to TTS next
            "language": language
        })
        print(f"[✓] {request_id} routed to LLM Worker")

    except Exception as e:
        err_type = type(e).__name__
        tb = traceback.format_exc()
        error_message = f"{err_type}: {repr(e)}"
        error_context = {
            "request_id": request_id,
            "audio_url": audio_url,
            "user_id": user_id,
            "http_status": getattr(response, "status_code", None),
            "content_type": response.headers.get("Content-Type") if response is not None else None,
        }

        print(f"[x] STT failed for {request_id}: {error_message}")
        print(f"[x] STT context: {json.dumps(error_context, default=str)}")
        print(f"[x] STT traceback:\n{tb}")

        frappe.log_error(
            message=(
                f"STT Worker failed\n"
                f"Error: {error_message}\n"
                f"Context: {json.dumps(error_context, default=str)}\n"
                f"Traceback:\n{tb}"
            ),
            title="tap_ai STT Worker Error",
        )

        if request_id:
            frappe.cache().set(
                request_id,
                json.dumps({
                    "status": "failed",
                    "error": error_message,
                    "error_type": err_type,
                }),
            )
    finally:
        if input_path and os.path.exists(input_path):
            os.remove(input_path)

    ch.basic_ack(delivery_tag=method.delivery_tag)

def start():
    rabbitmq_url = frappe.conf.get("rabbitmq_url") or "amqp://guest:guest@localhost:5672/"
    try:
        parameters = pika.URLParameters(rabbitmq_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue="audio_stt_queue", durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue="audio_stt_queue", on_message_callback=process_message)
        print(" [*] STT Worker running. Waiting for messages...")
        channel.start_consuming()
    except Exception as e:
        print(f"[!] STT Worker crashed: {str(e)}")