# tap_ai/workers/stt_worker.py

import frappe
import json
import pika
import requests
import os
import time
import uuid
import traceback
from urllib.parse import urlparse
from openai import OpenAI
from loguru import logger
from tap_ai.utils.mq import publish_to_queue

SUPPORTED_AUDIO_EXTENSIONS = {"mp3", "wav", "m4a", "ogg", "webm", "flac", "mp4", "mpeg", "mpga"}


def _get_openai_client() -> OpenAI:
    api_key = frappe.conf.get("openai_api_key")
    if not api_key:
        frappe.throw("OpenAI API key not found")
    return OpenAI(api_key=api_key)


def _get_audio_extension(audio_url: str, content_type: str | None) -> str:
    path = urlparse(audio_url).path
    ext = os.path.splitext(path)[1].replace(".", "").lower()
    if ext in SUPPORTED_AUDIO_EXTENSIONS:
        return ext
    if content_type and "audio/" in content_type:
        guessed = content_type.split("/")[-1].lower()
        if guessed in SUPPORTED_AUDIO_EXTENSIONS:
            return guessed
    return "mp3"


def _detect_intent_language(client: OpenAI, text: str) -> str:
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Determine the language the user intended to speak. Ignore the script. Reply ONLY with ISO code like en, hi."},
            {"role": "user", "content": text}
        ],
        temperature=0
    )
    return completion.choices[0].message.content.strip().lower()


class STTWorker:
    """
    RabbitMQ consumer for speech-to-text transcription.

    Downloads audio from a URL, transcribes via Whisper, detects language,
    then forwards the text to the LLM worker queue.

    Accepts optional rabbitmq_url and queue so tests can inject a local broker.
    """

    def __init__(
        self,
        rabbitmq_url: str | None = None,
        queue: str = "audio_stt_queue",
    ) -> None:
        self.rabbitmq_url = rabbitmq_url or frappe.conf.get("rabbitmq_url") or "amqp://guest:guest@localhost:5672/"
        self.queue = queue

    def process_message(self, ch, method, properties, body):
        payload = json.loads(body)
        request_id = payload.get("request_id")
        audio_url = payload.get("audio_url")
        user_id = payload.get("user_id")
        input_path = None
        response = None
        stt_started_at_ms = int(time.time() * 1000)

        logger.info(f"STT Worker processing {request_id} from {audio_url}")

        try:
            current_state = frappe.cache().get(request_id)
            state_dict = json.loads(current_state) if current_state else {}
            state_dict["status"] = "transcribing"
            frappe.cache().set(request_id, json.dumps(state_dict))

            client = _get_openai_client()

            response = requests.get(audio_url, timeout=20)
            response.raise_for_status()
            ext = _get_audio_extension(audio_url, response.headers.get("Content-Type"))
            input_path = f"/tmp/{uuid.uuid4().hex}.{ext}"

            with open(input_path, "wb") as f:
                f.write(response.content)

            with open(input_path, "rb") as f:
                transcript = client.audio.transcriptions.create(
                    model="whisper-1",
                    file=f
                )

            text = transcript.text.strip()
            language = _detect_intent_language(client, text)

            logger.info(f"Transcribed: '{text}' (language: {language})")

            state_dict.update({
                "status": "transcribed",
                "transcribed_text": text,
                "language": language,
                "stt_timing_ms": int(time.time() * 1000) - stt_started_at_ms,
            })
            state_dict.setdefault("metadata", {})
            state_dict["metadata"].setdefault("timings_ms", {})
            state_dict["metadata"]["timings_ms"]["stt"] = state_dict["stt_timing_ms"]
            frappe.cache().set(request_id, json.dumps(state_dict))

            publish_to_queue("text_query_queue", {
                "request_id": request_id,
                "query": text,
                "user_id": user_id,
                "is_voice": True,
                "language": language
            })
            logger.info(f"{request_id} routed to LLM Worker")

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

            logger.error(f"STT failed for {request_id}: {error_message}")
            logger.debug(f"STT context: {json.dumps(error_context, default=str)}")
            logger.debug(f"STT traceback:\n{tb}")

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

    def start(self) -> None:
        """Initialize RabbitMQ connection and begin consuming."""
        try:
            parameters = pika.URLParameters(self.rabbitmq_url)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue=self.queue, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=self.queue, on_message_callback=self.process_message)
            logger.info(f"STT Worker running on '{self.queue}'. Waiting for messages.")
            channel.start_consuming()
        except Exception as e:
            logger.critical(f"STT Worker crashed: {e}")


def start():
    """Entry point called by the worker runner."""
    STTWorker().start()
