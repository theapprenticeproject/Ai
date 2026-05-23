# tap_ai/workers/tts_worker.py

import frappe
import time
import json
import pika
import os
import uuid
from openai import OpenAI
from loguru import logger
from frappe.utils import get_url


def _get_openai_client() -> OpenAI:
    api_key = frappe.conf.get("openai_api_key")
    if not api_key:
        frappe.throw("OpenAI API key not found")
    return OpenAI(api_key=api_key)


class TTSWorker:
    """
    RabbitMQ consumer for text-to-speech synthesis.

    Generates audio via OpenAI TTS, saves it to the Frappe File Manager,
    and updates the Redis request state with the public audio URL.

    Accepts optional rabbitmq_url and queue so tests can inject a local broker.
    """

    def __init__(
        self,
        rabbitmq_url: str | None = None,
        queue: str = "audio_tts_queue",
    ) -> None:
        self.rabbitmq_url = rabbitmq_url or frappe.conf.get("rabbitmq_url") or "amqp://guest:guest@localhost:5672/"
        self.queue = queue

    def process_message(self, ch, method, properties, body):
        payload = json.loads(body)
        request_id = payload.get("request_id")
        answer = payload.get("answer")
        language = payload.get("language", "en")
        transcribed_text = payload.get("transcribed_text", "")

        logger.info(f"TTS Worker generating audio for {request_id}")

        try:
            current_state = frappe.cache().get(request_id)
            state_dict = json.loads(current_state) if current_state else {}
            state_dict["status"] = "generating_audio"
            frappe.cache().set(request_id, json.dumps(state_dict))

            client = _get_openai_client()
            output_path = f"/tmp/{uuid.uuid4().hex}.mp3"

            with client.audio.speech.with_streaming_response.create(
                model="tts-1",
                voice="alloy",
                input=answer
            ) as r:
                r.stream_to_file(output_path)

            with open(output_path, "rb") as f:
                file_doc = frappe.get_doc({
                    "doctype": "File",
                    "file_name": os.path.basename(output_path),
                    "is_private": 0,
                    "content": f.read()
                })
                file_doc.insert(ignore_permissions=True)

            if os.path.exists(output_path):
                os.remove(output_path)

            public_audio_url = get_url(file_doc.file_url)
            state_dict.update({
                "status": "success",
                "audio_url": public_audio_url,
                "answer_text": answer,
                "transcribed_text": transcribed_text,
                "language": language
            })
            frappe.cache().set(request_id, json.dumps(state_dict))
            logger.info(f"{request_id} audio generated: {public_audio_url}")

        except Exception as e:
            logger.error(f"TTS failed for {request_id}: {e}")
            frappe.cache().set(request_id, json.dumps({"status": "failed", "error": str(e)}))

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
            logger.info(f"TTS Worker running on '{self.queue}'. Waiting for messages.")
            channel.start_consuming()
        except Exception as e:
            logger.critical(f"TTS Worker crashed: {e}")


def start():
    """Entry point called by the worker runner."""
    TTSWorker().start()
