import os
import uuid
from urllib.parse import urlparse

import frappe
import requests
from openai import OpenAI

from tap_ai.services.ratelimit import check_rate_limit

SUPPORTED_AUDIO_EXTENSIONS = {"mp3", "wav", "m4a", "ogg", "webm", "flac", "mp4", "mpeg", "mpga"}


def _extract_api_key() -> str | None:
    auth = frappe.get_request_header("Authorization") or ""
    if not auth.lower().startswith("token "):
        return None
    try:
        return auth.split()[1].split(":")[0]
    except Exception:
        return None


def _resolve_user_id(data: dict) -> str:
    user_id = data.get("user_id")
    if not user_id or user_id == "Guest":
        user_id = frappe.session.user
    return user_id


def _get_openai_client() -> OpenAI:
    api_key = frappe.conf.get("openai_api_key")
    if not api_key:
        raise ValueError("OpenAI API key not found")
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
    if not text:
        return "en"

    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": "Determine the language the user intended to speak. Ignore the script. Reply ONLY with ISO code like en, hi.",
            },
            {"role": "user", "content": text},
        ],
        temperature=0,
    )
    language = (completion.choices[0].message.content or "").strip().lower()
    return language or "en"


@frappe.whitelist(methods=["POST"], allow_guest=True)
def transcribe():
    """
    Synchronous STT API.
    Input (POST): audio_url, optional user_id
    Output: {status, transcribed_text, language, audio_url, error}
    """
    data = frappe.local.form_dict or {}
    audio_url = (data.get("audio_url") or "").strip()
    user_id = _resolve_user_id(data)

    if not audio_url:
        return {
            "status": "failed",
            "transcribed_text": None,
            "language": None,
            "audio_url": None,
            "error": "Missing audio_url",
        }

    api_key = _extract_api_key()
    ok, _, reset = check_rate_limit(
        api_key=api_key,
        scope=f"stt_api_{user_id}",
        limit=30,
        window_sec=60,
    )
    if not ok:
        return {
            "status": "failed",
            "transcribed_text": None,
            "language": None,
            "audio_url": audio_url,
            "error": f"STT rate limit exceeded. Try again in {reset} seconds.",
        }

    input_path = None
    try:
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
                file=f,
            )

        transcribed_text = (transcript.text or "").strip()
        language = _detect_intent_language(client, transcribed_text)

        return {
            "status": "success",
            "transcribed_text": transcribed_text,
            "language": language,
            "audio_url": audio_url,
            "error": None,
        }
    except Exception as exc:
        frappe.log_error(f"STT API Error: {exc}", "tap_ai.api.transcribe")
        return {
            "status": "failed",
            "transcribed_text": None,
            "language": None,
            "audio_url": audio_url,
            "error": str(exc),
        }
    finally:
        if input_path and os.path.exists(input_path):
            os.remove(input_path)
