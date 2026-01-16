# tap_ai/api/voice_query.py

import frappe
import json
import uuid
import requests
import os
from urllib.parse import urlparse
from openai import OpenAI

BASE_URL = "https://ai.evalix.xyz"

SUPPORTED_AUDIO_EXTENSIONS = {
    "mp3", "wav", "m4a", "ogg", "webm", "flac", "mp4", "mpeg", "mpga"
}


def get_openai_client():
    api_key = frappe.conf.get("openai_api_key")
    if not api_key:
        frappe.throw("OpenAI API key not found")
    return OpenAI(api_key=api_key)


client = get_openai_client()


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


def detect_intent_language(text: str) -> str:
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "Determine the language the user intended to speak. "
                    "Ignore the script. Reply ONLY with ISO code like en, hi."
                )
            },
            {"role": "user", "content": text}
        ],
        temperature=0
    )
    return completion.choices[0].message.content.strip().lower()


@frappe.whitelist(methods=["POST"], allow_guest=True)
def voice_query():
    """
    Voice Query API
    Returns request_id immediately
    """
    data = frappe.local.form_dict or {}
    audio_url = data.get("audio_url")
    user_id = data.get("user_id", frappe.session.user)

    if not audio_url:
        frappe.throw("audio_url is required")

    request_id = f"VREQ_{uuid.uuid4().hex[:8]}"

    # Save initial pending state
    frappe.cache().set(request_id, json.dumps({
        "status": "pending",
        "user_id": user_id
    }))

    # Enqueue background processing
    frappe.enqueue(
        "tap_ai.api.voice_query._process_voice",
        queue="long",
        timeout=300,
        request_id=request_id,
        audio_url=audio_url,
        user_id=user_id
    )

    return {"request_id": request_id}


def _process_voice(request_id: str, audio_url: str, user_id: str):
    """
    Background job:
    STT → detect language → core AI → TTS → save result
    """
    try:
        # Download audio
        response = requests.get(audio_url, timeout=20)
        ext = get_audio_extension(audio_url, response.headers.get("Content-Type"))
        input_path = f"/tmp/{uuid.uuid4().hex}.{ext}"

        with open(input_path, "wb") as f:
            f.write(response.content)

        # STT
        with open(input_path, "rb") as f:
            transcript = client.audio.transcriptions.create(
                model="gpt-4o-transcribe",
                file=f
            )

        text = transcript.text.strip()
        language = detect_intent_language(text)

        # Call core query API
        forced_prompt = f"Answer strictly in {language}. {text}"

        r = requests.post(
            f"{BASE_URL}/api/method/tap_ai.api.query.query",
            params={"q": forced_prompt, "user_id": user_id},
            timeout=30
        )
        r.raise_for_status()
        core_request_id = r.json()["message"]["request_id"]

        # Save intermediate state
        frappe.cache().set(request_id, json.dumps({
            "status": "processing",
            "core_request_id": core_request_id,
            "language": language,
            "transcribed_text": text
        }))

    except Exception as e:
        frappe.cache().set(request_id, json.dumps({
            "status": "failed",
            "error": str(e)
        }))
        frappe.log_error(str(e), "Voice Query Failed")
