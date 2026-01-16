import frappe
import requests
import uuid
import os
import time
from urllib.parse import urlparse
from openai import OpenAI


# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

BASE_URL = "https://ai.evalix.xyz"

SUPPORTED_AUDIO_EXTENSIONS = {
    "mp3", "wav", "m4a", "ogg", "webm", "flac", "mp4", "mpeg", "mpga"
}


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def get_openai_client():
    api_key = frappe.conf.get("openai_api_key")
    if not api_key:
        frappe.throw("OpenAI API key not found in site_config.json")
    return OpenAI(api_key=api_key)


client = get_openai_client()


def detect_intent_language(text: str) -> str:
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "Determine the language the user intended to speak. "
                    "Ignore the script. Reply ONLY with a lowercase ISO code "
                    "such as en, hi, ta."
                )
            },
            {"role": "user", "content": text}
        ],
        temperature=0
    )
    return completion.choices[0].message.content.strip().lower()


def get_audio_extension(audio_url: str, content_type: str | None) -> str:
    """
    Detect audio extension from URL or Content-Type header
    """
    # 1️. Try URL extension
    path = urlparse(audio_url).path
    ext = os.path.splitext(path)[1].replace(".", "").lower()

    if ext in SUPPORTED_AUDIO_EXTENSIONS:
        return ext

    # 2. Try Content-Type header
    if content_type:
        if "audio/" in content_type:
            guessed = content_type.split("/")[-1].lower()
            if guessed in SUPPORTED_AUDIO_EXTENSIONS:
                return guessed

    # Safe fallback
    return "mp3"


def call_query_api(text: str, user_id: str) -> str:
    r = requests.post(
        f"{BASE_URL}/api/method/tap_ai.api.query.query",
        params={
            "q": text,
            "user_id": user_id
        },
        timeout=30
    )
    r.raise_for_status()
    return r.json()["message"]["request_id"]


def poll_result_api(request_id: str, timeout_sec: int = 60) -> dict:
    start = time.time()

    while time.time() - start < timeout_sec:
        r = requests.get(
            f"{BASE_URL}/api/method/tap_ai.api.result.result",
            params={"request_id": request_id},
            timeout=60
        )
        r.raise_for_status()
        data = r.json()["message"]

        if data.get("status") in ("success", "failed"):
            return data

        time.sleep(1.5)

    return {"status": "failed", "answer": "Request timed out"}


# ---------------------------------------------------------
# API
# ---------------------------------------------------------

@frappe.whitelist(methods=["POST"], allow_guest=True)
def process():
    data = frappe.local.form_dict or {}

    audio_url = data.get("audio_url")
    user_id = data.get("user_id", "voice_test_user")

    if not audio_url:
        frappe.throw("audio_url is required")

    # -----------------------------------------------------
    # 1. Download audio (ANY FORMAT)
    # -----------------------------------------------------
    response = requests.get(audio_url, timeout=20)
    audio_bytes = response.content

    ext = get_audio_extension(audio_url, response.headers.get("Content-Type"))
    input_path = f"/tmp/{uuid.uuid4().hex}.{ext}"

    with open(input_path, "wb") as f:
        f.write(audio_bytes)

    # -----------------------------------------------------
    # 2. Speech-to-Text (STT)
    # -----------------------------------------------------
    with open(input_path, "rb") as f:
        transcript = client.audio.transcriptions.create(
            model="gpt-4o-transcribe",
            file=f
        )

    text = transcript.text.strip()

    # -----------------------------------------------------
    # 3. Detect intended language
    # -----------------------------------------------------
    language = detect_intent_language(text)

    # -----------------------------------------------------
    # 4. Call core AI (force reply language)
    # -----------------------------------------------------
    forced_prompt = f"Answer strictly in {language}. {text}"

    request_id = call_query_api(forced_prompt, user_id)

    # -----------------------------------------------------
    # 5. Poll result
    # -----------------------------------------------------
    result = poll_result_api(request_id)

    if result.get("status") != "success":
        frappe.throw("Failed to get answer from core API")

    answer = result.get("answer", "").strip()

    # -----------------------------------------------------
    # 6. Text-to-Speech (TTS)
    # -----------------------------------------------------
    output_path = f"/tmp/{uuid.uuid4().hex}.mp3"

    with client.audio.speech.with_streaming_response.create(
        model="gpt-4o-mini-tts",
        voice="alloy",
        input=answer
    ) as r:
        r.stream_to_file(output_path)

    # -----------------------------------------------------
    # 7. Save audio file
    # -----------------------------------------------------
    with open(output_path, "rb") as f:
        file_doc = frappe.get_doc({
            "doctype": "File",
            "file_name": os.path.basename(output_path),
            "is_private": 0,
            "content": f.read()
        })
        file_doc.insert(ignore_permissions=True)

    # -----------------------------------------------------
    # 8. Response
    # -----------------------------------------------------
    return {
        "transcribed_text": text,
        "answer_text": answer,
        "audio_url": file_doc.file_url,
        "language": language
    }
