# tap_ai/api/voice_test.py

import frappe
import requests
import uuid
import os
import time
from openai import OpenAI


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def get_openai_client():
    api_key = frappe.conf.get("openai_api_key")
    if not api_key:
        frappe.throw("OpenAI API key not found in site_config.json")
    return OpenAI(api_key=api_key)


def call_query_api(text: str, user_id: str) -> str:
    r = requests.post(
        "http://tap.localhost/api/method/tap_ai.api.query.query",
        params={
            "q": text,
            "user_id": user_id
        },
        headers={
            "Content-Type": "application/json"
        },
        timeout=30
    )
    r.raise_for_status()
    return r.json()["message"]["request_id"]


def poll_result_api(request_id: str, timeout_sec: int = 30) -> dict:
    start = time.time()
    while time.time() - start < timeout_sec:
        r = requests.get(
            "http://tap.localhost/api/method/tap_ai.api.result.result",
            params={"request_id": request_id},
            headers={
                "Content-Type": "application/json"
            },
            timeout=60
        )
        r.raise_for_status()
        data = r.json()["message"]
        if data.get("status") in ("success", "failed"):
            return data
        time.sleep(1.5)

    return {"status": "failed", "answer": "Request timed out"}


def translate_back(answer: str, target_language: str) -> str:
    if target_language == "en":
        return answer

    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": f"Translate the following text to {target_language}"},
            {"role": "user", "content": answer}
        ]
    )
    return completion.choices[0].message.content


client = get_openai_client()


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
    # 1. Download audio
    # -----------------------------------------------------
    audio_bytes = requests.get(audio_url, timeout=20).content
    input_path = f"/tmp/{uuid.uuid4().hex}.ogg"

    with open(input_path, "wb") as f:
        f.write(audio_bytes)

    # -----------------------------------------------------
    # 2. STT
    # -----------------------------------------------------
    with open(input_path, "rb") as f:
        transcript = client.audio.transcriptions.create(
            model="gpt-4o-transcribe",
            file=f
        )

    text = transcript.text
    language = getattr(transcript, "language", "en")

    # -----------------------------------------------------
    # 3. Query core AI
    # -----------------------------------------------------
    request_id = call_query_api(text, user_id)

    # -----------------------------------------------------
    # 4. Poll result
    # -----------------------------------------------------
    result = poll_result_api(request_id)

    if result.get("status") != "success":
        frappe.throw("Failed to get answer from core API")

    answer = result.get("answer", "")
    final_answer = translate_back(answer, language)

    # -----------------------------------------------------
    # 5. TTS (IMPORTANT FIX)
    # -----------------------------------------------------
    output_path = f"/tmp/{uuid.uuid4().hex}.mp3"

    with client.audio.speech.with_streaming_response.create(
        model="gpt-4o-mini-tts",
        voice="alloy",
        input=final_answer
    ) as r:
        r.stream_to_file(output_path)

    # -----------------------------------------------------
    # 6. Save output file
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
    # Response
    # -----------------------------------------------------
    return {
        "transcribed_text": text,
        "answer_text": final_answer,
        "audio_url": file_doc.file_url,
        "language": language
    }
