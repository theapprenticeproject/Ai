# tap_ai/api/voice_result.py

import frappe
import json
import time
import uuid
import os
import requests
from openai import OpenAI

BASE_URL = "https://ai.evalix.xyz"


def get_openai_client():
    api_key = frappe.conf.get("openai_api_key")
    if not api_key:
        frappe.throw("OpenAI API key not found")
    return OpenAI(api_key=api_key)


client = get_openai_client()


@frappe.whitelist(methods=["GET"], allow_guest=True)
def voice_result(request_id: str):
    """
    Voice Result API
    Returns final voice + text output
    """
    cached = frappe.cache().get(request_id)
    if not cached:
        frappe.throw(f"No such request_id: {request_id}")

    data = json.loads(cached)

    if data.get("status") == "failed":
        return data

    if data.get("status") != "processing":
        return data

    core_request_id = data["core_request_id"]

    # Poll core result
    r = requests.get(
        f"{BASE_URL}/api/method/tap_ai.api.result.result",
        params={"request_id": core_request_id},
        timeout=30
    )
    r.raise_for_status()
    core_data = r.json()["message"]

    if core_data.get("status") != "success":
        return {"status": "processing"}

    answer = core_data.get("answer", "")

    # TTS
    output_path = f"/tmp/{uuid.uuid4().hex}.mp3"
    with client.audio.speech.with_streaming_response.create(
        model="gpt-4o-mini-tts",
        voice="alloy",
        input=answer
    ) as r:
        r.stream_to_file(output_path)

    # Save file
    with open(output_path, "rb") as f:
        file_doc = frappe.get_doc({
            "doctype": "File",
            "file_name": os.path.basename(output_path),
            "is_private": 0,
            "content": f.read()
        })
        file_doc.insert(ignore_permissions=True)

    final = {
        "status": "success",
        "transcribed_text": data.get("transcribed_text"),
        "answer_text": answer,
        "audio_url": file_doc.file_url,
        "language": data.get("language")
    }

    frappe.cache().set(request_id, json.dumps(final))
    return final
