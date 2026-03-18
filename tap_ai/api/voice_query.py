# tap_ai/api/voice_query.py

import frappe
import json
import uuid
from tap_ai.utils.mq import publish_to_queue

@frappe.whitelist(methods=["POST"], allow_guest=True)
def voice_query():
    """
    Voice Query API
    Returns request_id immediately. 
    Actual STT, LLM, and TTS processing are handled by RabbitMQ workers.
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

    # Publish to RabbitMQ STT Queue instead of frappe.enqueue
    payload = {
        "request_id": request_id,
        "audio_url": audio_url,
        "user_id": user_id
    }
    publish_to_queue("audio_stt_queue", payload)

    return {"request_id": request_id}