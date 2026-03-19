# tap_ai/api/voice_query.py

import frappe
import json
import uuid
from tap_ai.utils.mq import publish_to_queue
from tap_ai.services.ratelimit import check_rate_limit 

@frappe.whitelist(methods=["POST"], allow_guest=True)  
def voice_query():  
    """  
    Voice Query API  
    Returns request_id immediately.   
    Actual STT, LLM, and TTS processing are handled by RabbitMQ workers.  
    """  
    data = frappe.local.form_dict or {}  
    audio_url = data.get("audio_url")  
      
    # Secure user_id extraction - don't trust POST body blindly  
    user_id = data.get("user_id")  
    if not user_id or user_id == "Guest":  
        user_id = frappe.session.user  
  
    if not audio_url:  
        frappe.throw("audio_url is required")  
  
    # Add rate limiting (same as text API but more restrictive for voice)  
    auth = frappe.get_request_header("Authorization") or ""  
    api_key = None  
    if auth.lower().startswith("token "):  
        try:  
            api_key = auth.split()[1].split(":")[0]  
        except Exception:  
            api_key = None  
  
    # Voice queries are more expensive - lower limit  
    ok, remaining, reset = check_rate_limit(  
        api_key=api_key,  
        scope=f"voice_api_{user_id}",   
        limit=30,  # Half of text API limit  
        window_sec=60  
    )  
    if not ok:  
        frappe.throw(  
            f"Voice query rate limit exceeded. Try again in {reset} seconds.",  
            frappe.TooManyRequestsError,  
        )  
  
    request_id = f"VREQ_{uuid.uuid4().hex[:8]}"  
  
    # Save initial pending state with TTL  
    frappe.cache().set(request_id, json.dumps({  
        "status": "pending",  
        "user_id": user_id  
    }), ex=3600)  # 1 hour TTL  
  
    # Publish to RabbitMQ STT Queue  
    payload = {  
        "request_id": request_id,  
        "audio_url": audio_url,  
        "user_id": user_id  
    }  
    publish_to_queue("audio_stt_queue", payload)  
  
    return {"request_id": request_id}