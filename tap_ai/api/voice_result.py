# tap_ai/api/voice_result.py

import frappe
import json
import requests
from openai import OpenAI

BASE_URL = "https://ai.evalix.xyz"


def get_openai_client():  
    """Lazy initialization - only create client when needed"""  
    api_key = frappe.conf.get("openai_api_key")  
    if not api_key:  
        frappe.throw("OpenAI API key not found")  
    return OpenAI(api_key=api_key) 


@frappe.whitelist(methods=["GET"], allow_guest=True)  
def voice_result(request_id: str):  
    """  
    Voice Result API  
    Returns final voice + text output from worker cache  
    """  
    cached = frappe.cache().get(request_id)  
    if not cached:  
        frappe.throw(f"No such request_id: {request_id}")  
  
    data = json.loads(cached)  
  
    # Return cached result immediately - no duplicate processing  
    if data.get("status") in ["failed", "success"]:  
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
  
    # Return processing status - TTS is handled by tts_worker  
    return {  
        "status": "processing",  
        "transcribed_text": data.get("transcribed_text"),  
        "answer_text": core_data.get("answer", ""),  
        "language": data.get("language")  
    }