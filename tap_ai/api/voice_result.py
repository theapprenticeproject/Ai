# tap_ai/api/voice_result.py

import frappe
from tap_ai.api.result import result

@frappe.whitelist(methods=["GET"], allow_guest=True)  
def voice_result(request_id: str):  
    """
    Backward-compatible alias for the unified result endpoint.
    """
    return result(request_id)