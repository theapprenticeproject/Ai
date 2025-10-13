# tap_ai/api/result.py

import frappe
import json

@frappe.whitelist(methods=["GET"], allow_guest=True)
def result(request_id: str):
    """
    Result API: Fetch the answer using the request_id.
    """
    cached = frappe.cache().get(request_id)
    if not cached:
        frappe.throw(f"No such request_id: {request_id}")

    return {**json.loads(cached)}
