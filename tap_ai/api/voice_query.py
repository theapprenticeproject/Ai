# tap_ai/api/voice_query.py

import frappe
from tap_ai.api.query import query


@frappe.whitelist(methods=["POST"], allow_guest=True)
def voice_query():
    """
    Backward-compatible alias for the unified query endpoint.
    Expects audio_url in POST body.
    """
    return query()