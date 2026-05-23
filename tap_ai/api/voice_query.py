# tap_ai/api/voice_query.py
"""
Backward-compatible voice query alias.

Clients that pre-date the unified `query` endpoint call this URL directly.
It simply delegates to `tap_ai.api.query.query`, which handles both text
and voice based on whether `q` or `audio_url` is present in the POST body.

Endpoint:
    POST /api/method/tap_ai.api.voice_query.voice_query
    Body: { "audio_url": "...", "user_id": "..." }
"""

import frappe
from tap_ai.api.query import query


@frappe.whitelist(methods=["POST"], allow_guest=True)
def voice_query():
    """
    Backward-compatible alias for the unified query endpoint.
    Expects audio_url in POST body.
    """
    return query()