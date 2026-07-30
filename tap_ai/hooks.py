app_name = "tap_ai"
app_title = "Tap Ai"
app_publisher = "Anish Aman"
app_description = "LMS system for tap"
app_email = "tech4dev@gmail.com"
app_license = "MIT"


# ======================================================
# DOC_EVENTS - Cache Invalidation for Knowledge Bank
# ======================================================
doc_events = {
	"TAP Response Knowledge": {
		"after_insert": [
			"tap_ai.services.kb.direct_response_bank.invalidate_kb_cache",
            "tap_ai.services.rag.pgvector_store.sync_kb_entry_to_pgvector",
            "tap_ai.services.routing.doctype_profiler.queue_kb_profile_refresh",
		],
		"after_update": [
			"tap_ai.services.kb.direct_response_bank.invalidate_kb_cache",
            "tap_ai.services.rag.pgvector_store.sync_kb_entry_to_pgvector",
            "tap_ai.services.routing.doctype_profiler.queue_kb_profile_refresh",
		],
		"after_delete": [
			"tap_ai.services.kb.direct_response_bank.invalidate_kb_cache",
            "tap_ai.services.rag.pgvector_store.delete_kb_entry_from_pgvector",
            "tap_ai.services.routing.doctype_profiler.queue_kb_profile_refresh",
		],
	}
}

# Invalidate prompt cache when prompt suggestions (if implemented as a doctype) change
doc_events["Prompt Suggestion"] = {
    "after_insert": "tap_ai.utils.prompt_bank.invalidate_prompt_cache",
    "after_update": "tap_ai.utils.prompt_bank.invalidate_prompt_cache",
    "after_delete": "tap_ai.utils.prompt_bank.invalidate_prompt_cache",
}

# ======================================================
# DOC_EVENTS - pgvector Auto-Sync for All Indexed DocTypes
# ======================================================
import json
import os

def _register_pgvector_sync_hooks():
    """
    Dynamically register pgvector sync hooks for all doctypes in the allowlist.
    Called on app startup via after_migrate hook.
    """
    global doc_events

    schema_path = os.path.join(os.path.dirname(__file__), "schema", "tap_ai_schema.json")
    try:
        with open(schema_path, "r") as f:
            schema_data = json.load(f)
            allowlist = schema_data.get("allowlist", [])
            # Strip "tab" prefix to get actual doctype names
            pgvector_sync_doctypes = [dt.replace("tab", "") for dt in allowlist]

            # Register hooks for all allowed doctypes
            for doctype in pgvector_sync_doctypes:
                doc_events.setdefault(doctype, {})
                doc_events[doctype].update({
                    "after_insert": [
                        "tap_ai.services.rag.pgvector_store.sync_to_pgvector_on_insert",
                        "tap_ai.services.routing.doctype_profiler.queue_profile_refresh",
                    ],
                    "after_update": [
                        "tap_ai.services.rag.pgvector_store.sync_to_pgvector_on_update",
                        "tap_ai.services.routing.doctype_profiler.queue_profile_refresh",
                    ],
                    "after_delete": [
                        "tap_ai.services.routing.doctype_profiler.queue_profile_refresh",
                    ],
                })

            print(f"[tap_ai] Registered pgvector sync hooks for {len(pgvector_sync_doctypes)} doctypes")
    except Exception as e:
        print(f"[tap_ai] Warning: Could not register pgvector sync hooks: {e}")

# Call on app startup
_register_pgvector_sync_hooks()


# ======================================================
# AFTER_MIGRATE - Re-register hooks on every app restart
# ======================================================
after_migrate = [
    "tap_ai.hooks._register_pgvector_sync_hooks"
]


