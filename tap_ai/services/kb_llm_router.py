"""
KB LLM Router

When a query matches KB routing patterns but has no exact-text match,
a single LLM call scans the full Knowledge Bank to either match an entry
or generate a direct answer.

Flow:
1. Fetch all active KB entries (IDs, queries, alternate queries).
2. Pass user query + entire KB payload to one LLM prompt.
3. LLM returns a matched KB ID (kb_exact) or synthesizes a reply (llm_generated).
4. Render the final response (inject user variables) and return a structured dict.
5. Cache LLM outputs to reduce latency on repeated queries.
"""

import json
import time
from typing import Any, Dict, List, Optional

import frappe
from tap_ai.infra.config import get_config
from tap_ai.infra.llm_client import llm_invoke_cached
from tap_ai.services.direct_response_bank import (
    get_direct_response_entries,
    _render_response,
    _parse_aliases,
)
from tap_ai.services.prompt_bank import get_system_message_for_context


LLM_VERIFIER_CACHE_TTL = 900  # 15 minutes


SINGLE_PASS_KB_PROMPT = '''You are TAP Buddy, a supportive educational assistant.
I will provide you with a User Query and a Knowledge Bank (a list of allowed responses, their IDs, and the queries they match).

Task:
1. Scan the Knowledge Bank to find a semantic match for the User Query. Look at both the `student_query` and the `alternate_queries`.
2. If there is a strong semantic intent match, you MUST return EXACT JSON:
   {"match": "<id>", "source": "kb_exact", "answer": "<the exact KB response, personalized with student info if applicable>"}
3. If the user query is completely unrelated to anything in the Knowledge Bank, act as a helpful AI and answer directly. Return EXACT JSON:
   {"match": null, "source": "llm_generated", "answer": "<your concise, friendly, helpful 1-2 sentence response>"}

Rules:
- Do NOT invent or hallucinate IDs.
- Keep LLM generated replies concise, empathetic, and age-appropriate.
- Return ONLY valid JSON.
'''


def verify_and_respond(
    query: str,
    user_profile: Optional[Dict[str, Any]] = None,
    chat_history: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """
    STAGE 2: LLM FALLBACK WITH FULL KB CONTEXT
    ==========================================

    Called when regex matched but exact lookup failed.

    This ensures no regex-matched query is left unhandled by giving the LLM
    complete context about available KB responses. The LLM can:
    1. Match the query to a KB entry (even with slight variations)
    2. Return the matched KB response (no additional LLM generation needed)
    3. OR generate a custom answer if no KB match is appropriate

    Process:
    1. Load ALL active KB entries
    2. For each entry, collect match_queries (student_query + alternate_queries)
    3. Format lightweight payload: {id, match_queries, response}
    4. Pass entire KB to LLM with the user query
    5. LLM decides: {"match": entry_id, "source": "kb_exact", "answer": ...}
       OR:          {"match": null, "source": "llm_generated", "answer": "..."}
    6. If matched KB entry: return KB response (with variable rendering)
       If LLM generated: return LLM answer

    Timing: ~200-500ms (1 LLM call with full context)
    """
    start = time.perf_counter()
    chat_history = chat_history or []

    # 1. Fetch ALL active entries
    all_entries = get_direct_response_entries()

    # 2. Format a lightweight payload to save tokens
    entries_payload = []
    for e in all_entries:
        if not e or not e.get("is_active", 1):
            continue

        alt_queries = _parse_aliases(e.get("alternate_queries"))
        all_match_queries = [e.get("student_query")] + alt_queries

        entries_payload.append({
            "id": e.get("name"),
            "match_queries": [q for q in all_match_queries if q],
            "response": e.get("response"),
        })

    # 3. Build LLM messages
    try:
        persona = get_system_message_for_context(user_profile=user_profile)
    except Exception:
        persona = ""

    messages: List = [("system", SINGLE_PASS_KB_PROMPT)]
    if persona:
        messages.append(("system", f"When generating a direct answer (no KB match), speak as this persona:\n{persona}"))
    if chat_history:
        messages.append(("system", "Recent chat context: " + " | ".join([m.get("content", "") for m in chat_history[-3:]])))
    messages.append(("user", json.dumps({"user_query": query, "knowledge_bank": entries_payload}, ensure_ascii=False)))

    # 4. Invoke LLM
    model = get_config("primary_llm_model") or "gpt-4o-mini"
    raw_selection = llm_invoke_cached(
        messages, model=model, temperature=0.1, cache_ttl=LLM_VERIFIER_CACHE_TTL, max_tokens=800
    )

    # 5. Parse LLM output
    try:
        cleaned = raw_selection.replace("```json", "").replace("```", "").strip()
        decision = json.loads(cleaned)
    except Exception:
        # Failsafe: malformed JSON — invoke a bare fallback
        fallback_system = persona or "You are TAP Buddy. Answer concisely."
        fallback = llm_invoke_cached(
            [("system", fallback_system), ("user", query)],
            model=model,
            temperature=0.3,
            cache_ttl=LLM_VERIFIER_CACHE_TTL,
            max_tokens=300,
        )
        timing_ms = int((time.perf_counter() - start) * 1000)
        return {
            "question": query,
            "answer": str(fallback).strip(),
            "response_type": "llm_generated",
            "user_context": "personalized" if user_profile else "general",
            "metadata": {"decision": "fallback_malformed_json", "timing_ms": timing_ms},
        }

    # 6. Extract result
    match_id = decision.get("match")
    source = decision.get("source")
    answer_text = decision.get("answer", "")

    if match_id and source == "kb_exact":
        answer_text = _render_response(answer_text, user_profile=user_profile)
        response_type = "knowledge_bank"
    else:
        response_type = "llm_generated"

    timing_ms = int((time.perf_counter() - start) * 1000)
    return {
        "question": query,
        "answer": answer_text,
        "response_type": response_type,
        "user_context": "personalized" if user_profile else "general",
        "metadata": {
            "matched_id": match_id,
            "decision": source,
            "timing_ms": timing_ms,
        },
    }
