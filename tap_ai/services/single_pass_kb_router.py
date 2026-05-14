"""
Single-Pass LLM Knowledge Bank Router

Flow:
1. Fetch all active Knowledge Bank entries (IDs, queries, and alternate queries).
2. Pass the user's query and the entire Knowledge Bank payload to a single LLM prompt.
3. The LLM performs a semantic sweep of all entries. 
    - If a strong intent match is found, it returns the matched KB ID.
    - If no match is found, it acts autonomously and synthesizes a direct response.
4. Render the final response (injecting user variables) and return the structured dict.
5. Cache LLM outputs to reduce latency on repeated queries.

This router replaces the previous two-step (classifier + selector) pipeline, eliminating 
classification bottlenecks and reducing sequential API network latency.
"""

import json
import time
import hashlib
from typing import Any, Dict, List, Optional

import frappe
from tap_ai.infra.config import get_config
from tap_ai.infra.llm_client import LLMClient
from tap_ai.services.direct_response_bank import (
    # probe_direct_response_match,  # probe-based flow replaced by LLM-first
    get_direct_response_entries,
    get_entries_for_category,
    _render_response,
)
from tap_ai.services.prompt_bank import get_system_message_for_context


LLM_VERIFIER_CACHE_TTL = 900  # 15 minutes


def _llm(model: Optional[str] = None, temperature: float = 0.0, max_tokens: int = 800):
    return LLMClient.get_client(model=model or (get_config("primary_llm_model") or "gpt-4o-mini"),
                                temperature=temperature, max_tokens=max_tokens)


def _llm_invoke_cached(messages: List, model: str, temperature: float = 0.0, cache_ttl: int = LLM_VERIFIER_CACHE_TTL, max_tokens: int = 800) -> str:
    try:
        payload = {"messages": messages, "model": model, "temperature": temperature}
        cache_key = "llm_verifier:" + hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
        cached = frappe.cache().get(cache_key)
        if cached:
            if isinstance(cached, bytes):
                cached = cached.decode("utf-8", errors="ignore")
            print(f"> Verifier LLM cache hit {cache_key[:12]}...")
            return str(cached)
    except Exception:
        cache_key = None

    llm = _llm(model=model, temperature=temperature, max_tokens=max_tokens)
    start = time.time()
    resp = llm.invoke(messages)
    content = getattr(resp, "content", "") or ""
    content = str(content).strip()

    try:
        if cache_key and content:
            frappe.cache().set(cache_key, content, ex=cache_ttl)
    except Exception:
        pass

    print(f"> Verifier LLM invoke ({model}) took {int((time.time() - start) * 1000)}ms")
    return content


SYSTEM_PROMPT = '''You are an assistant that decides whether a curated Knowledge Bank response matches the user's intent.

Input includes:
- User query
- Candidate knowledge-bank entry metadata: title, matched_query, match_score, category
- The curated KB response text

Task:
1) If the user's intent is the same as the candidate's intent, return JSON: {"action": "use_kb", "final_answer": "<the KB response possibly lightly personalized>", "reason": "short explanation"}
2) If the intent is different, return JSON: {"action": "llm_answer", "final_answer": "<LLM-generated answer>", "reason": "short explanation"}

Intent matters more than fuzzy score.
If the query is a greeting, small talk, identity question, or program explanation and the KB candidate matches that same intent, use KB.
If the query asks something else, do not force KB just because the response is semantically related.

Be concise. Preserve any essential facts from the KB when using it. Personalize using student name/grade if provided.

Return ONLY JSON and nothing else.
'''


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

def verify_and_respond(query: str, user_profile: Optional[Dict[str, Any]] = None, chat_history: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
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
    
    Examples of queries handled here:
      Regex Match: "heyyyy"        → LLM recognizes "hey" variant → Returns KB "Hey" response
      Regex Match: "gud morning"   → LLM recognizes "good morning" variant → Returns KB response
      Regex Match: "submit kartau" → No good KB match → LLM generates supportive answer
    """
    start = time.perf_counter()
    chat_history = chat_history or []

    # 1. Fetch ALL active entries directly
    all_entries = get_direct_response_entries()
    
    # 2. Format a lightweight payload to save tokens
    # We combine student_query and alternate_queries to give the LLM max context
    entries_payload = []
    for e in all_entries:
        if not e or not e.get("is_active", 1):
            continue
            
        # Parse alternate queries from string/list to flat list
        from tap_ai.services.direct_response_bank import _parse_aliases
        alt_queries = _parse_aliases(e.get("alternate_queries"))
        all_match_queries = [e.get("student_query")] + alt_queries
        
        entries_payload.append({
            "id": e.get("name"),
            "match_queries": [q for q in all_match_queries if q], # remove empty
            "response": e.get("response")
        })

    # 3. Build the LLM Messages
    messages = []
    messages.append(("system", SINGLE_PASS_KB_PROMPT))
    
    if chat_history:
        messages.append(("system", "Recent chat context: " + " | ".join([m.get('content','') for m in chat_history[-3:]])))

    messages.append(("user", json.dumps({
        "user_query": query, 
        "knowledge_bank": entries_payload
    }, ensure_ascii=False)))

    # 4. Invoke LLM
    model = get_config("primary_llm_model") or "gpt-4o-mini"
    # Note: max_tokens increased to allow for longer prompt processing
    raw_selection = _llm_invoke_cached(messages, model=model, temperature=0.1, max_tokens=800)

    # 5. Parse the LLM output
    try:
        cleaned = raw_selection.replace("```json", "").replace("```", "").strip()
        decision = json.loads(cleaned)
    except Exception:
        # Failsafe: if LLM breaks JSON format, do a raw fallback
        fallback = _llm_invoke_cached([("system", "You are TAP Buddy. Answer concisely."), ("user", query)], model=model, temperature=0.3, max_tokens=300)
        timing_ms = int((time.perf_counter() - start) * 1000)
        return {
            "question": query, 
            "answer": str(fallback).strip(), 
            "response_type": "llm_generated", 
            "user_context": "personalized" if user_profile else "general", 
            "metadata": {"decision": "fallback_malformed_json", "timing_ms": timing_ms}
        }

    # 6. Extract results
    match_id = decision.get("match")
    source = decision.get("source")
    answer_text = decision.get("answer", "")

    # If it chose a KB exact match, apply your rendering (name replacement, etc.)
    if match_id and source == "kb_exact":
        # Render the response just in case the LLM didn't perfectly replace variables
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
            "timing_ms": timing_ms
        },
    }