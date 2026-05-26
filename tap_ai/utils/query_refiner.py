"""Query refinement utility — rewrites follow-up questions into standalone queries.

Uses recent chat history to resolve pronouns and contextual references so that
all downstream engines (router, RAG, SQL) receive a self-contained question.

Greetings and unconditional KB intents should bypass this entirely — their
meaning is fixed regardless of conversation history.
"""

from __future__ import annotations

from typing import Dict, List

import frappe
from loguru import logger

from tap_ai.infra.config import get_config
from tap_ai.infra.llm_client import llm_invoke_cached


REFINER_PROMPT = """Given a chat history and a follow-up question, rewrite the follow-up question to be a standalone question that a search engine can understand.

- If already standalone, return as is
- Incorporate relevant context from history
- Do NOT answer the question

Return ONLY the refined question.
"""

_FOLLOW_UP_MARKERS = (
    "it", "this", "that", "these", "those", "they", "them", "he", "she", "yes",
    "first one", "second one", "third one", "the above", "previous", "earlier",
    "same", "that one", "explain more", "summarize that", "what about", "how about",
)


def _to_int(value, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _should_refine_query(query: str, history: List[Dict[str, str]]) -> bool:
    """Return True only for likely follow-up queries; skip standalone ones to save ~1-2s."""
    if not history:
        return False

    force_refine = str(get_config("rag_force_query_refine") or "").strip().lower()
    if force_refine in ("1", "true", "yes", "on"):
        return True

    q = (query or "").strip().lower()
    if not q:
        return False

    if any(marker in q for marker in _FOLLOW_UP_MARKERS):
        return True

    if q.startswith(("and ", "then ", "also ", "so ")):
        return True

    return False


def refine_query_with_history(query: str, history: List[Dict[str, str]]) -> str:
    """Rewrite a follow-up query into a standalone question using recent chat history.

    Returns the original query unchanged if no refinement is needed.
    """
    if not _should_refine_query(query, history):
        return query

    history_turns = max(1, _to_int(get_config("rag_refine_history_turns") or 2, 2))
    max_chars_per_msg = max(80, _to_int(get_config("rag_refine_message_chars") or 240, 240))
    recent_history = history[-history_turns:]

    if not recent_history:
        return query

    formatted_history = "\n".join(
        f"{msg.get('role', 'user')}: {(msg.get('content') or '')[:max_chars_per_msg]}"
        for msg in recent_history
    )

    prompt = (
        f"CHAT HISTORY:\n{formatted_history}\n\n"
        f"FOLLOW-UP QUESTION:\n{query}\n\n"
        f"REFINED STANDALONE QUESTION:"
    )

    try:
        refined = llm_invoke_cached(
            [("system", REFINER_PROMPT), ("user", prompt)],
            model="gpt-4o-mini",
            temperature=0.0,
            max_tokens=120,
        )
        logger.debug(f"Refined query: {refined}")
        return refined
    except Exception as e:
        frappe.log_error(f"Query refiner failed: {e}")
        return query
