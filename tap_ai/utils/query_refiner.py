"""Query refinement utility — rewrites follow-up questions into standalone queries.

Uses recent chat history to resolve pronouns and contextual references so that
all downstream engines (router, RAG, SQL) receive a self-contained question.

Greetings and unconditional KB intents should bypass this entirely — their
meaning is fixed regardless of conversation history.
"""

from __future__ import annotations

from typing import Dict, List

import frappe
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from loguru import logger

from tap_ai.infra.config import get_config
from tap_ai.infra.llm_client import LLMClient


_REFINER_SYSTEM = """You are a context resolver. Given a chat history and a follow-up message, return a refined version that stands alone — someone should understand it without seeing the conversation.

Rules:
1. Resolve ambiguous references (pronouns like "it", "this", "that"; ordinals like "the first one"; phrases like "the previous topic") by substituting the specific entity from history.
2. Preserve the message type — do NOT convert a statement into a question:
   - QUESTION or information request → rewrite as a complete standalone question.
   - STATEMENT, reaction, or acknowledgement (e.g. "thanks for the video", "that was great", "got it") → keep as a statement, only filling in missing references (e.g. "thanks for the video" → "thanks for the video about [topic from history]").
3. If the message is already self-contained with no ambiguous references, return it unchanged.
4. Quiz shortcut: if the last assistant message contained a quiz with labelled options (A), B), C)…) and the current message is a single letter or matching option text, expand to: "Is [option text] the correct answer to the quiz: [question text]?"
5. Never answer the message. Never invent context not present in history.

Return ONLY the refined message. No explanation, no prefix.
"""

_REFINE_PROMPT = ChatPromptTemplate.from_messages([
    ("system", _REFINER_SYSTEM),
    ("human", "CHAT HISTORY:\n{history}\n\nFOLLOW-UP MESSAGE:\n{query}\n\nREFINED STANDALONE MESSAGE:"),
])

def _to_int(value, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _should_refine_query(query: str, history: List[Dict[str, str]]) -> bool:
    """Return True whenever there is conversation history to draw context from."""
    return bool(history) and bool((query or "").strip())


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

    try:
        chain = _REFINE_PROMPT | LLMClient.get_client(
            model="gpt-4o-mini", temperature=0.0, max_tokens=120
        ) | StrOutputParser()
        refined = chain.invoke({"history": formatted_history, "query": query})
        logger.debug(f"Refined query: {refined}")
        return refined
    except Exception as e:
        frappe.log_error(f"Query refiner failed: {e}")
        return query
