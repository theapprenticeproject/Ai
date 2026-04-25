"""
Direct conversational answerer for greetings, small talk, and motivational guidance.
"""

import time
from typing import Dict, Any, List, Optional

import frappe
from langchain_openai import ChatOpenAI

from tap_ai.infra.config import get_config


def _llm(model: str = "gpt-4o-mini", temperature: float = 0.4) -> ChatOpenAI:
    from tap_ai.infra.llm_client import LLMClient

    return LLMClient.get_client(
        model=model,
        temperature=temperature,
        max_tokens=500,
    )


DIRECT_CHAT_SYSTEM_PROMPT = """You are a warm, supportive educational assistant for students.

Use this mode for:
- greetings and small talk (hello, good morning, how are you)
- encouragement and study motivation
- light guidance when a student feels stuck or demotivated

Style rules:
1. Keep replies concise, empathetic, and practical
2. Use simple, age-appropriate language
3. Give 2-4 concrete next steps for guidance requests
4. Avoid making up institutional data or database facts
5. If user asks for structured platform data, suggest asking a specific content/data question

Safety:
- If the message suggests self-harm, abuse, or immediate danger, respond with empathy,
  encourage contacting a trusted adult/counselor immediately, and local emergency services.
"""


def answer_direct(
    query: str,
    user_profile: Optional[Dict[str, Any]] = None,
    chat_history: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """Generate a direct conversational response without SQL or RAG retrieval."""
    start = time.perf_counter()
    llm = _llm(model=get_config("primary_llm_model") or "gpt-4o-mini")
    chat_history = chat_history or []

    user_name = ""
    if user_profile and user_profile.get("name"):
        user_name = str(user_profile["name"])

    personalization = (
        f"Student name: {user_name}.\n"
        f"Grade: {user_profile.get('grade', 'N/A') if user_profile else 'N/A'}.\n"
        if user_name
        else ""
    )

    messages = [
        ("system", DIRECT_CHAT_SYSTEM_PROMPT),
        ("system", personalization) if personalization else ("system", ""),
    ]

    # Keep only recent conversational context.
    for msg in chat_history[-4:]:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if role in ("user", "assistant") and content:
            messages.append((role, content))

    messages.append(("user", query))

    try:
        resp = llm.invoke(messages)
        answer = getattr(resp, "content", "").strip() or "I am here with you. Tell me what feels hardest right now, and we can break it into small steps."
        timing_ms = int((time.perf_counter() - start) * 1000)
        return {
            "question": query,
            "answer": answer,
            "response_type": "direct_llm",
            "user_context": "personalized" if user_profile else "general",
            "metadata": {
                "timings_ms": {
                    "direct_llm": timing_ms,
                    "total": timing_ms,
                }
            },
        }
    except Exception as e:
        timing_ms = int((time.perf_counter() - start) * 1000)
        frappe.log_error(f"Direct answer generation failed: {e}", "tap_ai.services.direct_answerer")
        return {
            "question": query,
            "answer": "I am here to help. Would you like a quick study plan for the next 15 minutes?",
            "response_type": "direct_llm",
            "error": str(e),
            "metadata": {
                "timings_ms": {
                    "direct_llm": timing_ms,
                    "total": timing_ms,
                }
            },
        }
