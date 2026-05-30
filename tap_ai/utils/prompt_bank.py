"""Prompt bank service: load prompt suggestions and render system prompts."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import frappe

from tap_ai.models import ContentDetails, UserProfile

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "prompt_suggestions.json")
CACHE_KEY = "tap_ai:prompt_suggestions:v1"


def _load_from_disk() -> Dict[str, Any]:
    try:
        with open(SCHEMA_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def _load_from_doctype() -> Dict[str, Any]:
    try:
        if not frappe.db.exists("DocType", "Prompt Suggestion"):
            return {}

        rows = frappe.get_all(
            "Prompt Suggestion",
            filters={"is_active": 1},
            fields=["name", "identifier", "title", "description", "system_prompt", "is_default"],
            order_by="is_default desc, modified desc",
        )
        if not rows:
            return {}

        payload: Dict[str, Any] = {}
        default_row = None
        for row in rows:
            key = row.get("identifier") or row.get("name")
            payload[key] = {
                "id": row.get("identifier") or row.get("name"),
                "title": row.get("title"),
                "description": row.get("description"),
                "system_prompt": row.get("system_prompt") or "",
            }
            if row.get("is_default") and not default_row:
                default_row = payload[key]

        if default_row:
            payload["default"] = default_row
        elif rows:
            first_key = rows[0].get("identifier") or rows[0].get("name")
            payload["default"] = payload.get(first_key, {})

        return payload
    except Exception:
        return {}


def _get_all() -> Dict[str, Any]:
    try:
        cached = frappe.cache().get(CACHE_KEY)
        if cached:
            if isinstance(cached, bytes):
                cached = cached.decode("utf-8")
            return json.loads(cached)
    except Exception:
        pass

    data = _load_from_doctype() or _load_from_disk()
    try:
        frappe.cache().set(CACHE_KEY, json.dumps(data), ex=3600)
    except Exception:
        pass
    return data


def invalidate_prompt_cache() -> None:
    try:
        frappe.cache().delete(CACHE_KEY)
    except Exception:
        pass


def get_prompt_by_id(prompt_id: str = "default") -> Optional[Dict[str, Any]]:
    all_prompts = _get_all()
    # Accept either top-level keys or id field
    if prompt_id in all_prompts:
        return all_prompts[prompt_id]

    for _, val in all_prompts.items():
        if isinstance(val, dict) and val.get("id") == prompt_id:
            return val

    return None


def render_system_prompt(
    prompt_id: str = "default",
    student_name: Optional[str] = None,
    current_step: Optional[str] = None,
    topic_name: Optional[str] = None,
    class_name: Optional[str] = None,
    state: Optional[str] = None,
    language: Optional[str] = None,
) -> str:
    prompt = get_prompt_by_id(prompt_id) or get_prompt_by_id("default") or {}
    text = prompt.get("system_prompt", "")
    replacements = {
        "[STUDENT_NAME]": student_name or "Student",
        "[CURRENT_STEP]": current_step or "the activity",
        "[TOPIC_NAME]": topic_name or "the topic",
        "[CLASS]": class_name or "",
        "[STATE]": state or "",
        "[LANGUAGE]": language or "",
    }

    for token, val in replacements.items():
        text = text.replace(token, val)

    return text.strip()


def get_system_message_for_context(
    prompt_id: str = "default",
    user_profile: Optional[UserProfile] = None,
    content_details: Optional[ContentDetails] = None,
) -> str:
    student_name = None
    class_name = None
    state = None
    language = None
    current_step = None
    topic_name = None

    if user_profile:
        student_name = user_profile.name
        class_name = str(user_profile.grade or "")
        language = user_profile.language

    if content_details:
        topic_name = content_details.title
        current_step = content_details.current_step or content_details.step

    return render_system_prompt(
        prompt_id=prompt_id,
        student_name=student_name,
        current_step=current_step,
        topic_name=topic_name,
        class_name=class_name,
        state=state,
        language=language,
    )
