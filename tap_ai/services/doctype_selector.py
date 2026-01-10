# tap_ai/services/doctype_selector.py

import json
import logging
from typing import List, Dict, Any, Optional
from functools import lru_cache

import frappe
from langchain_openai import ChatOpenAI

from tap_ai.infra.config import get_config
from tap_ai.infra.sql_catalog import load_schema  

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a routing assistant.

Given:
- A natural language question about TAP AI data
- A JSON schema that lists DocTypes, their fields, and link relationships
- OPTIONAL user context (user type, grade)

Return ONLY a JSON object with:
{
  "doctypes": ["DocType A", "DocType B", ...],   // ordered by relevance
  "reason": "short explanation (<= 30 words)"
}

Rules:
- Choose the minimum set of DocTypes that can answer the query.
- Prefer DocTypes explicitly mentioning fields used in the question.
- If user context is provided, prefer DocTypes relevant to that user type.
- Use link relationships only if required to answer the query.
- Keep 'doctypes' length <= TOP_N.
- No prose outside JSON. No backticks.
"""


def _llm() -> Optional[ChatOpenAI]:
    api_key = get_config("openai_api_key")
    model = get_config("primary_llm_model") or "gpt-4o-mini"

    if not api_key:
        logger.error("OpenAI API key missing.")
        return None

    return ChatOpenAI(
        model_name=model,
        openai_api_key=api_key,
        temperature=0.0,
        max_tokens=400,
    )


def _schema_summary(schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compact schema summary to keep prompt small.
    """
    tables = schema.get("tables", {})
    links = schema.get("allowed_joins", []) or schema.get("links", [])

    compact_tables = {}
    for tname, tinfo in tables.items():
        cols = tinfo.get("columns") or []
        compact_tables[tname] = {
            "doctype": tinfo.get("doctype") or tname.replace("tab", "", 1),
            "fields": cols[:25],
            "description": (tinfo.get("description") or "")[:160],
        }

    return {
        "tables": compact_tables,
        "links": links[:25],
    }


@lru_cache(maxsize=256)
def pick_doctypes(
    query: str,
    top_n: int = 5,
    user_profile_json: Optional[str] = None,
) -> List[str]:
    """
    Pick the most relevant DocTypes for a query.

    NOTE:
    - user_profile_json is a JSON string for cache safety
    - Pass None if no user context
    """
    query = (query or "").strip().lower()
    schema = load_schema()
    summary = _schema_summary(schema)
    llm = _llm()

    if not llm:
        return []

    # Decode user profile (optional)
    user_profile = None
    if user_profile_json:
        try:
            user_profile = json.loads(user_profile_json)
        except Exception:
            user_profile = None

    schema_snippet = json.dumps(summary, ensure_ascii=False)

    # --- Optional user context hints ---
    user_context = ""
    if user_profile:
        if user_profile.get("type"):
            user_context += f"USER TYPE: {user_profile['type']}\n"
        if user_profile.get("grade"):
            user_context += f"GRADE: {user_profile['grade']}\n"

    user_msg = (
        f"TOP_N={top_n}\n\n"
        f"{user_context}\n"
        f"QUESTION:\n{query}\n\n"
        f"SCHEMA SUMMARY:\n{schema_snippet}"
    )

    try:
        resp = llm.invoke(
            [
                ("system", SYSTEM_PROMPT),
                ("user", user_msg),
            ]
        )
        txt = resp.content.strip()
        data = json.loads(txt)
        doctypes = data.get("doctypes", [])

        return _normalize_doctypes(doctypes, summary)[:top_n]

    except Exception as e:
        logger.warning("DocType selection LLM failed: %s", e)
        return []


def _normalize_doctypes(
    candidates: List[str],
    summary: Dict[str, Any]
) -> List[str]:
    """
    Normalize LLM-proposed names to canonical DocType names in schema.
    """
    schema_names = {}
    for table_name, info in summary["tables"].items():
        clean = info["doctype"]
        schema_names[clean.lower()] = clean

    normalized = []
    for name in candidates:
        key = name.lower().replace("tab", "").strip()
        if key in schema_names:
            normalized.append(schema_names[key])

    # Deduplicate while preserving order
    seen = set()
    final = []
    for d in normalized:
        if d not in seen:
            final.append(d)
            seen.add(d)

    return final
