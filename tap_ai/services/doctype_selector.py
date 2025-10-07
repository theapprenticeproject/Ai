# tap_ai/services/doctype_selector.py

import json
import logging
from typing import List, Dict, Any, Optional
from functools import lru_cache

logger = logging.getLogger(__name__)

import frappe
from langchain_openai import ChatOpenAI

from tap_ai.infra.config import get_config
from tap_ai.infra.sql_catalog import load_schema  

SYSTEM_PROMPT = """You are a routing assistant. 
Given:
- A natural language question about TAP ai data
- A JSON schema that lists DocTypes, their fields, and link relationships

Return ONLY a JSON object with:
{
  "doctypes": ["DocType A", "DocType B", ...],   // ordered by relevance
  "reason": "short explanation (<= 30 words)"
}

Rules:
- Choose the minimum set of DocTypes that can answer the query.
- Prefer DocTypes explicitly mentioning fields used in the question.
- If multiple similar doctypes exist (e.g., Student vs Backend Students), prioritize the one that most closely matches the entire phrase used in the query (e.g., "backend students" → Backend Students).
- If no explicit match exists, pick the semantically closest.
- Use link relationships to include supporting DocTypes only if needed to answer the query.
- Keep 'doctypes' length <= TOP_N (the tool will tell you).
- No prose outside JSON. No backticks.
"""

def _llm() -> Optional[ChatOpenAI]:
    api_key = get_config("openai_api_key")
    model = "gpt-3.5-turbo" or get_config("primary_llm_model")
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
    """Compact the schema to essentials to keep prompt small."""
    # Expect schema like: {"tables": { "<table>": {"description": "...", "columns": [...]}}, "links": [...]}
    tables = schema.get("tables", {})
    links = schema.get("allowed_joins", []) or schema.get("links", [])
    # Take only names + field lists (clip to first ~25 to keep prompt small)
    compact_tables = {}
    for tname, tinfo in tables.items():
        cols = tinfo.get("columns") or tinfo.get("fields") or []
        compact_tables[tname] = {
            "doctype": tinfo.get("doctype") or tname.replace("tab", "", 1),
            "fields": cols[:25],
            "description": tinfo.get("description", "")[:160]
        }
    return {"tables": compact_tables, "links": links}

@lru_cache(maxsize=256)
def pick_doctypes(query: str, top_n: int = 5) -> List[str]:
    """
    Use LLM + tap_ai_schema.json to pick the best DocTypes for this query.
    Falls back to a lightweight heuristic if the LLM output isn't valid JSON.
    """
    query = (query or "").strip().lower()
    schema = load_schema()
    summary = _schema_summary(schema)
    llm = _llm()

    # Build a small, cached prompt to avoid recomputing
    # You can cache the summary string if you like:
    schema_snippet = json.dumps(summary, ensure_ascii=False)

    user_msg = (
        f"TOP_N={top_n}\n\n"
        f"QUESTION:\n{query}\n\n"
        f"SCHEMA SUMMARY (DocTypes with fields & links):\n{schema_snippet}"
    )

    if not llm:
        logger.warning("LLM not available; using heuristic fallback.")
        return _fallback_doctypes(query, summary, top_n)

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
        # Clean up "tabX" / bare names and dedupe
        doctypes = _normalize_doctypes(doctypes, summary)
        return doctypes[:top_n] if doctypes else _fallback_doctypes(query, summary, top_n)
    except Exception as e:
        logger.warning("DocType selection LLM failed: %s", e)
        return _fallback_doctypes(query, summary, top_n)

def _normalize_doctypes(candidates: List[str], summary: Dict[str, Any]) -> List[str]:
    """Map user/LLM-proposed names to canonical DocType names found in schema."""
    schema_names = set()
    map_lower: Dict[str, str] = {}
    for k in summary["tables"].keys():
        # schema uses either "tabX" or clean names in your loader; handle both
        clean = k.replace("tab", "", 1) if k.startswith("tab") else k
        schema_names.add(clean)
        map_lower[clean.lower()] = clean
    normalized = []
    for name in candidates:
        nl = name.lower().replace("tab", "", 1).strip()
        if nl in map_lower:
            normalized.append(map_lower[nl])
    # dedupe preserve order
    seen = set()
    out = []
    for n in normalized:
        if n not in seen:
            out.append(n)
            seen.add(n)
    return out

def _fallback_doctypes(query: str, summary: Dict[str, Any], top_n: int) -> List[str]:
    """
    Simple heuristic:
    - score tables by keyword overlap on field names + description
    - prefer a few obviously relevant doctypes
    """
    ql = query.lower()
    scored: List[tuple] = []
    for tname, tinfo in summary["tables"].items():
        clean = tname.replace("tab", "", 1)
        score = 0
        desc = (tinfo.get("description") or "").lower()
        if "student" in ql and "student" in clean.lower():
            score += 5
        if "school" in ql and "school" in clean.lower():
            score += 5
        if "activity" in ql and "activity" in clean.lower():
            score += 5
        # field keyword overlap
        for f in (tinfo.get("fields") or []):
            fl = (str(f) or "").lower()
            if fl and any(tok in ql for tok in fl.split("_")):
                score += 1
        if desc and any(w in desc for w in ql.split()):
            score += 1
        if score:
            scored.append((score, clean))
    scored.sort(reverse=True)
    return [name for _, name in scored[:top_n]] or list(summary["tables"].keys())[:top_n]
