# tap_ai/services/routing_patterns.py
"""
Fast routing pattern definitions for TAP AI Router.

These regex patterns enable zero-LLM routing for common conversational intents,
reducing latency and API costs for text_to_sql queries and for the greeting/identity
fast-path.

Patterns are organized by intent category and updated from TAP Response Knowledge
doctype to ensure alignment with curated responses.

ROUTING FLOW:
=============

1. FAST PATTERN MATCHING (Regex - Zero LLM Cost)
   └─ query → match_fast_kb_unconditional() or match_fast_sql()
      ├─ Greeting/identity match: → vector_search tool, refinement skipped
      │  (TAP Response Knowledge is indexed in pgvector, so vector_search
      │   resolves these directly via cosine-similarity namespace routing)
      ├─ SQL Match: → text_to_sql tool
      └─ No Match: → LLM-based routing (defaults to vector_search)
"""

import re

# ======================================================
# SQL INTENT PATTERNS
# ======================================================

FAST_SQL_PATTERNS = re.compile(
    r"\b(list|count|how many|show me all|filter|total|stats)\b",
    re.I
)

# ======================================================
# GREETING / IDENTITY FAST-PATH PATTERNS
# ======================================================

# Unconditional KB pattern — used by llm_worker.py on the RAW query BEFORE refinement.
# Only includes intents whose meaning is fixed regardless of conversation history:
# greetings, goodbyes, identity, and gratitude.
#
# Context-dependent words (yes/ok/done/continue) are intentionally excluded so that
# e.g. "yes" after a RAG response about TAP activities is correctly refined to the
# actual follow-up intent before routing, instead of getting a generic KB reply.
FAST_KB_UNCONDITIONAL_PATTERNS = re.compile(
    r"""
    # Greetings & Sign-offs
    ^(?:
        h(?:i+|e+y+)|hello+|hey+|sup|yo|
        good\s*(?:morning|afternoon|evening|night)|gm|gn|
        namaste|shubh\s*prabhat|
        bye+|goodbye|ta+ta|tata|
        thank(?:\s*you)?|thanks|shukriya|
        welcome|howdy
    )\b
    |
    # Identity / Who are you
    (?:who\s+are\s+you|aap\s+kaun\s+ho|tap\s+buddy|mera\s+naam)
    """,
    re.I | re.X | re.UNICODE,
)


import re as _re

# Letter options: A) B) C) (with DOTALL so newlines between options are fine)
_QUIZ_LETTER_OPTIONS_RE = _re.compile(r'\bA\).*\bB\).*\bC\)', _re.S)
# Numbered options: "1. ..." "2. ..." "3. ..." or "1) ..." "2) ..." "3) ..."
_QUIZ_NUMBER_OPTIONS_RE = _re.compile(r'\b1[.)]\s*.+\b2[.)]\s*.+\b3[.)]', _re.S)

# Bare letter answer: "A", "b", "C.", "d)"
_QUIZ_BARE_LETTER_RE = _re.compile(r'^\s*[A-Da-d]\s*[).:,]?\s*$')
# Bare digit answer: "1", "2", "3", "4"
_QUIZ_BARE_DIGIT_RE = _re.compile(r'^\s*[1-4]\s*[).:,]?\s*$')
# "Option A" or "Option 1" style
_QUIZ_OPTION_PREFIX_RE = _re.compile(r'^[Oo]ption\s+[A-Da-d1-4]\b')


def is_quiz_context(history: list) -> bool:
    """True if the most recent assistant message contained quiz options (letter or numbered)."""
    if not history:
        return False
    last = next((m for m in reversed(history) if m.get("role") == "assistant"), None)
    if not last:
        return False
    content = last.get("content", "")
    return bool(
        _QUIZ_LETTER_OPTIONS_RE.search(content) or
        _QUIZ_NUMBER_OPTIONS_RE.search(content)
    )


def is_quiz_answer(query: str) -> bool:
    """True if the query looks like a quiz answer (bare letter, bare digit, or 'Option X')."""
    q = (query or "").strip()
    return bool(
        _QUIZ_BARE_LETTER_RE.match(q) or
        _QUIZ_BARE_DIGIT_RE.match(q) or
        _QUIZ_OPTION_PREFIX_RE.match(q)
    )


def match_fast_kb_unconditional(query: str) -> bool:
    """Check if query is an unconditional KB intent (safe to skip refinement).

    Only matches salutation-type queries whose meaning is fixed regardless of
    conversation history (greetings, goodbyes, identity, gratitude).
    Use this on the raw query before refinement runs.
    """
    q = (query or "").strip()
    return bool(FAST_KB_UNCONDITIONAL_PATTERNS.match(q)) if q else False


def match_fast_sql(query: str) -> bool:
    """Check if query matches text_to_sql fast patterns."""
    q = (query or "").strip()
    return bool(FAST_SQL_PATTERNS.search(q)) if q else False
