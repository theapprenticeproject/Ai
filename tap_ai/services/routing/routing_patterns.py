# tap_ai/services/routing_patterns.py
"""
Fast routing pattern definitions for TAP AI Router.

These regex patterns enable zero-LLM routing for common conversational intents,
reducing latency and API costs for knowledge_bank and text_to_sql queries.

Patterns are organized by intent category and updated from TAP Response Knowledge
doctype to ensure alignment with curated responses.

ROUTING FLOW:
=============

1. FAST PATTERN MATCHING (Regex - Zero LLM Cost)
   └─ query → match_fast_kb() or match_fast_sql()
      ├─ KB Match: → knowledge_bank tool
      ├─ SQL Match: → text_to_sql tool
      └─ No Match: → LLM-based routing

2. KNOWLEDGE BANK EXECUTION (Two Cases)
   ├─ CASE 1: REGEX MATCH + EXACT KB MATCH
   │  └─ lookup_exact_direct_response()
   │     ├─ Checks ALL candidates: student_query + alternate_queries
   │     └─ Returns KB response if normalized query matches (FAST PATH)
   │
   └─ CASE 2: REGEX MATCH BUT NO EXACT KB MATCH
      └─ verify_kb_and_respond()
         ├─ Loads ALL active KB entries with match_queries
         ├─ Passes full KB context to LLM
         ├─ LLM decides: Match from KB OR Generate answer
         └─ Returns KB response (if matched) or LLM-generated answer

BENEFITS:
- Case 1: ~50ms (no LLM, fast cache lookup)
- Case 2: LLM fallback ensures no query is left unhandled
- Alternate queries ensure comprehensive matching
"""

import re

# ======================================================
# KNOWLEDGE BANK CONTENT GUARD
# ======================================================

# Queries containing these words likely ask for content/data even if they
# superficially match KB routing patterns (e.g. "what is a video?").
# The worker uses this to prevent KB mis-routing for content queries.
KB_CONTENT_WORDS = (
    "activity", "video", "topic", "course", "explain",
    "what is", "lesson", "quiz", "assignment",
)

# ======================================================
# SQL INTENT PATTERNS
# ======================================================

FAST_SQL_PATTERNS = re.compile(
    r"\b(list|count|how many|show me all|filter|total|stats)\b",
    re.I
)

# ======================================================
# KNOWLEDGE BANK INTENT PATTERNS
# ======================================================

# Full KB pattern set — used by router.py on the REFINED query.
# Includes context-dependent words (yes/ok/done) which are safe to match
# on a refined query because refinement has already resolved their meaning.
FAST_KB_PATTERNS = re.compile(
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
    |
    # Help / Stuck / Problems
    (?:
        problem|madad|help|stuck|no\s+ideas?|
        kya\s+likhun|kuch\s+samajh\s+nahi|
        i\s+(?:don't\s+)?know|
        please\s+(?:talk|listen)|
        i'm\s+(?:bored|stuck|confused)|
        bore\s+ho\s+(?:raha|rahi)
    )
    |
    # Submission / Done
    (?:
        \bdone\b|kar\s*diya|submit|ho\s*gaya|
        i\s+(?:have\s+)?done|already\s+did|
        finished|complete\s*d
    )
    |
    # Excitement / Positive
    (?:
        awesome|amazing|great|cool|fun|
        i\s+(?:love|like)\s+(?:this|tap|buddy|it)|
        maza\s+aa\s*(?:gaya|raha)|
        impressed|wow|woah|nice
    )
    |
    # Promotion / Achievement
    (?:
        pass\s+ho\s+gaya|promoted|new\s+class|
        badhai\s+(?:do|de)|
        congratulations|celebrate
    )
    |
    # Continue / Ready  (context-dependent — only safe after refinement)
    # Require these to be the ENTIRE query so "yes, teach me X" doesn't mis-route.
    (?:
        \b(?:yes|ready|continue|chalo|haan|ok)\s*[!?.,]*\s*$
    )
    |
    # Very short / unclear  (context-dependent — only safe after refinement)
    # Require these to be the ENTIRE query so "yep, teach me X" doesn't mis-route.
    (?:
        \b(?:k|ok|acha|thik\s+hai|hmm|sure|yep|yup)\s*[!?.,]*\s*$
    )
    """,
    re.I | re.X | re.UNICODE,
)

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


def match_fast_kb(query: str) -> bool:
    """Check if query matches the full knowledge_bank fast patterns (use on refined query)."""
    q = (query or "").strip()
    return bool(FAST_KB_PATTERNS.match(q)) if q else False


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
