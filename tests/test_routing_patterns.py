"""
Unit tests for tap_ai/services/routing_patterns.py

These tests are pure Python — no Frappe, no DB, no LLM needed.
Run with: pytest tests/test_routing_patterns.py -v

What we're protecting against:
- A regex edit accidentally breaking greeting detection (routing breaks for every user)
- Context-dependent words (yes/ok/done) leaking into the unconditional fast-path
  and getting generic KB replies instead of context-aware responses
- SQL intent words getting misclassified
"""

import pytest
from tap_ai.services.routing_patterns import (
    match_fast_kb,
    match_fast_kb_unconditional,
    match_fast_sql,
    KB_CONTENT_WORDS,
)


# ======================================================
# match_fast_kb_unconditional — greetings / sign-offs / identity
# These must ALWAYS route to KB regardless of conversation history.
# ======================================================

class TestUnconditionalKB:
    def test_basic_greetings(self):
        for q in ["hi", "hello", "hey", "Hi", "HELLO", "heyy", "hiii"]:
            assert match_fast_kb_unconditional(q), f"Expected unconditional KB for {q!r}"

    def test_time_greetings(self):
        for q in ["good morning", "good afternoon", "good evening", "good night", "gm", "gn"]:
            assert match_fast_kb_unconditional(q), f"Expected unconditional KB for {q!r}"

    def test_hinglish_greetings(self):
        for q in ["namaste", "shubh prabhat"]:
            assert match_fast_kb_unconditional(q), f"Expected unconditional KB for {q!r}"

    def test_sign_offs(self):
        for q in ["bye", "byeee", "goodbye", "tata"]:
            assert match_fast_kb_unconditional(q), f"Expected unconditional KB for {q!r}"

    def test_gratitude(self):
        for q in ["thanks", "thank you", "shukriya"]:
            assert match_fast_kb_unconditional(q), f"Expected unconditional KB for {q!r}"

    def test_identity_questions(self):
        for q in ["who are you", "tap buddy"]:
            assert match_fast_kb_unconditional(q), f"Expected unconditional KB for {q!r}"

    # Context-dependent words must NOT be in the unconditional set.
    # "yes" after a RAG response about a TAP activity should be refined
    # to the actual follow-up intent, not get a generic KB reply.
    def test_contextual_words_excluded(self):
        for q in ["yes", "ok", "sure", "done", "continue", "ready", "k", "yep", "yup", "haan"]:
            assert not match_fast_kb_unconditional(q), (
                f"{q!r} is context-dependent and must NOT be in the unconditional set"
            )

    def test_content_queries_not_matched(self):
        for q in ["what is a video", "list all activities", "explain this topic"]:
            assert not match_fast_kb_unconditional(q), f"Content query should not match: {q!r}"


# ======================================================
# match_fast_kb — full set (used on refined queries)
# Includes context-dependent words because by the time this runs
# the query has already been refined to its standalone intent.
# ======================================================

class TestFullKB:
    def test_includes_unconditional(self):
        for q in ["hi", "hello", "bye", "thanks", "who are you"]:
            assert match_fast_kb(q), f"Full KB should include greeting {q!r}"

    def test_includes_contextual_words(self):
        for q in ["yes", "ok", "sure", "done", "continue", "ready", "k", "yep"]:
            assert match_fast_kb(q), f"Full KB should include contextual word {q!r}"

    def test_submission_phrases(self):
        for q in ["done", "finished", "submitted", "kar diya", "ho gaya"]:
            assert match_fast_kb(q), f"Expected KB for submission phrase {q!r}"

    def test_excitement(self):
        for q in ["awesome", "amazing", "wow", "nice", "great"]:
            assert match_fast_kb(q), f"Expected KB for excitement phrase {q!r}"


# ======================================================
# match_fast_sql — data lookup intent
# ======================================================

class TestFastSQL:
    def test_list_queries(self):
        for q in ["list all videos", "list the activities", "List all students"]:
            assert match_fast_sql(q), f"Expected SQL intent for {q!r}"

    def test_count_queries(self):
        for q in ["how many students", "count all courses", "total assignments"]:
            assert match_fast_sql(q), f"Expected SQL intent for {q!r}"

    def test_show_queries(self):
        for q in ["show me all courses", "show me all videos"]:
            assert match_fast_sql(q), f"Expected SQL intent for {q!r}"

    def test_non_sql_not_matched(self):
        for q in ["hello", "who are you", "what is financial literacy"]:
            assert not match_fast_sql(q), f"Should not match SQL intent: {q!r}"


# ======================================================
# KB_CONTENT_WORDS — guard against KB mis-routing content queries
# ======================================================

class TestKBContentWords:
    def test_content_words_present(self):
        for word in ("activity", "video", "topic", "course", "lesson", "quiz", "assignment"):
            assert word in KB_CONTENT_WORDS, f"{word!r} must be in KB_CONTENT_WORDS"

    def test_guard_triggers_correctly(self):
        query = "what is a video"
        assert any(w in query.lower() for w in KB_CONTENT_WORDS), \
            "KB guard should trigger for content query"

    def test_guard_does_not_trigger_for_greetings(self):
        for q in ["hi", "hello", "who are you", "thanks"]:
            assert not any(w in q.lower() for w in KB_CONTENT_WORDS), \
                f"KB guard should NOT trigger for greeting {q!r}"
