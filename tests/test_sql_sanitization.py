"""
Unit tests for SQL sanitization in tap_ai/services/sql_answerer.py

These tests verify the backtick-to-double-quote safety net without needing
a database connection or LLM. The LLM sometimes generates MySQL/MariaDB
syntax (backticks) even though our remote database is PostgreSQL.

Run with: pytest tests/test_sql_sanitization.py -v
"""

import re
import pytest


def _sanitize_sql(sql: str) -> str:
    """Mirror of the sanitization logic in sql_answerer._generate_sql_query."""
    sql = sql.replace("```sql", "").replace("```", "").strip()
    sql = re.sub(r"`([^`]+)`", r'"\1"', sql)
    if "LIMIT" not in sql.upper():
        sql += " LIMIT 20"
    return sql


class TestBacktickSanitization:
    def test_backtick_table_name_converted(self):
        sql = "SELECT name FROM `tabAssignment` a LIMIT 10"
        result = _sanitize_sql(sql)
        assert "`" not in result
        assert '"tabAssignment"' in result

    def test_multiple_backtick_identifiers(self):
        sql = "SELECT a.name FROM `tabAssignment` a JOIN `tabStudent` s ON a.student = s.name LIMIT 10"
        result = _sanitize_sql(sql)
        assert "`" not in result
        assert '"tabAssignment"' in result
        assert '"tabStudent"' in result

    def test_already_double_quoted_unchanged(self):
        sql = 'SELECT name FROM "tabAssignment" a LIMIT 10'
        result = _sanitize_sql(sql)
        assert result == sql

    def test_code_fence_stripped(self):
        sql = "```sql\nSELECT name FROM \"tabAssignment\" LIMIT 5\n```"
        result = _sanitize_sql(sql)
        assert "```" not in result
        assert "SELECT" in result

    def test_limit_added_when_missing(self):
        sql = 'SELECT name FROM "tabAssignment"'
        result = _sanitize_sql(sql)
        assert "LIMIT 20" in result

    def test_limit_not_duplicated_when_present(self):
        sql = 'SELECT name FROM "tabAssignment" LIMIT 5'
        result = _sanitize_sql(sql)
        assert result.upper().count("LIMIT") == 1


class TestTextNormalization:
    """
    Tests for the KB text normalization used in direct_response_bank.py.
    Normalization underpins all exact-match KB lookups — a bug here means
    greetings stop matching silently.
    """

    def _normalize(self, value):
        import unicodedata
        value = str(value).strip().lower()
        value = unicodedata.normalize("NFKD", value)
        value = value.encode("ascii", "ignore").decode("ascii")
        value = re.sub(r"[^a-z0-9\s]", " ", value)
        value = re.sub(r"\s+", " ", value).strip()
        return value

    def test_basic_lowercase(self):
        assert self._normalize("Hello") == "hello"

    def test_strips_punctuation(self):
        assert self._normalize("hello!") == "hello"
        assert self._normalize("hi...") == "hi"

    def test_unicode_normalized(self):
        assert self._normalize("héllo") == "hello"
        assert self._normalize("namasté") == "namaste"

    def test_extra_spaces_collapsed(self):
        assert self._normalize("good   morning") == "good morning"

    def test_empty_string(self):
        assert self._normalize("") == ""

    def test_emoji_stripped(self):
        assert self._normalize("hello 👋") == "hello"
