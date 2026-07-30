"""Data access for TAP Response Knowledge — backs the pgvector KB index.

Routing no longer calls into this module directly (TAP Response Knowledge is
searched via vector_search / pgvector, same as any other DocType namespace).
This module remains the source of truth for reading/caching KB entries and is
used by pgvector_store.py to build embeddings and by the doc_events hooks to
keep the pgvector index and cache in sync.
"""

from __future__ import annotations

import json
import re
import unicodedata
from typing import Any, Dict, List, Optional

import frappe
from loguru import logger


KB_DOCTYPE = "TAP Response Knowledge"
KB_CACHE_KEY = "tap_ai:direct_response_knowledge:v1"
KB_CACHE_TTL = 3600


def normalize_text(value: Optional[str]) -> str:
	"""Normalize a query for exact and fuzzy matching."""
	if not value:
		return ""

	value = str(value).strip().lower()
	value = unicodedata.normalize("NFKD", value)
	value = value.encode("ascii", "ignore").decode("ascii")
	value = re.sub(r"[^a-z0-9\s]", " ", value)
	value = re.sub(r"\s+", " ", value).strip()
	return value


def _parse_aliases(raw_value: Any) -> List[str]:
	if not raw_value:
		return []

	if isinstance(raw_value, list):
		items = raw_value
	elif isinstance(raw_value, tuple):
		items = list(raw_value)
	else:
		text = str(raw_value).strip()
		if not text:
			return []
		if text.startswith("["):
			try:
				parsed = json.loads(text)
				if isinstance(parsed, list):
					items = parsed
				else:
					items = [text]
			except Exception:
				items = re.split(r"[\n,]", text)
		else:
			items = re.split(r"[\n,]", text)

	aliases: List[str] = []
	for item in items:
		alias = str(item).strip()
		if alias and alias not in aliases:
			aliases.append(alias)
	return aliases


def _load_entries_from_cache() -> Optional[List[Dict[str, Any]]]:
	try:
		cached = frappe.cache().get(KB_CACHE_KEY)
		if not cached:
			return None
		if isinstance(cached, bytes):
			cached = cached.decode("utf-8", errors="ignore")
		entries = json.loads(cached)
		if isinstance(entries, list):
			return entries
	except Exception:
		return None
	return None


def _store_entries_in_cache(entries: List[Dict[str, Any]]) -> None:
	try:
		frappe.cache().set(KB_CACHE_KEY, json.dumps(entries, default=str), ex=KB_CACHE_TTL)
	except Exception as e:
		logger.warning(f"KB cache write failed — responses will not be cached: {e}")


def get_direct_response_entries(force_refresh: bool = False) -> List[Dict[str, Any]]:
	if not force_refresh:
		cached = _load_entries_from_cache()
		if cached is not None:
			return cached

	try:
		entries = frappe.get_all(
			KB_DOCTYPE,
			fields=[
				"name",
				"title",
				"category",
				"subcategory",
				"student_query",
				"normalized_query",
				"alternate_queries",
				"response",
				# priority removed from selection logic; no longer requested
				"language",
				"user_type",
				"response_tone",
				"notes",
				"is_active",
			],
			filters={"is_active": 1},
			order_by="modified desc",
		)
		entries = entries or []
		_store_entries_in_cache(entries)
		return entries
	except Exception as e:
		frappe.log_error(f"Direct response knowledge load failed: {e}", "tap_ai.services.direct_response_bank")
		return []


def invalidate_kb_cache() -> bool:
	"""Invalidate the in-memory/cache representation of the direct response knowledge.

	This is intended to be called from DocType event hooks when KB entries change.
	"""
	try:
		frappe.cache().delete(KB_CACHE_KEY)
		logger.info("Direct response KB cache invalidated")
		return True
	except Exception as e:
		frappe.log_error(f"Failed to invalidate KB cache: {e}", "tap_ai.services.direct_response_bank")
		return False



def get_entries_for_category(category: str, force_refresh: bool = False) -> List[Dict[str, Any]]:
	"""Return KB entries for a specific category.

	This returns a list of objects with stable fields used by the selection LLM:
	- id (mapped from `name`)
	- student_query
	- alternate_queries
	- response
	- title
	- subcategory
	- is_active
	"""
	entries = get_direct_response_entries(force_refresh=force_refresh)
	if not entries:
		return []
	filtered: List[Dict[str, Any]] = []
	for e in entries:
		if not e or not e.get("is_active", 1):
			continue
		if (e.get("category") or "").strip().lower() != (category or "").strip().lower():
			continue
		filtered.append({
			"id": e.get("name"),
			"title": e.get("title"),
			"student_query": e.get("student_query"),
			"alternate_queries": e.get("alternate_queries"),
			"response": e.get("response"),
			"subcategory": e.get("subcategory"),
			"is_active": e.get("is_active", 1),
		})
	return filtered
