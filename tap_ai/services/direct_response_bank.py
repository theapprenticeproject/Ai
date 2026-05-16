"""Knowledge-bank lookup for direct TAP responses."""

from __future__ import annotations

import json
import re
import time
import unicodedata
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Tuple

import frappe


KB_DOCTYPE = "TAP Response Knowledge"
KB_CACHE_KEY = "tap_ai:direct_response_knowledge:v1"
KB_CACHE_TTL = 3600
KB_BASE_THRESHOLD = 0.82
KB_SHORT_QUERY_THRESHOLD = 0.88
KB_MEDIUM_QUERY_THRESHOLD = 0.84
KB_AMBIGUITY_GAP = 0.05


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


def _entry_candidates(entry: Dict[str, Any]) -> List[str]:
	"""
	Extract ALL candidate phrases from a KB entry.
	
	This ensures we check BOTH student_query and alternate_queries to maximize
	matching coverage in lookup_exact_direct_response().
	
	Process:
	1. Collect primary phrase: student_query
	2. Collect secondary phrase: normalized_query (if different)
	3. Parse alternate_queries string/list into individual items
	   └─ alternate_queries can be newline-separated, comma-separated, or a list
	   └─ _parse_aliases() handles all formats
	4. Return deduplicated list of all candidates
	
	Example:
	  entry = {
	    "student_query": "Hi",
	    "alternate_queries": "Hey\nHello\nHii",
	  }
	  
	  Returns: ["Hi", "Hey", "Hello", "Hii"]
	  
	  Then lookup_exact_direct_response() normalizes each and checks for matches.
	"""
	candidates = []
	for value in (entry.get("student_query"), entry.get("normalized_query")):
		text = str(value or "").strip()
		if text and text not in candidates:
			candidates.append(text)

	for alias in _parse_aliases(entry.get("alternate_queries")):
		if alias not in candidates:
			candidates.append(alias)

	return candidates


def _token_overlap(left: str, right: str) -> float:
	left_tokens = set(left.split())
	right_tokens = set(right.split())
	if not left_tokens or not right_tokens:
		return 0.0
	return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _raw_similarity(left: str, right: str) -> float:
	if not left or not right:
		return 0.0
	if left == right:
		return 1.0
	return SequenceMatcher(None, left, right).ratio()


def _score_candidate(query: str, candidate: str) -> float:
	query_raw = str(query or "").strip().lower()
	candidate_raw = str(candidate or "").strip().lower()
	if not query_raw or not candidate_raw:
		return 0.0
	if query_raw == candidate_raw:
		return 1.0

	query_norm = normalize_text(query_raw)
	candidate_norm = normalize_text(candidate_raw)
	if query_norm and query_norm == candidate_norm:
		return 0.98

	raw_score = _raw_similarity(query_raw, candidate_raw)
	norm_score = _raw_similarity(query_norm, candidate_norm)
	token_score = _token_overlap(query_norm, candidate_norm)
	length_penalty = min(abs(len(query_norm) - len(candidate_norm)) / 120.0, 0.2)
	base_score = max(raw_score, norm_score)
	if candidate_norm and (candidate_norm in query_norm or query_norm in candidate_norm):
		base_score = max(base_score, 0.9)
	if query_norm.startswith(candidate_norm) or candidate_norm.startswith(query_norm):
		base_score = max(base_score, 0.93)
	if base_score >= 0.85:
		return max(0.0, min(1.0, base_score - length_penalty))
	score = (base_score * 0.7) + (token_score * 0.25) + (min(raw_score, norm_score) * 0.05)
	return max(0.0, min(1.0, score - length_penalty))


def _minimum_score(query: str) -> float:
	query_norm = normalize_text(query)
	query_tokens = len(query_norm.split()) if query_norm else 0
	if query_tokens <= 2:
		return KB_SHORT_QUERY_THRESHOLD
	if query_tokens <= 4:
		return KB_MEDIUM_QUERY_THRESHOLD
	return KB_BASE_THRESHOLD


def select_best_response(query: str, entries: Iterable[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
	"""Pick the best matching response entry from an in-memory list."""
	best: Optional[Tuple[float, Dict[str, Any], str]] = None
	second_best_score = 0.0
	threshold = _minimum_score(query)

	for entry in entries:
		if not entry or not entry.get("is_active", 1):
			continue

		for candidate in _entry_candidates(entry):
			score = _score_candidate(query, candidate)
			if score < threshold:
				continue
			if best is None or score > best[0]:
				if best is not None:
					second_best_score = max(second_best_score, best[0])
				best = (score, entry, candidate)
			else:
				second_best_score = max(second_best_score, score)

	if not best:
		return None

	if best[0] < threshold:
		return None

	score, entry, matched_query = best
	result = dict(entry)
	result["matched_query"] = matched_query
	result["match_score"] = round(score, 3)
	return result


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
	except Exception:
		pass


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
		print("> Direct response KB cache invalidated")
		return True
	except Exception as e:
		frappe.log_error(f"Failed to invalidate KB cache: {e}", "tap_ai.services.direct_response_bank")
		return False



def _render_response(response: str, user_profile: Optional[Dict[str, Any]] = None) -> str:
	if not response:
		return ""

	placeholders = {
		"student_name": str((user_profile or {}).get("name") or "").strip(),
		"name": str((user_profile or {}).get("name") or "").strip(),
		"grade": str((user_profile or {}).get("grade") or "").strip(),
		"batch": str((user_profile or {}).get("batch") or "").strip(),
	}

	def replace(match: re.Match[str]) -> str:
		key = match.group(1)
		return placeholders.get(key, match.group(0))

	return re.sub(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}", replace, str(response))



def lookup_exact_direct_response(
	query: str,
	user_profile: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
	"""
	STAGE 1: EXACT MATCH LOOKUP (FAST PATH)
	=======================================
	
	Return a knowledge-bank response only when the query matches exactly after
	normalization. This is the fastest path in the KB execution flow.
	
	Process:
	1. Normalize the user's query (lowercase, remove special chars, trim)
	2. For each KB entry, get ALL candidates:
	   - student_query (primary intent phrase)
	   - alternate_queries (variants, typos, translations, etc.)
	   └─ _entry_candidates() handles parsing alternate_queries into a list
	3. For each candidate, normalize it the same way
	4. If normalized candidate == normalized query: MATCH FOUND!
	5. Render the response (e.g., replace {name} with user's actual name)
	6. Return immediately (no LLM needed)
	
	Examples of candidates matched:
	  KB Entry: student_query="Hi", alternate_queries="Hey,Hii"
	  ├─ Candidate 1: "Hi"   → Match "hi" (normalized)
	  ├─ Candidate 2: "Hey"  → Match "hey"
	  └─ Candidate 3: "Hii"  → Match "hii" (even with typo)
	
	Timing: ~50ms (no LLM, simple normalization + loop)
	"""
	start = time.perf_counter()
	entries = get_direct_response_entries()
	query_norm = normalize_text(query)

	for entry in entries:
		if not entry or not entry.get("is_active", 1):
			continue

		# Check ALL candidates: student_query + alternate_queries
		for candidate in _entry_candidates(entry):
			if normalize_text(candidate) != query_norm:
				continue

			answer = _render_response(entry.get("response", ""), user_profile=user_profile).strip()
			timing_ms = int((time.perf_counter() - start) * 1000)
			return {
				"question": query,
				"answer": answer,
				"response_type": "knowledge_bank",
				"user_context": "personalized" if user_profile else "general",
				"metadata": {
					"timings_ms": {
						"knowledge_bank": timing_ms,
						"processing_total": timing_ms,
					},
					"answer_source": "knowledge_bank_exact",
					"knowledge_bank": {
						"doctype": KB_DOCTYPE,
						"name": entry.get("name"),
						"title": entry.get("title"),
						"category": entry.get("category"),
						"subcategory": entry.get("subcategory"),
						"student_query": entry.get("student_query"),
						"matched_query": candidate,
						"match_score": 1.0,
					},
				},
			}

	return None


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
