# Changelog

All notable changes to TAP AI are documented here.

---

## [2.0.0] - 2026-04-15

### Added
- Hybrid Knowledge Bank verifier: the system now probes the best KB candidate and asks the LLM to verify whether the candidate appropriately answers the user's query; the LLM either returns the KB response (optionally lightly personalized) or generates a fresh answer. This reduces false positives (e.g., distinguishing "who are you" vs "how are you").
- Router now uses the hybrid verifier for `knowledge_bank` routed queries.
- DocType event hooks invalidate the KB cache on insert/update/delete to keep the KB context fresh.
- Verifier LLM cache to reduce latency for repeated verification queries (TTL configurable).
- Unified query and result endpoints replacing separate text/voice APIs.
- Backward-compatible `voice_query` and `voice_result` alias endpoints.
- `routing_patterns.py`: regex fast-path for zero-LLM KB and SQL routing.
- `prompt_bank.py`: per-context persona injection via `Prompt Suggestion` DocType.
- `metrics.py`: RabbitMQ queue health endpoint.
- `wait.py`: delay endpoint for Glific workflow pacing.
- pytest suite for routing patterns and SQL sanitization.

### Changed
- SQL generation now uses PostgreSQL double-quote syntax.
- All log prefixes replaced with plain text (removed emojis from Python logging).

### Fixed
- Router skips query refinement for unconditional KB intents.
