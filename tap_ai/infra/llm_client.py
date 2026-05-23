# tap_ai/infra/llm_client.py

"""
Shared LLM client and cached invocation utility for TAP AI services.
"""

import json
import time
import hashlib
from typing import List, Optional

import frappe
from langchain_openai import ChatOpenAI
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

from tap_ai.infra.config import get_config


class LLMClient:
    """Singleton LLM client manager."""

    _instances = {}

    @classmethod
    def get_client(
        cls,
        model: str = "gpt-4o-mini",
        temperature: float = 0.0,
        max_tokens: int = 1500,
    ) -> ChatOpenAI:
        cache_key = f"{model}_{temperature}_{max_tokens}"

        if cache_key not in cls._instances:
            api_key = get_config("openai_api_key")
            if not api_key:
                raise ValueError("OpenAI API key not configured")

            timeout = int(get_config("llm_request_timeout_s") or 60)
            cls._instances[cache_key] = ChatOpenAI(
                model_name=model,
                openai_api_key=api_key,
                temperature=temperature,
                max_tokens=max_tokens,
                timeout=timeout,
                max_retries=2,
            )

        return cls._instances[cache_key]

    @classmethod
    def clear_cache(cls):
        cls._instances.clear()


def llm_invoke_cached(
    messages: List,
    model: str = "gpt-4o-mini",
    temperature: float = 0.0,
    cache_ttl: int = 3600,
    max_tokens: int = 700,
) -> str:
    """Invoke LLM with Redis response caching; falls back to live invoke on cache issues."""
    try:
        payload = {"messages": messages, "model": model, "temperature": temperature}
        cache_key = "llm_cache:" + hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

        cached = frappe.cache().get(cache_key)
        if cached:
            if isinstance(cached, bytes):
                cached = cached.decode("utf-8", errors="ignore")
            return str(cached)
    except Exception:
        cache_key = None

    llm = LLMClient.get_client(model=model, temperature=temperature, max_tokens=max_tokens)
    start = time.time()

    # Retry on transient errors (timeouts, connection issues, server errors).
    # Do NOT retry on configuration errors (ValueError, KeyError) — those won't self-heal.
    _non_retryable = (ValueError, KeyError, TypeError)

    @retry(
        retry=retry_if_exception(lambda e: not isinstance(e, _non_retryable)),
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=2, max=8),
        reraise=True,
    )
    def _invoke():
        return llm.invoke(messages)

    resp = _invoke()
    content = getattr(resp, "content", "") or ""
    content = str(content).strip()

    try:
        if cache_key and content:
            frappe.cache().set(cache_key, content, ex=cache_ttl)
    except Exception as e:
        logger.warning(f"LLM response cache write failed: {e}")

    logger.debug(f"LLM invoke ({model}) took {int((time.time() - start) * 1000)}ms")
    return content
