# infra/config.py
"""
TAP AI configuration loader.

Provides a unified config interface that reads from Frappe's `site_config.json`
when running inside a Frappe bench, and falls back gracefully when running as a
standalone microservice or in tests.

Usage:
    from tap_ai.infra.config import get_config
    api_key = get_config("openai_api_key")

All tap_ai config keys live in site_config.json alongside standard Frappe keys.
See README.md for the full list of supported keys and their defaults.

Module-level singleton: `config` (TAPConfig instance)
Helper functions: `get_config(key, default)`, `dump_config()`
"""

from typing import Any, Dict
from loguru import logger

def _try_import_frappe():
    try:
        import frappe  
        return frappe
    except Exception:
        return None

def _read_site_config_from_frappe(fr):
    try:
        return fr.get_site_config() or {}
    except Exception:
        return {}

class TAPConfig:
    """
    Config loader that prefers Frappe's site_config.json.
    Works both inside Frappe and as a standalone microservice.
    """
    def __init__(self):
        self._config: Dict[str, Any] = {}
        self._load_config()

    def _load_config(self) -> None:
        # 1) Try Frappe first
        frappe = _try_import_frappe()
        site_config = _read_site_config_from_frappe(frappe) if frappe else {}


        self._config = site_config or {}
        logger.debug("tap_ai configuration loaded")

    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)

    def is_enabled(self, feature: str) -> bool:
        return self._config.get(f"enable_{feature}", False)

    def validate_setup(self) -> dict:
        status = {
            "openai_ready": bool(self.get("openai_api_key")),
            "redis_ready": bool(self.get("redis_url")) and self.is_enabled("redis"),
        }
        for service, ready in status.items():
            level = "info" if ready else "warning"
            getattr(logger, level)(f"Service {service}: {'ok' if ready else 'not configured'}")
        return status

# Global instance + helpers
config = TAPConfig()

def get_config(key: str, default: Any = None) -> Any:
    return config.get(key, default)

def dump_config() -> dict:
    """Return the full loaded config (useful for debugging)."""
    return config._config
