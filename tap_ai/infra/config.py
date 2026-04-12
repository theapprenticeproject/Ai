# infra/config.py

from typing import Any, Dict

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
        print("✅ Configuration loaded successfully")

    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)

    def is_enabled(self, feature: str) -> bool:
        return self._config.get(f"enable_{feature}", False)

    def validate_setup(self) -> dict:
        status = {
            "openai_ready": bool(self.get("openai_api_key")),
            "redis_ready": bool(self.get("redis_url")) and self.is_enabled("redis"),
        }
        print("🔍 Service Status:")
        for service, ready in status.items():
            print(f"   {'✅' if ready else '❌'} {service}: {'Ready' if ready else 'Not configured'}")
        return status

# Global instance + helpers
config = TAPConfig()

def get_config(key: str, default: Any = None) -> Any:
    return config.get(key, default)

def dump_config() -> dict:
    """Return the full loaded config (useful for debugging)."""
    return config._config
