# infra/config.py

from typing import Any, Dict
import logging, os, json
from pathlib import Path

logger = logging.getLogger(__name__)

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

def _read_site_config_from_path() -> Dict[str, Any]:
    """Optional fallback for microservice mode (no frappe import)."""
    sites_dir = os.getenv("FRAPPE_SITES_DIR")
    site_name = os.getenv("FRAPPE_SITE_NAME")
    if not (sites_dir and site_name):
        return {}
    p = Path(sites_dir) / site_name / "site_config.json"
    if not p.exists():
        return {}
    try:
        with open(p, "r") as f:
            return json.load(f) or {}
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


        # 2) Defaults 
        cfg = {
            # API Keys
            "openai_api_key": "",

            # Models
            "primary_llm_model": "gpt-4o-mini",
            "embedding_model": "text-embedding-3-small",

            # Neo4j
            # "neo4j_uri": "",
            # "neo4j_user": "neo4j",
            # "neo4j_password": "",
            # "neo4j_database": "neo4j",
            # "aura_instance_id": "",
            # "aura_instance_name": "",
            
            # Pinecone
            "pinecone_api_key": "",

            # Qdrant
            "qdrant_url": "",
            "qdrant_api_key": "",
            "qdrant_collection": "",

            # Redis (optional for microservice)
            "redis_url": "redis://127.0.0.1:6379",

            # Perf
            "max_context_length": 2048,
            "vector_search_k": 5,
            "max_response_tokens": 500,
            "batch_size": 100,

            # Flags
            "enable_neo4j": True,
            "enable_redis": True,
            "enable_debug": True,
        }

        # 3) Merge site_config values (if any)
        #    Keep your original key names so it’s a drop-in
        for k in cfg.keys():
            if k in site_config:
                cfg[k] = site_config[k]

        self._config = cfg
        print("✅ Configuration loaded successfully")
        # self._log_neo4j_config()

    # def _log_neo4j_config(self):
    #     neo4j_uri = self.get("neo4j_uri")
    #     if neo4j_uri:
    #         if "neo4j+s://" in neo4j_uri:
    #             print("✅ Neo4j Cloud (Aura) configuration detected")
    #             print(f"   🌐 Instance: {self.get('aura_instance_name', 'Unknown')}")
    #             print(f"   🆔 ID: {self.get('aura_instance_id', 'Unknown')}")
    #         elif "bolt://" in neo4j_uri:
    #             print("✅ Neo4j Local configuration detected")
    #         else:
    #             print("⚠️ Unknown Neo4j URI format")
    #     else:
    #         print("❌ No Neo4j URI configured")

    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)

    def is_enabled(self, feature: str) -> bool:
        return self._config.get(f"enable_{feature}", False)

    # def get_neo4j_config(self) -> dict:
    #     return {
    #         "uri": self.get("neo4j_uri"),
    #         "user": self.get("neo4j_user"),
    #         "password": self.get("neo4j_password"),
    #         "database": self.get("neo4j_database"),
    #         "aura_instance_id": self.get("aura_instance_id"),
    #         "aura_instance_name": self.get("aura_instance_name"),
    #         "is_aura": "neo4j+s://" in self.get("neo4j_uri", ""),
    #         "is_local": "bolt://" in self.get("neo4j_uri", ""),
    #     }

    def validate_setup(self) -> dict:
        # neo4j_config = self.get_neo4j_config()
        status = {
            "openai_ready": bool(self.get("openai_api_key")),
            # "neo4j_ready": bool(neo4j_config["uri"]) and self.is_enabled("neo4j"),
            # "neo4j_cloud": neo4j_config["is_aura"],
            "redis_ready": bool(self.get("redis_url")) and self.is_enabled("redis"),
        }
        print("🔍 Service Status:")
        for service, ready in status.items():
            print(f"   {'✅' if ready else '❌'} {service}: {'Ready' if ready else 'Not configured'}")
        # if status["neo4j_ready"]:
        #     if neo4j_config["is_aura"]:
        #         print(f"   🌐 Neo4j Aura Instance: {neo4j_config['aura_instance_name']}")
        #     elif neo4j_config["is_local"]:
        #         print("   🏠 Neo4j Local Instance")
        # return status

# Global instance + helpers
config = TAPConfig()

def get_config(key: str, default: Any = None) -> Any:
    return config.get(key, default)

def dump_config() -> dict:
    """Return the full loaded config (useful for debugging)."""
    return config._config


# def get_neo4j_config() -> dict:
#     return config.get_neo4j_config()
