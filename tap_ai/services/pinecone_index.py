# tap_ai/services/pinecone_index.py
# Added a delete_index function for easy resets.
from __future__ import annotations
import time
import frappe
from typing import Optional
from pinecone import Pinecone, ServerlessSpec

from tap_ai.infra.config import get_config

def _pc() -> Pinecone:
    api_key = get_config("pinecone_api_key")
    if not api_key:
        raise RuntimeError("Missing pinecone_api_key in site_config.json")
    return Pinecone(api_key=api_key)

def ensure_index(
    index_name: Optional[str] = None,
    dimension: Optional[int] = None,
    cloud: str = "aws",
    region: str = "us-east-1",
    metric: str = "cosine",
) -> dict:
    """
    Create the Pinecone index if it does not exist.
    Dimension must match your embedding model (e.g. OpenAI text-embedding-3-small = 1536).
    """
    pc = _pc()
    name = index_name or get_config("pinecone_index") or "tap-ai-byo"
    dim = int(dimension or get_config("embedding_dimension") or 1536)

    if name not in pc.list_indexes().names():
        print(f"Index '{name}' not found. Creating now...")
        pc.create_index(
            name=name,
            dimension=dim,
            metric=metric,
            spec=ServerlessSpec(cloud=cloud, region=region),
        )
        # Wait until the index is ready
        while not pc.describe_index(name).status['ready']:
            time.sleep(1)
        print(f"Index '{name}' created successfully.")
    else:
        print(f"Index '{name}' already exists.")

    return {"index": name, "dimension": dim, "metric": metric, "ready": True}

def delete_index(index_name: Optional[str] = None) -> dict:
    """
    Deletes the specified Pinecone index. This is irreversible.
    """
    pc = _pc()
    name = index_name or get_config("pinecone_index") or "tap-ai-byo"
    
    if name in pc.list_indexes().names():
        print(f"Attempting to delete index '{name}'. This is irreversible...")
        pc.delete_index(name)
        print(f"✅ Index '{name}' deleted successfully.")
        return {"index": name, "status": "deleted"}
    else:
        print(f"Index '{name}' does not exist. Nothing to delete.")
        return {"index": name, "status": "not_found"}

# ---- bench CLI ----
def cli_ensure_index(index_name: Optional[str] = None, dimension: Optional[int] = None):
    out = ensure_index(index_name=index_name, dimension=dimension)
    print(frappe.as_json(out))
    return out

def cli_delete_index(index_name: Optional[str] = None):
    """
    Bench command to delete the index.
    Example:
      bench execute tap_ai.services.pinecone_index.cli_delete_index
    """
    out = delete_index(index_name=index_name)
    print(frappe.as_json(out))
    return out
