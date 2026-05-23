# tap_ai/services/pinecone_index.py
"""
Pinecone index lifecycle management.

Handles creation, inspection, and deletion of the Pinecone vector index used
by the RAG engine. Reads index name, embedding dimension, cloud/region, and
metric from site_config.json so the same code works across environments.

Key functions:
    ensure_index()   — create the index if it does not exist (idempotent)
    describe_index() — return current index stats (vector count, dimension)
    delete_index()   — drop the index (useful for full resets during dev)

These are admin utilities; they are NOT called on the hot path.
The hot path (query time) uses pinecone_store.py directly.

Config keys (site_config.json):
    pinecone_api_key, pinecone_index, embedding_dimension
"""

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
    name = get_config("pinecone_index") or "tap-ai-byo" or index_name
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
    name = get_config("pinecone_index") or "tap-ai-byo" or index_name

    if name in pc.list_indexes().names():
        print(f"Attempting to delete index '{name}'. This is irreversible...")
        pc.delete_index(name)
        print(f"Index '{name}' deleted.")
        return {"index": name, "status": "deleted"}
    else:
        print(f"Index '{name}' does not exist. Nothing to delete.")
        return {"index": name, "status": "not_found"}

# ---- bench CLI ----
def cli_ensure_index(index_name: Optional[str] = None, dimension: Optional[int] = None):
    '''
    bench execute tap_ai.services.pinecone_index.cli_ensure_index
    '''
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
