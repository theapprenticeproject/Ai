# tap_ai/services/pinecone_store.py
from __future__ import annotations

import time
import decimal
import json
from datetime import date, datetime, time as dtime
from typing import Dict, List, Optional, Any

import frappe
from pinecone import Pinecone
from langchain_openai import OpenAIEmbeddings

from tap_ai.infra.config import get_config
from tap_ai.infra.sql_catalog import load_schema
from tap_ai.services.doctype_selector import pick_doctypes


# -------------------------------------------------------------------
# Pinecone / Embedding helpers
# -------------------------------------------------------------------

def _pc() -> Pinecone:
    api_key = get_config("pinecone_api_key")
    if not api_key:
        raise RuntimeError("Missing pinecone_api_key in site_config.json")
    return Pinecone(api_key=api_key)

def _index():
    pc = _pc()
    name = get_config("pinecone_index") or "tap-lms-byo"
    return pc.Index(name)

def _emb() -> OpenAIEmbeddings:
    api_key = get_config("openai_api_key")
    model = get_config("embedding_model") or "text-embedding-3-small"
    if not api_key:
        raise RuntimeError("Missing openai_api_key in site_config.json")
    return OpenAIEmbeddings(model=model, api_key=api_key)


# -------------------------------------------------------------------
# Utility helpers
# -------------------------------------------------------------------

def _to_plain(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (datetime, date, dtime)):
        return v.isoformat()
    return str(v)

def _record_to_text(doctype: str, row: Dict[str, Any]) -> str:
    parts = []
    meta = frappe.get_meta(doctype)

    title_field = meta.title_field
    if title_field and row.get(title_field):
        label = meta.get_field(title_field).label or title_field
        parts.append(f"{label}: {row[title_field]}")

    parts.append(f"DocType: {doctype}")
    parts.append(f"ID: {row.get('name')}")

    for k, v in row.items():
        if k in ("name", title_field) or v in (None, ""):
            continue
        parts.append(f"{k}: {_to_plain(v)}")

    return "\n".join(parts)

def get_db_columns_for_doctype(doctype: str) -> List[str]:
    table = f"tab{doctype}"
    try:
        return frappe.db.get_table_columns(table) or []
    except Exception:
        desc = frappe.db.sql(f"DESCRIBE `{table}`", as_dict=True)
        return [d["Field"] for d in desc]


# -------------------------------------------------------------------
# ExcludedDoctypes handling
# -------------------------------------------------------------------

def _get_excluded_doctypes() -> set[str]:
    excluded = set()
    try:
        recs = frappe.get_all("ExcludedDoctypes", fields=["name"], limit=1)
        if not recs:
            return excluded
        doc = frappe.get_doc("ExcludedDoctypes", recs[0].name)
        for row in doc.excluded_doctype:
            if row.doctype_name:
                excluded.add(row.doctype_name)
    except Exception:
        pass
    return excluded

def _filter_excluded(doctypes: List[str]) -> List[str]:
    excluded = _get_excluded_doctypes()
    return [dt for dt in doctypes if dt not in excluded]


# -------------------------------------------------------------------
# Upsert pipeline
# -------------------------------------------------------------------

def upsert_doctype(  
    doctype: str,  
    since: Optional[str] = None,  
    group_records: int = 20,  
    embed_batch: int = 64,  
) -> Dict[str, Any]:  
  
    idx = _index()  
    emb = _emb()  
  
    total_records = 0  
    total_vectors = 0  
  
    table = f"tab{doctype}"  
    columns = get_db_columns_for_doctype(doctype)  
  
    if "name" in columns:  
        columns = ["name"] + [c for c in columns if c != "name"]  
  
    # Build filters - actually use the 'since' parameter  
    filters = {"docstatus": ("<", 2)}  
    if since:  
        filters["modified"] = (">=", since)  
  
    rows = frappe.get_all(  
        doctype,  
        fields=columns,  
        filters=filters,  # Now uses the since parameter  
    )

    buffer_texts, buffer_ids, buffer_meta = [], [], []

    def flush():
        nonlocal total_vectors
        if not buffer_texts:
            return
        vectors = emb.embed_documents(buffer_texts)
        payload = [
            {
                "id": buffer_ids[i],
                "values": vectors[i],
                "metadata": buffer_meta[i],
            }
            for i in range(len(buffer_texts))
        ]
        idx.upsert(vectors=payload, namespace=doctype)
        total_vectors += len(payload)
        buffer_texts.clear()
        buffer_ids.clear()
        buffer_meta.clear()

    group: List[Dict[str, Any]] = []

    for row in rows:
        total_records += 1
        group.append(row)

        if len(group) >= group_records:
            record_ids = [str(r["name"]) for r in group]
            text = "\n\n---\n\n".join(_record_to_text(doctype, r) for r in group)

            meta = {
                "doctype": doctype,
                "record_ids": record_ids,
                "count": len(group),
                "text": text,
            }

            buffer_texts.append(text)
            buffer_ids.append(f"{doctype}:{record_ids[0]}")
            buffer_meta.append(meta)
            group = []

            if len(buffer_texts) >= embed_batch:
                flush()

    if group:
        record_ids = [str(r["name"]) for r in group]
        text = "\n\n---\n\n".join(_record_to_text(doctype, r) for r in group)
        buffer_texts.append(text)
        buffer_ids.append(f"{doctype}:{record_ids[0]}")
        buffer_meta.append({
            "doctype": doctype,
            "record_ids": record_ids,
            "count": len(group),
            "text": text,
        })

    flush()

    return {
        "doctype": doctype,
        "records_seen": total_records,
        "vectors_upserted": total_vectors,
    }


def upsert_all(
    doctypes: Optional[List[str]] = None,
    since: Optional[str] = None,
) -> Dict[str, Any]:

    if doctypes is None:
        schema = load_schema()
        doctypes = [t.replace("tab", "") for t in schema.get("allowlist", [])]

    out = {}
    for dt in doctypes:
        try:
            out[dt] = upsert_doctype(dt, since=since)
        except Exception as e:
            out[dt] = {"error": str(e)}
            frappe.log_error(f"Upsert failed for {dt}", str(e))

    return out


# -------------------------------------------------------------------
# SEARCH (THIS IS THE IMPORTANT PART)
# -------------------------------------------------------------------

def search_auto_namespaces(
    q: str,
    k: int = 8,
    route_top_n: int = 4,
    filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:

    idx = _index()
    emb = _emb()

    # 1. Route doctypes using LLM
    doctypes = pick_doctypes(q, top_n=route_top_n) or []

    # 2. Enforce exclusion list
    doctypes = _filter_excluded(doctypes)

    # 3. Fallback to schema allowlist if routing failed
    if not doctypes:
        schema = load_schema()
        doctypes = [
            t.replace("tab", "")
            for t in schema.get("allowlist", [])[:route_top_n]
        ]

    qvec = emb.embed_query(q)
    all_matches: List[Dict[str, Any]] = []

    for ns in doctypes:
        try:
            res = idx.query(
                namespace=ns,
                vector=qvec,
                top_k=k,
                filter=filters,
                include_metadata=True,
                include_values=False,
            )

            for m in res.get("matches", []):
                all_matches.append({
                    "id": m.id,
                    "score": m.score,
                    "namespace": ns,
                    "metadata": m.metadata,
                })

        except Exception as e:
            frappe.log_error(
                f"Pinecone query failed for namespace {ns}", str(e)
            )

    all_matches.sort(key=lambda x: x.get("score", 0), reverse=True)

    return {
        "q": q,
        "routed_doctypes": doctypes,
        "k": k,
        "matches": all_matches[:k],
    }


# -------------------------------------------------------------------
# Bench CLIs
# -------------------------------------------------------------------

def cli_upsert_all():
    """
    bench execute tap_ai.services.pinecone_store.cli_upsert_all
    """
    out = upsert_all()
    print(frappe.as_json(out, indent=2))
    return out

def cli_search_auto(q: str, k: int = 8, route_top_n: int = 4):
    out = search_auto_namespaces(q=q, k=k, route_top_n=route_top_n)
    print(frappe.as_json(out, indent=2))
    return out

