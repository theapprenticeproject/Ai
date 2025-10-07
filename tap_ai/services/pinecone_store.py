# tap_ai/services/pinecone_store.py
from __future__ import annotations

import time
import decimal
from datetime import date, datetime, time as dtime
from typing import Dict, List, Optional, Any

import frappe
from pinecone import Pinecone
from langchain_openai import OpenAIEmbeddings

from tap_ai.infra.config import get_config
from tap_ai.infra.sql_catalog import load_schema
from tap_ai.services.doctype_selector import pick_doctypes

# --------- helpers ---------

def _pc() -> Pinecone:
    api_key = get_config("pinecone_api_key")
    if not api_key:
        raise RuntimeError("Missing pinecone_api_key in site_config.json")
    return Pinecone(api_key=api_key)

def _index():
    pc = _pc()
    name = get_config("pinecone_index") or "tap-ai-byo"
    return pc.Index(name)

def _emb() -> OpenAIEmbeddings:
    api_key = get_config("openai_api_key")
    model = get_config("embedding_model") or "text-embedding-3-small"
    if not api_key:
        raise RuntimeError("Missing openai_api_key in site_config.json")
    return OpenAIEmbeddings(model=model, api_key=api_key)

def _to_plain(v: Any) -> Any:
    """Make values JSON-safe for text conversion."""
    if v is None: return None
    if isinstance(v, (str, int, float, bool)): return v
    if isinstance(v, decimal.Decimal): return float(v)
    if isinstance(v, (datetime, date, dtime)): return v.isoformat()
    return str(v)

def _record_to_text(doctype: str, row: Dict[str, Any]) -> str:
    """
    Flatten a record to a text block, giving extra weight to the doctype's most important field.
    """
    parts = []
    meta = frappe.get_meta(doctype)
    
    title_field = None
    title_value = None

    official_title_field = meta.title_field
    if official_title_field and official_title_field in row and row[official_title_field]:
        title_field = official_title_field
        title_value = row[title_field]
    else:
        fallback_fields = [
            'title', 'name1', 'video_name', 'assignment_name', 'project_name', 
            'quiz_name', 'objective_name', 'unit_name', 'comp_name', 'note_name'
        ]
        for field in fallback_fields:
            if field in row and row[field]:
                title_field = field
                title_value = row[field]
                break

    if title_field and title_value:
        title_label = meta.get_field(title_field).label or title_field.replace("_", " ").title()
        parts.append(f"{title_label}: {title_value}")

    parts.append(f"DocType: {doctype}")
    parts.append(f"ID: {row.get('name','')}")
    
    for k, v in row.items():
        if k in ('name', title_field) or v in (None, ""):
            continue
        parts.append(f"{k}: {_to_plain(v)}")
        
    return "\n".join(parts)

def get_db_columns_for_doctype(doctype: str) -> list[str]:
    """Return only actual DB columns for the DocType's table, with a robust fallback."""
    table = f"tab{doctype}"
    try:
        return frappe.db.get_table_columns(table) or []
    except Exception:
        desc = frappe.db.sql(f"DESCRIBE `{table}`", as_dict=True)
        return [d["Field"] for d in desc]

# --------- upsert pipeline ---------

def upsert_doctype(
    doctype: str,
    since: Optional[str] = None,
    group_records: int = 20,
    embed_batch: int = 64,
) -> Dict[str, Any]:
    idx = _index()
    emb = _emb()
    total_records, total_vectors = 0, 0
    table = f"tab{doctype}"
    
    try:
        select_cols = frappe.db.get_table_columns(table) or []
    except Exception:
        desc = frappe.db.sql(f"DESCRIBE `{table}`", as_dict=True)
        select_cols = [d["Field"] for d in desc]

    if "name" in select_cols:
        select_cols = ["name"] + [c for c in select_cols if c != "name"]

    where_parts = ["docstatus < 2"]
    params: List[Any] = []
    if since:
        where_parts.append("modified >= %s")
        params.append(since)
    where_sql = " AND ".join(where_parts)

    page_size = 1000
    offset = 0
    buffer_texts, buffer_ids, buffer_meta = [], [], []

    def flush():
        nonlocal total_vectors
        if not buffer_texts: return
        
        vectors_values = emb.embed_documents(buffer_texts)
        vectors = [
            {"id": buffer_ids[i], "values": vectors_values[i], "metadata": buffer_meta[i]}
            for i in range(len(buffer_texts))
        ]
        idx.upsert(vectors=vectors, namespace=doctype)
        total_vectors += len(vectors)
        buffer_texts.clear(); buffer_ids.clear(); buffer_meta.clear()

    group: List[Dict[str, Any]] = []
    while True:
        rows = frappe.get_all(doctype, filters={"docstatus": ("<", 2)}, fields=select_cols, limit_page_length=page_size, limit_start=offset)
        if not rows: break

        for row in rows:
            total_records += 1
            group.append(row)
            if len(group) >= group_records:
                record_ids = [str(g.get("name")) for g in group]
                combined_text = "\n\n--- END OF RECORD ---\n\n".join([_record_to_text(doctype, g) for g in group])
                
                # --- THIS IS THE KEY CHANGE ---
                # Create a metadata payload with filterable fields
                meta = {
                    "doctype": doctype,
                    "record_ids": record_ids,
                    "text": combined_text,
                    "count": len(group)
                }
                # Add specific, known filterable fields from the first record
                first_rec = group[0]
                filterable_fields = ["status", "difficulty_tier", "language", "assignment_type", "grade_level", "subject"]
                for field in filterable_fields:
                    if field in first_rec and first_rec[field]:
                        meta[field] = first_rec[field]

                buffer_texts.append(combined_text)
                buffer_ids.append(f"{doctype}:{record_ids[0]}:+{len(record_ids)}")
                buffer_meta.append(meta)
                group = []

                if len(buffer_texts) >= embed_batch: flush()
        offset += page_size
    
    # Process final partial group
    if group:
        record_ids = [str(g.get("name")) for g in group]
        combined_text = "\n\n--- END OF RECORD ---\n\n".join([_record_to_text(doctype, g) for g in group])
        meta = {"doctype": doctype, "record_ids": record_ids, "text": combined_text, "count": len(group)}
        first_rec = group[0]
        filterable_fields = ["status", "difficulty_tier", "language", "assignment_type", "grade_level", "subject"]
        for field in filterable_fields:
            if field in first_rec and first_rec[field]:
                meta[field] = first_rec[field]
        buffer_texts.append(combined_text)
        buffer_ids.append(f"{doctype}:{record_ids[0]}:+{len(record_ids)}")
        buffer_meta.append(meta)
    
    flush()
    return {"doctype": doctype, "records_seen": total_records, "vectors_upserted": total_vectors}

def upsert_all(
    doctypes: Optional[List[str]] = None,
    since: Optional[str] = None,
    group_records: int = 20,
    embed_batch: int = 64,
) -> Dict[str, Any]:
    """Upsert multiple doctypes with user-friendly console logging."""
    if doctypes is None:
        schema = load_schema()
        doctypes = [t[3:] if t.startswith("tab") else t for t in schema.get("allowlist", [])]

    print(f"Starting upsert for {len(doctypes)} DocTypes...")

    out: Dict[str, Any] = {}
    for i, dt in enumerate(doctypes):
        print(f"\n[{i+1}/{len(doctypes)}] Processing DocType: {dt}...")
        try:
            result = upsert_doctype(dt, since=since, group_records=group_records, embed_batch=embed_batch)
            out[dt] = result
            if "error" in result:
                print(f"❗️ Error processing {dt}: {result['error']}")
            else:
                print(f"✅ Finished {dt}: Saw {result.get('records_seen', 0)} records, upserted {result.get('vectors_upserted', 0)} vectors.")

        except Exception as e:
            error_msg = str(e)
            out[dt] = {"error": error_msg}
            frappe.log_error(f"Failed to upsert doctype {dt}", error_msg)
            print(f"❗️ Critical error processing {dt}: {error_msg}")

    print("\n--- Upsert process completed. ---")
    return out

# --------- search ---------

def search_auto_namespaces(
    q: str,
    k: int = 8,
    route_top_n: int = 4,
    filters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Routes query to relevant DocTypes, then searches Pinecone with optional metadata filters.
    """
    idx = _index()
    emb = _emb()
    doctypes = pick_doctypes(q, top_n=route_top_n) or ["VideoClass"] # Fallback to a default
    
    qvec = emb.embed_query(q)
    all_matches: List[Dict] = []
    
    for ns in doctypes:
        try:
            res = idx.query(
                namespace=ns,
                vector=qvec,
                top_k=k,
                filter=filters, 
                include_values=False,
                include_metadata=True,
            )
            for m in res.get("matches", []):
                match_dict = {
                    "id": m.id,
                    "score": m.score,
                    "namespace": ns,
                    "metadata": m.metadata,
                }
                all_matches.append(match_dict)
        except Exception as e:
            frappe.log_error(f"Pinecone query failed for ns={ns}: {e}")

    all_matches.sort(key=lambda x: x.get("score", 0.0), reverse=True)
    return {
        "q": q, "routed_doctypes": doctypes, "k": k, "matches": all_matches[:k],
    }
# --------- bench CLIs ---------

def cli_upsert_all(doctypes: Optional[List[str]] = None, since: Optional[str] = None, group_records: int = 20):
    # use since parameter for incremental updates, e.g. "2025-10-01 12:34:56"
    out = upsert_all(doctypes=doctypes, since=since, group_records=group_records)
    print(frappe.as_json(out))
    return out

def cli_search_auto(q: str, k: int = 8, route_top_n: int = 4):
    out = search_auto_namespaces(q=q, k=k, route_top_n=route_top_n)
    print(frappe.as_json(out, indent=2))
    return out
