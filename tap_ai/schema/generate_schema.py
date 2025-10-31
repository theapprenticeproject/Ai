# tap_ai/schema/generate_schema.py
import os
import json
import re
import frappe
from typing import Dict, List, Any, Tuple, Set

OUT_PATH = os.path.join(os.path.dirname(__file__), "tap_ai_schema.json")

# What to exclude
SYSTEM_PREFIXES = ("__", "_", "Data Import", "Deleted", "Version")
EXCLUDE_APPS = []  # Add apps to exclude, e.g., ["frappe"] to exclude core
EXCLUDE_MODULES = ("Core", "Desk", "Email", "Printing", "Website", "Workflow")

def snake_to_title(s: str) -> str:
    return re.sub(r"[_\-]+", " ", s).title()

def discover() -> Tuple[Dict, List[Dict], Dict, Dict]:
    """Discover ALL DocTypes from ALL installed apps."""
    
    tables: Dict[str, Dict] = {}
    joins: List[Dict] = []
    aliases: Dict[str, List[str]] = {}
    allowlist: Dict[str, bool] = {}
    
    # Get all non-child DocTypes from database
    all_doctypes = frappe.get_all(
        "DocType",
        fields=["name", "module"],
        filters={"istable": 0}
    )
    
    print(f"🔍 Found {len(all_doctypes)} DocTypes across all apps")
    
    for dt_info in all_doctypes:
        doctype = dt_info.name
        module = dt_info.get("module", "")
        
        # Skip system doctypes
        if doctype.startswith(SYSTEM_PREFIXES):
            continue
        if module in EXCLUDE_MODULES:
            continue
        
        try:
            meta = frappe.get_meta(doctype)
            table_name = f"tab{doctype}"
            
            # Get fields
            columns = ["name"]
            display_field = meta.title_field
            
            for f in meta.fields:
                if f.fieldname and f.fieldtype not in ("Section Break", "Column Break", "Tab Break", "HTML"):
                    columns.append(f.fieldname)
            
            # Auto-detect display field
            if not display_field:
                for fallback in ["title", "name1", "subject", f"{doctype.lower()}_name"]:
                    if fallback in columns:
                        display_field = fallback
                        break
            
            # Create table info
            description = f"{snake_to_title(doctype)} records."
            if display_field:
                description += f" Key field: {display_field}."
            
            tables[table_name] = {
                "doctype": doctype,
                "pk": "name",
                "display_field": display_field,
                "columns": sorted(set(columns)),
                "description": description,
            }
            
            allowlist[table_name] = True
            
            # Build joins
            for f in meta.fields:
                if f.fieldtype == "Link" and f.options:
                    joins.append({
                        "left_table": table_name,
                        "left_key": f.fieldname,
                        "right_table": f"tab{f.options}",
                        "right_key": "name",
                        "why": f"{doctype}.{f.fieldname} links to {f.options}.name"
                    })
                elif f.fieldtype == "Table" and f.options:
                    joins.append({
                        "left_table": table_name,
                        "left_key": "name",
                        "right_table": f"tab{f.options}",
                        "right_key": "parent",
                        "why": f"{doctype} is parent for {f.options} (child table)."
                    })
            
            # Build aliases
            if display_field:
                aliases[f"{doctype.lower()}_name"] = [table_name, display_field]
            aliases[f"{doctype.lower()}_id"] = [table_name, "name"]
            
        except Exception as e:
            print(f"   ⚠️  Skipped {doctype}: {e}")
            continue
    
    print(f"✅ Processed {len(tables)} DocTypes successfully")
    return tables, joins, aliases, allowlist

def write_schema(payload: Dict[str, Any]):
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    print(f"📝 Schema written to: {OUT_PATH}")

def main():
    print(f"🚀 Generating schema for ALL installed apps...")
    tables, joins, aliases, allowlist = discover()
    
    payload = {
        "tables": tables,
        "allowed_joins": joins,
        "aliases": aliases,
        "allowlist": sorted(list(allowlist.keys())),
        "guardrails": [
            "Use ONLY tables in 'allowlist'.",
            "Use ONLY joins in 'allowed_joins'.",
            "Primary Key is always 'name'.",
            "Prefer 'display_field' when showing records.",
            "Always include LIMIT in queries.",
        ],
    }
    
    write_schema(payload)
    return payload

def cli():
    """bench execute tap_ai.schema.generate_schema.cli"""
    main()
    return {"status": "success"}

if __name__ == "__main__":
    main()