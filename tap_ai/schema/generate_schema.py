import os
import json
import re
import frappe
from typing import Dict, List, Any, Tuple

# --- Configuration ---
APP_NAME = "tap_lms"  # The source app for Doctypes
DOCTYPE_DIR = os.path.join(frappe.get_app_path(APP_NAME), "tap_lms/doctype")  # Dynamically locate tap_lms doctypes
OUT_PATH = os.path.join(os.path.dirname(__file__), "tap_ai_schema.json")

# Optional: restrict discovery to these specific tap_lms modules
ALLOWED_MODULES = []  # e.g., ["student", "VideoClass"] → keep empty to include all

SYSTEM_DTYPES_PREFIXES = ("__", "_")

def snake_to_title(s: str) -> str:
    """Converts a snake_case or kebab-case string to Title Case."""
    return re.sub(r"[_\-]+", " ", s).title()

def load_doctype(path: str) -> Dict[str, Any]:
    """Loads a DocType's JSON definition file."""
    with open(path, "r") as f:
        return json.load(f)

def discover() -> Tuple[Dict, List[Dict], Dict, Dict]:
    """Discovers tap_lms DocTypes and builds a structured schema."""
    tables: Dict[str, Dict] = {}
    joins: List[Dict] = []
    aliases: Dict[str, List[str]] = {}
    allowlist: Dict[str, bool] = {}

    for root, _, files in os.walk(DOCTYPE_DIR):
        # Skip non-allowed modules if filtering is active
        if ALLOWED_MODULES:
            module_name = os.path.basename(os.path.dirname(root)).lower()
            if module_name not in ALLOWED_MODULES:
                continue

        for file in files:
            if not file.endswith(".json"):
                continue

            path = os.path.join(root, file)
            try:
                doc = load_doctype(path)
            except Exception:
                continue

            if doc.get("doctype") != "DocType":
                continue

            doctype = doc.get("name")
            if not doctype or doctype.startswith(SYSTEM_DTYPES_PREFIXES):
                continue

            # Build Frappe-style table name
            table_name = f"tab{doctype}"
            allowlist[table_name] = True

            # Extract columns and identify display field
            fields = doc.get("fields", [])
            columns = []
            display_field = None
            for f in fields:
                fname = f.get("fieldname")
                if not fname:
                    continue
                columns.append(fname)
                if display_field is None and f.get("fieldtype") in ("Data", "Small Text", "Text", "Read Only"):
                    if fname == "name1":
                        display_field = "name1"

            if display_field is None:
                if "name1" in columns:
                    display_field = "name1"
                elif "title" in columns:
                    display_field = "title"

            # Create a human-readable description
            human_desc = f"{snake_to_title(doctype)} records. Key columns: name (Primary Key)"
            if display_field:
                human_desc += f", {display_field} (display name)."

            tables[table_name] = {
                "doctype": doctype,
                "pk": "name",
                "display_field": display_field,
                "columns": sorted(set(columns + ["name"])),
                "description": human_desc,
            }

            # Build joins
            for f in fields:
                if f.get("fieldtype") == "Link" and f.get("options"):
                    joins.append({
                        "left_table": table_name,
                        "left_key": f.get("fieldname"),
                        "right_table": f"tab{f.get('options')}",
                        "right_key": "name",
                        "why": f"{doctype}.{f.get('fieldname')} links to {f.get('options')}.name"
                    })
                elif f.get("fieldtype") == "Table" and f.get("options"):
                    joins.append({
                        "left_table": table_name,
                        "left_key": "name",
                        "right_table": f"tab{f.get('options')}",
                        "right_key": "parent",
                        "why": f"{doctype} is the parent for {f.get('options')} records (a child table)."
                    })

            # Suggested aliases
            if display_field:
                aliases[f"{doctype.lower()}_name"] = [table_name, display_field]
            aliases[f"{doctype.lower()}_id"] = [table_name, "name"]

    return tables, joins, aliases, allowlist

def write_schema(payload: Dict[str, Any]):
    """Writes the generated schema to JSON."""
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    print(f"✅ Schema successfully generated at: {OUT_PATH}")

def main():
    """Main function to discover schema and write it to a file."""
    print(f"🔍 Starting schema discovery for {APP_NAME} ...")
    tables, joins, aliases, allowlist = discover()

    guardrails = [
        "Use ONLY tables listed in the 'allowlist'.",
        "Use ONLY joins defined in 'allowed_joins'.",
        "The Primary Key (PK) for all tables is 'name'.",
        "When showing a record name, prefer its 'display_field' if available.",
        "Always include LIMIT in large queries (e.g., LIMIT 20).",
    ]

    payload = {
        "tables": tables,
        "allowed_joins": joins,
        "aliases": aliases,
        "allowlist": sorted(list(allowlist.keys())),
        "guardrails": guardrails,
    }

    write_schema(payload)

if __name__ == "__main__":
    main()
