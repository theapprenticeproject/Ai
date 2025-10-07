import os
import json
import re
from typing import Dict, List, Any, Tuple

# --- Configuration ---
# Adjust this path if your script is located elsewhere relative to your app
APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Assumes your app is named 'tap_ai'
APP_NAME = "tap_ai" 
DOCTYPE_DIR = os.path.join(APP_ROOT, APP_NAME, "doctype")
OUT_PATH = os.path.join(os.path.dirname(__file__), "tap_ai_schema.json")

# Quick filter for non-business doctypes (e.g., __dashboard, _chart)
SYSTEM_DTYPES_PREFIXES = ("__", "_")

def snake_to_title(s: str) -> str:
    """Converts a snake_case or kebab-case string to Title Case."""
    return re.sub(r"[_\-]+", " ", s).title()

def load_doctype(path: str) -> Dict[str, Any]:
    """Loads a DocType's JSON definition file."""
    with open(path, "r") as f:
        return json.load(f)

def discover() -> Tuple[Dict, List[Dict], Dict, Dict]:
    """
    Discovers DocTypes and builds a schema including tables, joins, aliases,
    and an allowlist for an AI agent.

    Returns:
      tables: {table_name: {doctype, display_field, pk, columns, description}}
      joins:  [{"left_table","left_key","right_table","right_key","why"}]
      aliases: Suggested column aliases for nicer prompts
      allowlist: {table_name: True}
    """
    tables: Dict[str, Dict] = {}
    joins: List[Dict] = []
    aliases: Dict[str, List[str]] = {}
    allowlist: Dict[str, bool] = {}

    # Walk through the doctype directory to find all doctype definitions
    for root, _, files in os.walk(DOCTYPE_DIR):
        for file in files:
            if not file.endswith(".json"):
                continue

            path = os.path.join(root, file)
            try:
                doc = load_doctype(path)
            except Exception:
                # Ignore files that are not valid JSON
                continue

            # We are only interested in the main DocType definition files
            if doc.get("doctype") != "DocType":
                continue

            doctype = doc.get("name")
            if not doctype or doctype.startswith(SYSTEM_DTYPES_PREFIXES):
                continue

            # Frappe's database table naming convention is "tab" + DocType name
            table_name = f"tab{doctype}"
            allowlist[table_name] = True

            # --- Collect fields/columns and identify a display field ---
            fields = doc.get("fields", [])
            columns = []
            display_field = None
            for f in fields:
                fname = f.get("fieldname")
                if not fname: continue
                columns.append(fname)
                # Heuristic to pick a good display field (e.g., a title or description)
                if display_field is None and f.get("fieldtype") in ("Data", "Small Text", "Text", "Read Only"):
                    # Prefer 'name1' if it exists, as it's often a human-readable name
                    if fname == "name1":
                        display_field = "name1"

            # Fallback logic for display field if the first heuristic didn't find one
            if display_field is None:
                if "name1" in columns: display_field = "name1"
                elif "title" in columns: display_field = "title"
                else: display_field = None # Agent can default to using the 'name' PK

            # --- Create a human-readable description for the table ---
            human_desc = f"{snake_to_title(doctype)} records. Key columns: name (Primary Key)"
            if display_field:
                human_desc += f", {display_field} (display name)"
            human_desc += "."

            tables[table_name] = {
                "doctype": doctype,
                "pk": "name",
                "display_field": display_field,
                "columns": sorted(set(columns + ["name"])), # 'name' is always the PK
                "description": human_desc,
            }

            # --- Build joins from Link and Table fields ---
            for f in fields:
                # Case 1: 'Link' field (Many-to-One relationship)
                # This doctype's table has a column that is a foreign key to another table's primary key.
                if f.get("fieldtype") == "Link" and f.get("options"):
                    left_key = f.get("fieldname")
                    right_doctype = f.get("options")
                    right_table = f"tab{right_doctype}"
                    why = f"{doctype}.{left_key} links to {right_doctype}.name"
                    joins.append({
                        "left_table": table_name,
                        "left_key": left_key,
                        "right_table": right_table,
                        "right_key": "name", # Links are always to the target's PK
                        "why": why,
                    })

                # Case 2: 'Table' field (One-to-Many relationship / Child Table)
                # This doctype is a PARENT. Its primary key is a foreign key in a CHILD table's 'parent' column.
                elif f.get("fieldtype") == "Table" and f.get("options"):
                    # The current table is the parent
                    left_table = table_name
                    left_key = "name" # Parent's Primary Key

                    # The 'options' of a Table field is the child DocType
                    child_doctype = f.get("options")
                    child_table = f"tab{child_doctype}"
                    child_key = "parent" # Child's Foreign Key back to the parent

                    why = f"{doctype} is the parent for {child_doctype} records (a child table)."
                    joins.append({
                        "left_table": left_table,
                        "left_key": left_key,
                        "right_table": child_table,
                        "right_key": child_key,
                        "why": why,
                    })

            # --- Suggested aliases for convenience ---
            if display_field:
                aliases[f"{doctype.lower()}_name"] = [table_name, display_field]
            aliases[f"{doctype.lower()}_id"] = [table_name, "name"]

    return tables, joins, aliases, allowlist

def write_schema(payload: Dict[str, Any]):
    """Writes the generated schema payload to a JSON file."""
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    print(f"✅ Schema successfully generated at: {OUT_PATH}")

def main():
    """Main function to discover schema and write it to a file."""
    print("Starting schema discovery...")
    tables, joins, aliases, allowlist = discover()

    # Optional: Injected guardrails for an AI agent to follow
    guardrails = [
        "Use ONLY tables listed in the 'allowlist'.",
        "Use ONLY joins defined in the 'allowed_joins'.",
        "The Primary Key (PK) for all tables is the 'name' column.",
        "When displaying a record's name to a user, prefer using its 'display_field' column if available.",
        "Always include a LIMIT clause (e.g., LIMIT 20) in queries that might return many rows.",
    ]

    payload = {
        "tables": tables,
        "allowed_joins": joins,
        "aliases": aliases,
        "allowlist": sorted([t for t in allowlist.keys()]),
        "guardrails": guardrails,
        # You can add pre-defined, safe SQL views here in the future
        "safe_views": {},
    }
    write_schema(payload)

if __name__ == "__main__":
    main()
