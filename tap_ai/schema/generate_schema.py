# tap_ai/schema/generate_schema.py

import os
import json
import re
import frappe
from typing import Dict, List, Any, Tuple, Set

# -------------------------------------------------------------------
# Output path
# -------------------------------------------------------------------

OUT_PATH = os.path.join(os.path.dirname(__file__), "tap_ai_schema.json")

# -------------------------------------------------------------------
# Hard exclusions (system-level)
# -------------------------------------------------------------------

SYSTEM_PREFIXES = ("__", "_", "Data Import", "Deleted", "Version")
EXCLUDE_MODULES = ("Core", "Desk", "Email", "Printing", "Website", "Workflow")

# -------------------------------------------------------------------
# Utility helpers
# -------------------------------------------------------------------

def snake_to_title(s: str) -> str:
    return re.sub(r"[_\-]+", " ", s).title()


# -------------------------------------------------------------------
# ExcludedDoctypes integration
# -------------------------------------------------------------------

def get_excluded_doctypes() -> Set[str]:
    """
    Fetch excluded doctypes from ExcludedDoctypes DocType.
    Returns a set of doctype names to exclude.
    """
    excluded: Set[str] = set()

    try:
        records = frappe.get_all(
            "ExcludedDoctypes",
            fields=["name"],
            limit=1
        )

        if not records:
            print("ℹ️ No ExcludedDoctypes record found")
            return excluded

        doc = frappe.get_doc("ExcludedDoctypes", records[0].name)

        for row in doc.excluded_doctype:
            if row.doctype_name:
                excluded.add(row.doctype_name)

    except Exception as e:
        print(f"⚠️ Failed to load ExcludedDoctypes: {e}")

    return excluded


# -------------------------------------------------------------------
# Core discovery logic
# -------------------------------------------------------------------

def discover() -> Tuple[Dict, List[Dict], Dict, Dict]:
    """
    Discover ALL DocTypes from ALL installed apps,
    excluding system, module-based, and admin-defined doctypes.
    """

    tables: Dict[str, Dict] = {}
    joins: List[Dict] = []
    aliases: Dict[str, List[str]] = {}
    allowlist: Dict[str, bool] = {}

    excluded_doctypes = get_excluded_doctypes()

    all_doctypes = frappe.get_all(
        "DocType",
        fields=["name", "module"],
        filters={"istable": 0}
    )

    print(f"> Found {len(all_doctypes)} DocTypes across all apps")
    print(f"> Excluding {len(excluded_doctypes)} doctypes via ExcludedDoctypes")

    for dt_info in all_doctypes:
        doctype = dt_info.name
        module = dt_info.get("module", "")

        # ------------------------------------------------------------
        # Exclusion rules
        # ------------------------------------------------------------
        if doctype in excluded_doctypes:
            continue
        if doctype.startswith(SYSTEM_PREFIXES):
            continue
        if module in EXCLUDE_MODULES:
            continue

        try:
            meta = frappe.get_meta(doctype)
            table_name = f"tab{doctype}"

            # --------------------------------------------------------
            # Columns
            # --------------------------------------------------------
            columns = ["name"]
            display_field = meta.title_field

            for f in meta.fields:
                if f.fieldname and f.fieldtype not in (
                    "Section Break",
                    "Column Break",
                    "Tab Break",
                    "HTML",
                ):
                    columns.append(f.fieldname)

            # Auto-detect display field if not defined
            if not display_field:
                for fallback in [
                    "title",
                    "name1",
                    "subject",
                    f"{doctype.lower()}_name",
                ]:
                    if fallback in columns:
                        display_field = fallback
                        break

            # --------------------------------------------------------
            # Table metadata
            # --------------------------------------------------------
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

            # --------------------------------------------------------
            # Joins
            # --------------------------------------------------------
            for f in meta.fields:
                if f.fieldtype == "Link" and f.options:
                    joins.append({
                        "left_table": table_name,
                        "left_key": f.fieldname,
                        "right_table": f"tab{f.options}",
                        "right_key": "name",
                        "why": f"{doctype}.{f.fieldname} links to {f.options}.name",
                    })

                elif f.fieldtype == "Table" and f.options:
                    joins.append({
                        "left_table": table_name,
                        "left_key": "name",
                        "right_table": f"tab{f.options}",
                        "right_key": "parent",
                        "why": f"{doctype} is parent for {f.options} (child table).",
                    })

            # --------------------------------------------------------
            # Aliases
            # --------------------------------------------------------
            if display_field:
                aliases[f"{doctype.lower()}_name"] = [table_name, display_field]

            aliases[f"{doctype.lower()}_id"] = [table_name, "name"]

        except Exception as e:
            print(f"> Skipped {doctype}: {e}")
            continue

    print(f"> Processed {len(tables)} DocTypes successfully")
    return tables, joins, aliases, allowlist


# -------------------------------------------------------------------
# Write schema
# -------------------------------------------------------------------

def write_schema(payload: Dict[str, Any]):
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    print(f"> Schema written to: {OUT_PATH}")


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main():
    print("> Generating TAP AI schema with admin exclusions...")

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


# -------------------------------------------------------------------
# Bench CLI
# -------------------------------------------------------------------

def cli():
    """
    bench execute tap_ai.schema.generate_schema.cli
    """
    main()
    return {"status": "success"}


if __name__ == "__main__":
    main()
