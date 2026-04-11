# tap_ai/schema/generate_schema.py

import os
import json
import re
import frappe
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Any, Tuple, Set, Optional

# Import remote database utilities
from tap_ai.utils.remote_db import get_remote_connection

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


def list_system_doctypes() -> List[dict]:
    """
    List all doctypes that would be considered system doctypes.
    Fetches from remote database instead of local Frappe instance.
    Returns list of dicts with doctype name and module.
    """
    # Query remote database for DocTypes
    sql = """
    SELECT name, module
    FROM "tabDocType"
    WHERE istable = 0
    ORDER BY name
    """

    try:
        with get_remote_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql)
                all_doctypes = cursor.fetchall()
    except Exception as e:
        print(f"❌ Failed to fetch DocTypes from remote database: {e}")
        return [], []

    system_doctypes = []
    tap_lms_doctypes = []

    print(f"🔍 Analyzing {len(all_doctypes)} doctypes from remote database...")

    # Debug: collect all unique modules
    all_modules = set()
    for dt_info in all_doctypes:
        module = dt_info.get("module", "")
        all_modules.add(module)

    print(f"📋 Found modules: {sorted(all_modules)}")

    for dt_info in all_doctypes:
        doctype = dt_info["name"]
        module = dt_info.get("module", "")

        # Check if it's a system doctype
        is_system = False
        if doctype.startswith(SYSTEM_PREFIXES):
            is_system = True
        elif module in EXCLUDE_MODULES:
            is_system = True
        elif module.upper() not in ["TAP LMS"]:  # Check for TAP LMS (case insensitive)
            is_system = True

        if is_system:
            system_doctypes.append({
                "doctype": doctype,
                "module": module
            })
        else:
            tap_lms_doctypes.append({
                "doctype": doctype,
                "module": module
            })

    print(f"📊 System doctypes: {len(system_doctypes)}, TAP LMS doctypes: {len(tap_lms_doctypes)}")

    # Debug: show TAP LMS doctypes found
    if tap_lms_doctypes:
        print("🎯 TAP LMS doctypes identified:")
        for dt in tap_lms_doctypes:
            print(f"   + {dt['doctype']} (module: '{dt['module']}')")

    return system_doctypes, tap_lms_doctypes


def populate_excluded_doctypes():
    """
    Automatically populate ExcludedDoctypes with all system doctypes.
    Only excludes actual system doctypes, NOT TAP LMS doctypes.
    """
    system_doctypes, tap_lms_doctypes = list_system_doctypes()

    # Filter out TAP LMS doctypes from system_doctypes
    actual_system_doctypes = []
    for dt in system_doctypes:
        if dt["module"].upper() != "TAP LMS":
            actual_system_doctypes.append(dt)

    print(f"📊 Found {len(actual_system_doctypes)} actual system doctypes and {len(tap_lms_doctypes)} TAP LMS doctypes")

    # Additional TAP LMS DocTypes to exclude (settings, logging, communication, etc.)
    additional_exclusions = [
        # Settings & Configuration
        'API Key', 'GCS Settings', 'Glific Settings', 'Glific Sync Settings',
        'RabbitMQ Settings', 'SwiftChat Settings', 'WhatsApp API Settings', 'Gupshup OTP Settings',

        # Onboarding & Internal Processes
        'Backend Student Onboarding', 'Batch onboarding', 'StudentOnboardingProgress', 'OnboardingStage',

        # Logging & Analytics
        'InteractionLog', 'StudentContentLog', 'StudentStageProgress', 'Weekly Student Flow',
        'WeeklyTeacherFlow', 'ImpactMetrics', 'EngagementState', 'ModalityEffectiveness',

        # Communication & Integration
        'Glific Field Mapping', 'Glific Flow', 'Glific Teacher Group', 'GlificContactGroup',
        'StudentFeedbackChannel', 'Feedback Request', 'Issue Tracker', 'OTP Verification',

        # Specialized/Utility
        'Tap Models', 'Teacher Batch History', 'TransitionHistory', 'SDG Goal',
        'Grade Course Level Mapping', 'Stage Grades'
    ]

    # Combine system doctypes with additional TAP LMS exclusions
    all_exclusions = actual_system_doctypes + [{"doctype": dt, "module": "TAP LMS"} for dt in additional_exclusions]

    print(f"📋 Total exclusions: {len(all_exclusions)} ({len(actual_system_doctypes)} system + {len(additional_exclusions)} TAP LMS)")

    # Get or create ExcludedDoctypes record
    try:
        records = frappe.get_all("ExcludedDoctypes", fields=["name"], limit=1)
        if records:
            # Delete existing record to avoid row conflicts
            frappe.delete_doc("ExcludedDoctypes", records[0].name)
            frappe.db.commit()

        # Create new ExcludedDoctypes record
        doc = frappe.new_doc("ExcludedDoctypes")

        # Add all exclusions
        for exclusion in all_exclusions:
            doc.append("excluded_doctype", {
                "doctype_name": exclusion["doctype"]
            })

        doc.insert()
        frappe.db.commit()

        print(f"✅ Successfully populated ExcludedDoctypes with {len(all_exclusions)} total exclusions")

        # Show TAP LMS doctypes that will be included
        included_tap_lms = [dt for dt in tap_lms_doctypes if dt["doctype"] not in additional_exclusions]
        print(f"\n🎯 TAP LMS doctypes that will be included ({len(included_tap_lms)}):")
        for dt in sorted(included_tap_lms, key=lambda x: x["doctype"]):
            print(f"   + {dt['doctype']}")

    except Exception as e:
        print(f"❌ Failed to populate ExcludedDoctypes: {e}")
        import traceback
        traceback.print_exc()


def get_remote_doctype_meta(doctype: str) -> Optional[Dict[str, Any]]:
    """
    Get DocType metadata from remote database.
    Equivalent to frappe.get_meta(doctype) but for remote database.
    """
    try:
        with get_remote_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Get DocType basic info
                doctype_sql = """
                SELECT title_field, module
                FROM "tabDocType"
                WHERE name = %s
                """
                cursor.execute(doctype_sql, (doctype,))
                doctype_row = cursor.fetchone()

                if not doctype_row:
                    return None

                # Get DocField information
                fields_sql = """
                SELECT fieldname, fieldtype, options, label
                FROM "tabDocField"
                WHERE parent = %s
                ORDER BY idx
                """
                cursor.execute(fields_sql, (doctype,))
                fields_rows = cursor.fetchall()

                # Convert to similar format as frappe.get_meta()
                fields = []
                for row in fields_rows:
                    fields.append({
                        "fieldname": row["fieldname"],
                        "fieldtype": row["fieldtype"],
                        "options": row["options"],
                        "label": row["label"]
                    })

                return {
                    "title_field": doctype_row["title_field"],
                    "module": doctype_row["module"],
                    "fields": fields
                }

    except Exception as e:
        print(f"❌ Failed to get metadata for {doctype}: {e}")
        return None
    """
    Automatically populate ExcludedDoctypes with all system doctypes.
    Only excludes actual system doctypes, NOT TAP LMS doctypes.
    """
    system_doctypes, tap_lms_doctypes = list_system_doctypes()

    # Filter out TAP LMS doctypes from system_doctypes
    actual_system_doctypes = []
    for dt in system_doctypes:
        if dt["module"].upper() != "TAP LMS":
            actual_system_doctypes.append(dt)

    print(f"📊 Found {len(actual_system_doctypes)} actual system doctypes and {len(tap_lms_doctypes)} TAP LMS doctypes")

    # Get or create ExcludedDoctypes record
    try:
        records = frappe.get_all("ExcludedDoctypes", fields=["name"], limit=1)
        if records:
            doc = frappe.get_doc("ExcludedDoctypes", records[0].name)
        else:
            doc = frappe.new_doc("ExcludedDoctypes")
            doc.insert()
            frappe.db.commit()

        # Clear existing entries
        doc.excluded_doctype = []

        # Add only actual system doctypes (not TAP LMS)
        for system_dt in actual_system_doctypes:
            doc.append("excluded_doctype", {
                "doctype_name": system_dt["doctype"]
            })

        doc.save()
        frappe.db.commit()

        print(f"✅ Successfully populated ExcludedDoctypes with {len(actual_system_doctypes)} system doctypes")

        # Show TAP LMS doctypes that will be included
        print(f"\n🎯 TAP LMS doctypes that will be included ({len(tap_lms_doctypes)}):")
        for dt in sorted(tap_lms_doctypes, key=lambda x: x["doctype"]):
            print(f"   + {dt['doctype']}")

    except Exception as e:
        print(f"❌ Failed to populate ExcludedDoctypes: {e}")


# -------------------------------------------------------------------
# Core discovery logic
# -------------------------------------------------------------------

def discover() -> Tuple[Dict, List[Dict], Dict, Dict]:
    """
    Discover ALL DocTypes from remote database,
    excluding system, module-based, and admin-defined doctypes.
    """

    tables: Dict[str, Dict] = {}
    joins: List[Dict] = []
    aliases: Dict[str, List[str]] = {}
    allowlist: Dict[str, bool] = {}

    excluded_doctypes = get_excluded_doctypes()

    # Query remote database for DocTypes
    sql = """
    SELECT name, module
    FROM "tabDocType"
    WHERE istable = 0
    ORDER BY name
    """

    try:
        with get_remote_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql)
                all_doctypes = cursor.fetchall()
    except Exception as e:
        print(f"❌ Failed to fetch DocTypes from remote database: {e}")
        return {}, [], {}, {}

    print(f"> Found {len(all_doctypes)} DocTypes in remote database")
    print(f"> Excluding {len(excluded_doctypes)} doctypes via ExcludedDoctypes")

    for dt_info in all_doctypes:
        doctype = dt_info["name"]
        module = dt_info.get("module", "")

        # ------------------------------------------------------------
        # Exclusion rules (Note: TAP LMS doctypes are NOT excluded here - they pass through to ExcludedDoctypes check only)
        # ------------------------------------------------------------
        if doctype in excluded_doctypes:
            continue
        if doctype.startswith(SYSTEM_PREFIXES):
            continue
        if module in EXCLUDE_MODULES:
            continue

        try:
            # Get DocType metadata from remote database
            meta = get_remote_doctype_meta(doctype)
            if not meta:
                print(f"> Skipped {doctype}: Could not get metadata")
                continue

            table_name = f"tab{doctype}"

            # --------------------------------------------------------
            # Columns
            # --------------------------------------------------------
            columns = ["name"]
            display_field = meta.get("title_field")

            for field in meta.get("fields", []):
                fieldname = field.get("fieldname")
                fieldtype = field.get("fieldtype")

                if fieldname and fieldtype not in (
                    "Section Break",
                    "Column Break",
                    "Tab Break",
                    "HTML",
                ):
                    columns.append(fieldname)

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
            for field in meta.get("fields", []):
                fieldtype = field.get("fieldtype")
                fieldname = field.get("fieldname")
                options = field.get("options")

                if fieldtype == "Link" and options:
                    joins.append({
                        "left_table": table_name,
                        "left_key": fieldname,
                        "right_table": f"tab{options}",
                        "right_key": "name",
                        "why": f"{doctype}.{fieldname} links to {options}.name",
                    })

                elif fieldtype == "Table" and options:
                    joins.append({
                        "left_table": table_name,
                        "left_key": "name",
                        "right_table": f"tab{options}",
                        "right_key": "parent",
                        "why": f"{doctype} is parent for {options} (child table).",
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
    print("> Generating TAP AI schema from remote database with admin exclusions...")

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


def cli_list_system():
    """
    List all system doctypes that will be excluded.
    bench execute tap_ai.schema.generate_schema.cli_list_system
    """
    system_doctypes, tap_lms_doctypes = list_system_doctypes()

    # Filter out TAP LMS doctypes from system_doctypes for accurate count
    actual_system_doctypes = []
    for dt in system_doctypes:
        if dt["module"].upper() != "TAP LMS":
            actual_system_doctypes.append(dt)

    print(f"\n📋 System doctypes to exclude ({len(actual_system_doctypes)}):\n")

    # Group by module
    by_module = {}
    for dt in actual_system_doctypes:
        module = dt["module"] or "No Module"
        if module not in by_module:
            by_module[module] = []
        by_module[module].append(dt["doctype"])

    for module in sorted(by_module.keys()):
        doctypes = sorted(by_module[module])
        print(f"🔸 {module} ({len(doctypes)} doctypes):")
        for doctype in doctypes[:10]:  # Show first 10
            print(f"   - {doctype}")
        if len(doctypes) > 10:
            print(f"   ... and {len(doctypes) - 10} more")
        print()

    print(f"🎯 TAP LMS doctypes to include ({len(tap_lms_doctypes)}):")
    for dt in sorted(tap_lms_doctypes, key=lambda x: x["doctype"]):
        print(f"   + {dt['doctype']}")

    return {"system_doctypes": len(actual_system_doctypes), "tap_lms_doctypes": len(tap_lms_doctypes)}


def cli_populate_excluded():
    """
    Automatically populate ExcludedDoctypes with all system doctypes.
    bench execute tap_ai.schema.generate_schema.cli_populate_excluded
    """
    populate_excluded_doctypes()
    return {"status": "success"}


if __name__ == "__main__":
    main()
