# tap_ai/schema/list_system_doctypes.py

import frappe
from typing import List, Set

# -------------------------------------------------------------------
# System identification logic
# -------------------------------------------------------------------

SYSTEM_PREFIXES = ("__", "_", "Data Import", "Deleted", "Version")
EXCLUDE_MODULES = ("Core", "Desk", "Email", "Printing", "Website", "Workflow")

def is_system_doctype(doctype: str, module: str) -> bool:
    """
    Determine if a doctype is a system doctype that should be excluded.
    """
    # Check prefixes
    if doctype.startswith(SYSTEM_PREFIXES):
        return True

    # Check modules
    if module in EXCLUDE_MODULES:
        return True

    # TAP LMS is the target module, everything else is system
    if module != "TAP LMS":
        return True

    return False

def get_all_system_doctypes() -> List[dict]:
    """
    Get all doctypes that are considered system doctypes.
    Returns list of dicts with doctype name and module.
    """
    all_doctypes = frappe.get_all(
        "DocType",
        fields=["name", "module"],
        filters={"istable": 0}
    )

    system_doctypes = []
    for dt_info in all_doctypes:
        doctype = dt_info.name
        module = dt_info.get("module", "")

        if is_system_doctype(doctype, module):
            system_doctypes.append({
                "doctype": doctype,
                "module": module
            })

    return system_doctypes

def populate_excluded_doctypes():
    """
    Automatically populate the ExcludedDoctypes with all system doctypes.
    """
    system_doctypes = get_all_system_doctypes()

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

        # Add all system doctypes
        for system_dt in system_doctypes:
            doc.append("excluded_doctype", {
                "doctype_name": system_dt["doctype"]
            })

        doc.save()
        frappe.db.commit()

        print(f"✅ Successfully populated ExcludedDoctypes with {len(system_doctypes)} system doctypes")

    except Exception as e:
        print(f"❌ Failed to populate ExcludedDoctypes: {e}")

def list_system_doctypes():
    """
    Print all system doctypes grouped by module.
    """
    system_doctypes = get_all_system_doctypes()

    # Group by module
    by_module = {}
    for dt in system_doctypes:
        module = dt["module"] or "No Module"
        if module not in by_module:
            by_module[module] = []
        by_module[module].append(dt["doctype"])

    print(f"\n📋 Found {len(system_doctypes)} system doctypes to exclude:\n")

    for module in sorted(by_module.keys()):
        doctypes = sorted(by_module[module])
        print(f"🔸 {module} ({len(doctypes)} doctypes):")
        for doctype in doctypes:
            print(f"   - {doctype}")
        print()

    return system_doctypes

# -------------------------------------------------------------------
# Main functions
# -------------------------------------------------------------------

def main():
    """
    List all system doctypes.
    Use: bench execute tap_ai.schema.list_system_doctypes.main
    """
    list_system_doctypes()

def populate():
    """
    Automatically populate ExcludedDoctypes with system doctypes.
    Use: bench execute tap_ai.schema.list_system_doctypes.populate
    """
    print("🔄 Populating ExcludedDoctypes with system doctypes...")
    populate_excluded_doctypes()

if __name__ == "__main__":
    main()