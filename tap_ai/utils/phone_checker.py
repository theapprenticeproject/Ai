import frappe
import csv
import re
import os

def norm_last10(x):
    """Normalize to last 10 digits"""
    if not x:
        return None
    s = re.sub(r"\D", "", str(x))
    return s[-10:] if s else None

def check_missing_phones(
    input_csv=None,
    include_glific=False,
    out_missing=None,
    out_extras=None
):
    """
    Compare phone_number column in CSV with Student.phone / alt_phone (and optionally glific_id).
    Writes missing and extra numbers into site private/files.

    input_csv   : str -> filename (in private/files) OR full path
    out_missing : str -> filename (in private/files) OR full path
    out_extras  : str -> filename (in private/files) OR full path
    """

    # --- resolve input file ---
    if input_csv:
        if not os.path.isabs(input_csv):  
            input_csv = frappe.get_site_path("private", "files", input_csv)
    else:
        input_csv = frappe.get_site_path("private", "files", "phones.csv")

    # --- resolve outputs ---
    if out_missing:
        if not os.path.isabs(out_missing):
            out_missing = frappe.get_site_path("private", "files", out_missing)
    else:
        out_missing = frappe.get_site_path("private", "files", "missing_phones_last10.csv")

    if out_extras:
        if not os.path.isabs(out_extras):
            out_extras = frappe.get_site_path("private", "files", out_extras)
    else:
        out_extras = frappe.get_site_path("private", "files", "doctype_only_phones_last10.csv")

    # 1) Load CSV -> set(last10)
    csv_last10 = set()
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "phone_number" not in (reader.fieldnames or []):
            raise Exception(f"CSV must have a 'phone_number' column, found: {reader.fieldnames}")
        for row in reader:
            n = norm_last10(row.get("phone_number"))
            if n:
                csv_last10.add(n)

    # 2) Pull Student phones
    fields = ["name", "phone", "alt_phone"]
    if include_glific:
        fields.append("glific_id")

    existing_last10 = set()
    students = frappe.get_all("Student", fields=fields, limit_page_length=0)
    for s in students:
        for key in ("phone", "alt_phone"):
            n = norm_last10(s.get(key))
            if n:
                existing_last10.add(n)
        if include_glific:
            n = norm_last10(s.get("glific_id"))
            if n:
                existing_last10.add(n)

    # 3) Diff sets
    missing_in_doctype = csv_last10 - existing_last10
    doctype_only = existing_last10 - csv_last10

    # 4) Write outputs
    os.makedirs(os.path.dirname(out_missing), exist_ok=True)
    with open(out_missing, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["missing_phone_last10"])
        for n in sorted(missing_in_doctype):
            w.writerow([n])

    os.makedirs(os.path.dirname(out_extras), exist_ok=True)
    with open(out_extras, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["doctype_only_phone_last10"])
        for n in sorted(doctype_only):
            w.writerow([n])

    print("Done.")
    print(f"CSV phones total (unique last-10): {len(csv_last10)}")
    print(f"Missing in Student: {len(missing_in_doctype)} -> {out_missing}")
    print(f"In Student only:   {len(doctype_only)} -> {out_extras}")

