# Copyright (c) 2023, TAP Innovations Incorporated and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.utils import flt

def execute(filters=None):
    columns = get_columns()
    data = get_data(filters)
    chart = get_chart(data)
    summary = get_summary(data)
    
    return columns, data, None, chart, summary

def get_columns():
    return [
        {
            "fieldname": "student_name",
            "label": _("Student Name"),
            "fieldtype": "Data",
            "width": 200
        },
        {
            "fieldname": "phone",
            "label": _("Phone"),
            "fieldtype": "Data",
            "width": 120
        },
        {
            "fieldname": "set_name",
            "label": _("Onboarding Set"),
            "fieldtype": "Link",
            "options": "Backend Student Onboarding",
            "width": 180
        },
        {
            "fieldname": "stage_name",
            "label": _("Stage"),
            "fieldtype": "Link",
            "options": "OnboardingStage",
            "width": 180
        },
        {
            "fieldname": "status",
            "label": _("Status"),
            "fieldtype": "Data",
            "width": 120
        },
        {
            "fieldname": "start_timestamp",
            "label": _("Start Date"),
            "fieldtype": "Datetime",
            "width": 160
        },
        {
            "fieldname": "last_activity_timestamp",
            "label": _("Last Activity"),
            "fieldtype": "Datetime",
            "width": 160
        },
        {
            "fieldname": "completion_timestamp",
            "label": _("Completion Date"),
            "fieldtype": "Datetime",
            "width": 160
        }
    ]

def get_data(filters):
    # Build filter conditions
    conditions = ["ssp.stage_type = 'OnboardingStage'"]
    
    if filters.get("set"):
        conditions.append("bs.parent = %(set)s")
    
    if filters.get("stage"):
        conditions.append("ssp.stage = %(stage)s")
    
    if filters.get("status"):
        conditions.append("ssp.status = %(status)s")
    
    where_clause = " AND ".join(conditions)
    
    # Query for data using JOIN with Backend Students to get the set
    result = frappe.db.sql("""
        SELECT 
            s.name as student_id,
            s.name1 as student_name,
            s.phone as phone,
            bs.parent as set_name,
            bso.set_name as set_display_name,
            ssp.stage as stage_name,
            ssp.status as status,
            ssp.start_timestamp as start_timestamp,
            ssp.last_activity_timestamp as last_activity_timestamp,
            ssp.completion_timestamp as completion_timestamp
        FROM 
            `tabStudentStageProgress` ssp
        JOIN
            `tabStudent` s ON ssp.student = s.name
        LEFT JOIN
            `tabBackend Students` bs ON bs.student_id = s.name
        LEFT JOIN
            `tabBackend Student Onboarding` bso ON bs.parent = bso.name
        WHERE
            {where_clause}
        ORDER BY
            s.name1, ssp.stage
    """.format(where_clause=where_clause), filters, as_dict=1)
    
    return result

def get_chart(data):
    # Count stages by status
    status_counts = {
        "not_started": 0,
        "assigned": 0,
        "in_progress": 0,
        "completed": 0,
        "incomplete": 0,
        "skipped": 0
    }
    
    for row in data:
        status = row.get("status")
        if status in status_counts:
            status_counts[status] += 1
    
    # Only include statuses with counts > 0
    labels = []
    values = []
    colors = []
    
    status_colors = {
        "not_started": "#F8F9FA",  # light gray
        "assigned": "#17A2B8",     # info blue
        "in_progress": "#FFC107",  # warning yellow
        "completed": "#28A745",    # success green
        "incomplete": "#DC3545",   # danger red
        "skipped": "#6C757D"       # secondary gray
    }
    
    for status, count in status_counts.items():
        if count > 0:
            labels.append(_(status.replace("_", " ").title()))
            values.append(count)
            colors.append(status_colors.get(status))
    
    if not values:
        return None
    
    chart = {
        "type": "donut",
        "data": {
            "labels": labels,
            "datasets": [
                {
                    "values": values,
                    "colors": colors
                }
            ]
        },
        "height": 300,
        "options": {
            "title": {
                "text": _("Status Distribution")
            }
        }
    }
    
    return chart

def get_summary(data):
    # Count stages by status for summary
    status_counts = {
        "total": len(data),
        "not_started": 0,
        "assigned": 0,
        "in_progress": 0,
        "completed": 0,
        "incomplete": 0,
        "skipped": 0
    }
    
    for row in data:
        status = row.get("status")
        if status in status_counts:
            status_counts[status] += 1
    
    # Calculate completion rate
    completion_rate = 0
    if status_counts["total"] > 0:
        completion_rate = (status_counts["completed"] / status_counts["total"]) * 100
    
    # Build summary
    summary = [
        {
            "label": _("Total Students"),
            "value": status_counts["total"],
            "indicator": "blue"
        },
        {
            "label": _("Completed"),
            "value": status_counts["completed"],
            "indicator": "green"
        },
        {
            "label": _("In Progress"),
            "value": status_counts["in_progress"],
            "indicator": "orange"
        },
        {
            "label": _("Assigned"),
            "value": status_counts["assigned"],
            "indicator": "blue"
        },
        {
            "label": _("Incomplete"),
            "value": status_counts["incomplete"],
            "indicator": "red"
        },
        {
            "label": _("Completion Rate"),
            "value": f"{flt(completion_rate, 2)}%",
            "indicator": "green" if completion_rate >= 70 else "orange" if completion_rate >= 40 else "red"
        }
    ]
    
    return summary
