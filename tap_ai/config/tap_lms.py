# apps/your_custom_app/your_custom_app/config/your_custom_module.py

from frappe import _

def get_data():
    return [
        {
            "label": _("School"),
            "items": [
                {
                    "type": "doctype",
                    "name": "School",
                    "label": _("School"),
                    "description": _("Manage School"),
                    "onboard": 1,
                }
            ]
        }
    ]
