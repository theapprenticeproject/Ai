from . import __version__ as app_version
from frappe import get_all


app_name = "tap_ai"
app_title = "Tap Ai"
app_publisher = "Anish Aman"
app_description = "LMS system for tap"
app_email = "tech4dev@gmail.com"
app_license = "MIT"


# Document Events
doc_events = {
    "School": {
        "before_save": "tap_ai.tap_ai.doctype.school.school.before_save"
    },
    "Teacher": {
        "on_update": "tap_ai.glific_webhook.update_glific_contact"
    },
    "StudentStageProgress": {
        "after_insert": "tap_ai.tap_ai.doctype.studentonboardingprogress.studentonboardingprogress.update_student_progress",
        "on_update": "tap_ai.tap_ai.doctype.studentonboardingprogress.studentonboardingprogress.update_student_progress"
    }
}

# Scheduled Tasks
scheduler_events = {
    "daily": [
        "tap_ai.tap_ai.page.onboarding_flow_trigger.onboarding_flow_trigger.update_incomplete_stages"
    ]
}

# Page configurations
page_js = {"onboarding-flow-trigger": "public/js/onboarding_flow_trigger.js"}

# Reports
report_script_custom_doctypes = ["StudentStageProgress"]


# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/tap_ai/css/tap_ai.css"
# app_include_js = "/assets/tap_ai/js/tap_ai.js"

# include js, css files in header of web template
# web_include_css = "/assets/tap_ai/css/tap_ai.css"
# web_include_js = "/assets/tap_ai/js/tap_ai.js"

# include custom scss in every website theme (without file extension ".scss")
# website_theme_scss = "tap_ai/public/scss/website"

# include js, css files in header of web form
# webform_include_js = {"doctype": "public/js/doctype.js"}
# webform_include_css = {"doctype": "public/css/doctype.css"}

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
# doctype_js = {"doctype" : "public/js/doctype.js"}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
#       "Role": "home_page"
# }

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Jinja
# ----------

# add methods and filters to jinja environment
# jinja = {
#       "methods": "tap_ai.utils.jinja_methods",
#       "filters": "tap_ai.utils.jinja_filters"
# }

# Installation
# ------------

# before_install = "tap_ai.install.before_install"
# after_install = "tap_ai.install.after_install"

# Uninstallation
# ------------

# before_uninstall = "tap_ai.uninstall.before_uninstall"
# after_uninstall = "tap_ai.uninstall.after_uninstall"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "tap_ai.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
#       "Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
#       "Event": "frappe.desk.doctype.event.event.has_permission",
# }

# DocType Class
# ---------------
# Override standard doctype classes

# override_doctype_class = {
#       "ToDo": "custom_app.overrides.CustomToDo"
# }

# Document Events
# ---------------
# Hook on document methods and events

# doc_events = {
#       "*": {
#               "on_update": "method",
#               "on_cancel": "method",
#               "on_trash": "method"
#       }
# }

# Scheduled Tasks
# ---------------

# scheduler_events = {
#       "all": [
#               "tap_ai.tasks.all"
#       ],
#       "daily": [
#               "tap_ai.tasks.daily"
#       ],
#       "hourly": [
#               "tap_ai.tasks.hourly"
#       ],
#       "weekly": [
#               "tap_ai.tasks.weekly"
#       ],
#       "monthly": [
#               "tap_ai.tasks.monthly"
#       ],
# }

# Testing
# -------

# before_tests = "tap_ai.install.before_tests"

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
#       "frappe.desk.doctype.event.event.get_events": "tap_ai.event.get_events"
# }
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
#       "Task": "tap_ai.task.get_dashboard_data"
# }

# exempt linked doctypes from being automatically cancelled
#
# auto_cancel_exempted_doctypes = ["Auto Repeat"]

# Ignore links to specified DocTypes when deleting documents
# -----------------------------------------------------------

# ignore_links_on_delete = ["Communication", "ToDo"]


# User Data Protection
# --------------------

# user_data_fields = [
#       {
#               "doctype": "{doctype_1}",
#               "filter_by": "{filter_by}",
#               "redact_fields": ["{field_1}", "{field_2}"],
#               "partial": 1,
#       },
#       {
#               "doctype": "{doctype_2}",
#               "filter_by": "{filter_by}",
#               "partial": 1,
#       },
#       {
#               "doctype": "{doctype_3}",
#               "strict": False,
#       },
#       {
#               "doctype": "{doctype_4}"
#       }
# ]

# Authentication and authorization
# --------------------------------

# auth_hooks = [
#       "tap_ai.auth.validate"
# ]

fixtures = [{ "doctype": "Client Script", "filters": [ ["module", "in", ( "Tap ai" )] ] }]
