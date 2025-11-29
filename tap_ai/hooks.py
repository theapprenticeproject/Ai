app_name = "tap_ai"
app_title = "Tap Ai"
app_publisher = "Anish Aman"
app_description = "LMS system for tap"
app_email = "tech4dev@gmail.com"
app_license = "MIT"



doc_events = {
    "AI Integration Config": {
        "validate": "tap_ai.utils.dynamic_config.validate_config_consistency",
        "on_update": "tap_ai.utils.dynamic_config.on_config_update"
    }
}



