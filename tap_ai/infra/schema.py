import frappe

def get_remote_db_uri():
    host = frappe.conf.get("remote_db_host")
    db_name = frappe.conf.get("remote_db_name")
    user = frappe.conf.get("remote_db_user")
    password = frappe.conf.get("remote_db_password")
    port = frappe.conf.get("remote_db_port", 5433)
    
    # Standard PostgreSQL Connection URI format
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
