#!/usr/bin/env python3
"""
Simple test script for remote database connection
"""

import sys
import os

# Add the tap_ai directory to Python path
sys.path.insert(0, os.path.dirname(__file__))

try:
    # Mock frappe configuration for testing
    class MockConf:
        def get(self, key, default=None):
            # Mock configuration values
            config = {
                "remote_db_host": "127.0.0.1",
                "remote_db_port": 5433,
                "remote_db_name": "tap_ai_db",
                "remote_db_user": "tap_ai_user",
                "remote_db_password": "tap_ai_password"
            }
            return config.get(key, default)

    import frappe
    frappe.conf = MockConf()

    # Test the remote database connection
    from tap_ai.utils.remote_db import get_remote_connection, execute_remote_query

    print("Testing remote database connection...")

    # Try to get connection
    conn = get_remote_connection()
    print("✅ Connection established successfully")

    # Try a simple query
    results = execute_remote_query("SELECT 1 as test")
    print(f"✅ Query executed successfully: {results}")

    print("🎉 All tests passed!")

except Exception as e:
    print(f"❌ Test failed: {e}")
    import traceback
    traceback.print_exc()