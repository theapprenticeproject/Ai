import frappe
from tap_ai.utils.remote_db import execute_remote_query

def test_connection():
    print("\n> Starting comprehensive remote database test...")

    try:
        # Test basic connection
        results = execute_remote_query("SELECT 1 as test")
        print("✅ CONNECTION SUCCESSFUL!\n")

        # --- SECTION 1: USER DATA ---
        print("👤 Fetching 5 Users...")
        users = execute_remote_query('SELECT name, first_name, email FROM "tabUser" LIMIT 5;')
        for row in users:
            print(f"   - {dict(row)}")

        # --- SECTION 2: TABLE COUNT ---
        total_count = execute_remote_query('SELECT COUNT(*) as total FROM "tabDocType" WHERE istable = 0;')
        print(f"\n📊 Total standard DocTypes (Tables) in system: {total_count[0]['total']}")

        # --- SECTION 3: LMS TABLES ---
        print("\n📚 Finding LMS and Educational DocTypes...")
        lms_tables = execute_remote_query("""
            SELECT name, module
            FROM "tabDocType"
            WHERE module = 'LMS' OR name ILIKE '%course%' OR name ILIKE '%video%'
            LIMIT 10;
        """)
        for row in lms_tables:
            print(f"   - {row['name']} (Module: {row['module']})")

    except Exception as e:
        print(f"\n❌ CONNECTION FAILED: {e}")

    finally:
        print("\n> Test completed.")

# To run this test, execute: `bench execute tap_ai.test_remote_db.test_connection`        