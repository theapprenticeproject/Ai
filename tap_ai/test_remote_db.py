import frappe
import psycopg2
from psycopg2.extras import RealDictCursor

def test_connection():
    print("\n> Starting comprehensive remote database test...")
    
    host = frappe.conf.get("remote_db_host", "127.0.0.1")
    port = frappe.conf.get("remote_db_port", 5433)
    db_name = frappe.conf.get("remote_db_name")
    user = frappe.conf.get("remote_db_user")
    password = frappe.conf.get("remote_db_password")

    try:
        # Establish connection
        conn = psycopg2.connect(
            host=host, port=port, dbname=db_name, user=user, password=password
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        print("✅ CONNECTION SUCCESSFUL!\n")
        
        # --- SECTION 1: USER DATA ---
        print("👤 Fetching 5 Users...")
        cursor.execute('SELECT name, first_name, email FROM "tabUser" LIMIT 5;')
        users = cursor.fetchall()
        for row in users:
            print(f"   - {dict(row)}")
            
        # --- SECTION 2: TABLE COUNT ---
        cursor.execute('SELECT COUNT(*) as total FROM "tabDocType" WHERE istable = 0;')
        total_count = cursor.fetchone()
        print(f"\n📊 Total standard DocTypes (Tables) in system: {total_count['total']}")
        
        # --- SECTION 3: LMS TABLES ---
        print("\n📚 Finding LMS and Educational DocTypes...")
        query = """
            SELECT name, module 
            FROM "tabDocType" 
            WHERE module = 'LMS' OR name ILIKE '%course%' OR name ILIKE '%video%' 
            LIMIT 10;
        """
        cursor.execute(query)
        lms_tables = cursor.fetchall()
        for row in lms_tables:
            print(f"   - {row['name']} (Module: {row['module']})")
            
    except Exception as e:
        print(f"\n❌ CONNECTION FAILED: {e}")
        
    finally:
        if 'conn' in locals():
            conn.close()
            print("\n> Database connection closed.")