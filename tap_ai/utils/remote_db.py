# tap_ai/utils/remote_db.py
"""
Remote Database Connection Utilities

Provides connection management and query execution for the remote PostgreSQL database.
Used by SQL answerer and RAG answerer for data fetching.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional
import frappe


class RemoteDBConnection:
    """Singleton connection manager for remote PostgreSQL database"""

    _instance = None
    _connection = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_connection(self):
        """Get or create database connection"""
        if self._connection is None or self._connection.closed:
            self._connection = self._create_connection()
        return self._connection

    def _create_connection(self):
        """Create new database connection"""
        try:
            host = frappe.conf.get("remote_db_host", "127.0.0.1")
            port = frappe.conf.get("remote_db_port", 5433)
            db_name = frappe.conf.get("remote_db_name")
            user = frappe.conf.get("remote_db_user")
            password = frappe.conf.get("remote_db_password")

            if not all([host, port, db_name, user, password]):
                raise ValueError("Missing remote database configuration")

            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=db_name,
                user=user,
                password=password
            )
            print("✅ Remote database connection established")
            return conn

        except Exception as e:
            # Handle case where frappe.log_error might not be available
            try:
                frappe.log_error(f"Remote database connection failed: {e}")
            except AttributeError:
                print(f"Remote database connection failed: {e}")
            raise

    def close(self):
        """Close database connection"""
        if self._connection and not self._connection.closed:
            self._connection.close()
            self._connection = None


# Global connection instance
_remote_db = RemoteDBConnection()


def get_remote_connection():
    """Get remote database connection"""
    return _remote_db.get_connection()


def execute_remote_query(sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """
    Execute SQL query on remote database

    Args:
        sql: SQL query string
        params: Query parameters

    Returns:
        List of result dictionaries
    """
    try:
        conn = get_remote_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        if params is None or len(params) == 0:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
        results = cursor.fetchall()

        cursor.close()
        return [dict(row) for row in results]

    except Exception as e:
        # Handle case where frappe.log_error might not be available
        try:
            frappe.log_error(f"Remote query execution failed: {e}\nSQL: {sql}")
        except AttributeError:
            print(f"Remote query execution failed: {e}\nSQL: {sql}")
        raise Exception(f"Remote database query failed: {str(e)}")


def get_remote_all(doctype: str, fields: List[str] = None, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """
    Equivalent to frappe.get_all() but for remote database

    Args:
        doctype: DocType name
        fields: List of fields to select
        filters: Filter conditions

    Returns:
        List of records
    """
    table = f"tab{doctype}"
    fields_str = ", ".join(fields) if fields else "*"

    sql = f"SELECT {fields_str} FROM \"{table}\""

    # Build WHERE clause
    where_conditions = []
    params = []

    if filters:
        for field, value in filters.items():
            if isinstance(value, list) and len(value) == 2:
                # Handle frappe-style filters like ["in", ["value1", "value2"]]
                op, val = value
                if op == "in":
                    placeholders = ", ".join(["%s"] * len(val))
                    where_conditions.append(f"\"{field}\" IN ({placeholders})")
                    params.extend(val)
                elif op == "=":
                    where_conditions.append(f"\"{field}\" = %s")
                    params.append(val)
                elif op == "like":
                    where_conditions.append(f"\"{field}\" LIKE %s")
                    params.append(val)
            else:
                # Simple equality
                where_conditions.append(f"\"{field}\" = %s")
                params.append(value)

    if where_conditions:
        sql += " WHERE " + " AND ".join(where_conditions)

    return execute_remote_query(sql, tuple(params))


def get_remote_table_columns(table: str) -> List[str]:
    """
    Get column names for a table in remote database

    Args:
        table: Table name (without tab prefix)

    Returns:
        List of column names
    """
    try:
        sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
        """
        results = execute_remote_query(sql, (f"tab{table}",))
        return [row["column_name"] for row in results]
    except Exception as e:
        # Handle case where frappe.log_error might not be available
        try:
            frappe.log_error(f"Failed to get columns for table {table}: {e}")
        except AttributeError:
            print(f"Failed to get columns for table {table}: {e}")
        return []


def close_remote_connection():
    """Close remote database connection"""
    _remote_db.close()