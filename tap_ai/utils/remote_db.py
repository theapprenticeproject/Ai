# tap_ai/utils/remote_db.py
"""
Remote Database Connection Utilities with Connection Pooling

Provides connection management and query execution for the remote PostgreSQL database.
Used by SQL answerer and RAG answerer for data fetching.

 OPTIMIZATION: Connection pooling for 3-5x throughput (Phase 2)
"""

import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import List, Dict, Any, Optional
import frappe


class RemoteDBConnectionPool:
    """ Connection pool manager for remote PostgreSQL database (Phase 2)"""

    _instance = None
    _pool = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_pool(self):
        """Get or create connection pool"""
        if self._pool is None:
            self._pool = self._create_pool()
        return self._pool

    def _create_pool(self):
        """Create connection pool"""
        try:
            host = frappe.conf.get("remote_db_host", "127.0.0.1")
            port = frappe.conf.get("remote_db_port", 5433)
            db_name = frappe.conf.get("remote_db_name")
            user = frappe.conf.get("remote_db_user")
            password = frappe.conf.get("remote_db_password")

            if not all([host, port, db_name, user, password]):
                raise ValueError("Missing remote database configuration")

            # Pool size: min=5, max=20 (tunable via config)
            min_conn = int(frappe.conf.get("remote_db_pool_min", 5))
            max_conn = int(frappe.conf.get("remote_db_pool_max", 20))

            pool = psycopg2.pool.SimpleConnectionPool(
                min_conn,
                max_conn,
                host=host,
                port=port,
                dbname=db_name,
                user=user,
                password=password,
                connect_timeout=10,
            )

            print(f"✅ Remote DB pool created: {min_conn}-{max_conn} connections")
            return pool

        except Exception as e:
            try:
                frappe.log_error(f"Remote database pool creation failed: {e}")
            except AttributeError:
                print(f"Remote database pool creation failed: {e}")
            raise

    def close_all(self):
        """Close all connections in pool"""
        if self._pool:
            self._pool.closeall()
            self._pool = None

    @contextmanager
    def get_connection(self, timeout: int = 10):
        """Context manager for connection retrieval"""
        conn = None
        try:
            pool = self.get_pool()
            conn = pool.getconn()
            conn.set_isolation_level(0)  # Autocommit mode
            yield conn
        except psycopg2.pool.PoolError as e:
            try:
                frappe.log_error(f"Connection pool exhausted: {e}")
            except AttributeError:
                print(f"Connection pool exhausted: {e}")
            raise Exception("Database connection limit exceeded. Try again later.")
        except Exception as e:
            if conn:
                try:
                    pool.putconn(conn, close=True)
                except:
                    pass
            raise
        finally:
            if conn:
                try:
                    pool.putconn(conn)
                except:
                    pass


# Global pool instance
_remote_db_pool = RemoteDBConnectionPool()


def get_remote_connection():
    """Get remote database connection from pool"""
    return _remote_db_pool.get_pool().getconn()


def execute_remote_query(sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """
    Execute SQL query on remote database with connection pooling

    Args:
        sql: SQL query string
        params: Query parameters

    Returns:
        List of result dictionaries
    """
    try:
        with _remote_db_pool.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            try:
                if params is None or len(params) == 0:
                    cursor.execute(sql)
                else:
                    cursor.execute(sql, params)
                results = cursor.fetchall()
                return [dict(row) for row in results]
            finally:
                cursor.close()

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
    """Close all remote database connections"""
    _remote_db_pool.close_all()