# tap_ai/utils/remote_db.py
"""
Remote Database Connection Utilities

Provides connection management and query execution for the remote PostgreSQL database.
Used by SQL answerer and RAG answerer for data fetching.
"""

import os
import threading

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional
import frappe
from loguru import logger

_POOL = None
_POOL_PID = None
_POOL_LOCK = threading.Lock()


class _PooledConnectionHandle:
    def __init__(self, db_pool: pool.ThreadedConnectionPool, conn):
        self._pool = db_pool
        self._conn = conn
        self._returned = False

    def __getattr__(self, item):
        return getattr(self._conn, item)

    def _return(self) -> None:
        if not self._returned:
            try:
                self._pool.putconn(self._conn)
            finally:
                self._returned = True

    def close(self) -> None:
        self._return()

    def __enter__(self):
        return self._conn

    def __exit__(self, exc_type, exc, tb):
        self._return()


def _get_connection_config() -> Dict[str, Any]:
    host = frappe.conf.get("remote_db_host", "127.0.0.1")
    port = frappe.conf.get("remote_db_port", 5433)
    db_name = frappe.conf.get("remote_db_name")
    user = frappe.conf.get("remote_db_user")
    password = frappe.conf.get("remote_db_password")

    if not all([host, port, db_name, user, password]):
        raise ValueError("Missing remote database configuration")

    query_timeout_ms = int(frappe.conf.get("remote_db_query_timeout_ms", 30000) or 30000)
    return {
        "host": host,
        "port": port,
        "dbname": db_name,
        "user": user,
        "password": password,
        "connect_timeout": 10,
        "options": f"-c statement_timeout={query_timeout_ms}",
    }


def _close_pool() -> None:
    global _POOL
    if _POOL is not None:
        try:
            _POOL.closeall()
        except Exception:
            pass
        _POOL = None


def _create_pool() -> pool.ThreadedConnectionPool:
    cfg = _get_connection_config()
    minconn = int(frappe.conf.get("remote_db_pool_min", 2) or 2)
    maxconn = int(frappe.conf.get("remote_db_pool_max", 10) or 10)
    if minconn < 1:
        minconn = 1
    if maxconn < minconn:
        maxconn = minconn

    return pool.ThreadedConnectionPool(minconn=minconn, maxconn=maxconn, **cfg)


def _get_pool() -> pool.ThreadedConnectionPool:
    global _POOL, _POOL_PID

    pid = os.getpid()
    if _POOL is not None and _POOL_PID != pid:
        _close_pool()
        _POOL_PID = None

    if _POOL is None:
        with _POOL_LOCK:
            if _POOL is None:
                try:
                    _POOL = _create_pool()
                    _POOL_PID = pid
                    logger.info("Remote database connection pool established")
                except Exception as e:
                    try:
                        frappe.log_error(f"Remote database pool creation failed: {e}")
                    except AttributeError:
                        pass
                    logger.error(f"Remote database pool creation failed: {e}")
                    raise

    return _POOL


def get_remote_connection():
    """Get a pooled remote DB connection handle (supports direct use and with-context)."""
    db_pool = _get_pool()
    conn = db_pool.getconn()
    return _PooledConnectionHandle(db_pool, conn)


def execute_remote_query(sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """
    Execute SQL query on remote database

    Args:
        sql: SQL query string
        params: Query parameters

    Returns:
        List of result dictionaries
    """
    conn = None
    db_pool = None
    try:
        db_pool = _get_pool()
        conn = db_pool.getconn()
        max_rows = int(frappe.conf.get("remote_db_max_rows", 1000) or 1000)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            if params is None or len(params) == 0:
                cursor.execute(sql)
            else:
                cursor.execute(sql, params)
            results = cursor.fetchmany(max_rows + 1)

        if len(results) > max_rows:
            logger.warning(f"Query returned >{max_rows} rows — truncating. SQL: {sql[:200]}")
            results = results[:max_rows]

        return [dict(row) for row in results]

    except Exception as e:
        logger.error(f"Remote query execution failed: {e} | SQL: {sql}")
        try:
            frappe.log_error(f"Remote query execution failed: {e}\nSQL: {sql}")
        except AttributeError:
            pass
        raise Exception(f"Remote database query failed: {str(e)}")
    finally:
        if db_pool is not None and conn is not None:
            try:
                db_pool.putconn(conn)
            except Exception:
                pass


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
        logger.error(f"Failed to get columns for table {table}: {e}")
        try:
            frappe.log_error(f"Failed to get columns for table {table}: {e}")
        except AttributeError:
            pass
        return []


def close_remote_connection():
    """Close all pooled remote database connections"""
    with _POOL_LOCK:
        _close_pool()