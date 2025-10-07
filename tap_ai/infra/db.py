# File: ~/frappe-bench/apps/tap_ai/tap_ai/infra/db.py

from __future__ import annotations
import logging
from typing import Iterable, List, Optional

import frappe
from sqlalchemy import create_engine, text, inspect
from langchain_community.utilities import SQLDatabase

logger = logging.getLogger(__name__)

def _get_mariadb_uri() -> str:
    """Build a SQLAlchemy URI from Frappe's site_config."""
    site = frappe.get_site_config()
    db = site.get("db_name")
    user = site.get("db_user", db)
    pwd = site.get("db_password")
    host = site.get("db_host", "127.0.0.1")
    port = site.get("db_port", 3306)

    if not (db and user and pwd):
        raise RuntimeError("Missing DB credentials in site_config.json")

    return f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}"

def _discover_tables(database_uri: str) -> set:
    eng = create_engine(database_uri)
    try:
        insp = inspect(eng)
        return set(insp.get_table_names())
    finally:
        eng.dispose()

def get_sqldb(
    include_tables: Optional[List[str]] = None,
    sample_rows_in_table_info: int = 2,
) -> SQLDatabase:
    """
    Return a LangChain SQLDatabase with an allowlist that’s validated
    against the live MariaDB. Missing tables are auto-dropped (with a warning).
    """
    database_uri = _get_mariadb_uri()

    # Validate allowlist against actual DB tables
    if include_tables:
        actual = _discover_tables(database_uri)
        missing = sorted(set(include_tables) - actual)
        if missing:
            logger.warning(
                "Dropping %d missing tables from allowlist: %s",
                len(missing), ", ".join(missing)
            )
        include_tables = [t for t in include_tables if t in actual]
        if not include_tables:
            logger.warning("All allowlisted tables were missing; falling back to full visibility.")

    # If list becomes empty after filtering, pass None to expose all (or handle as you prefer)
    safe_include = include_tables if include_tables else None

    return SQLDatabase.from_uri(
        database_uri,
        include_tables=safe_include,
        sample_rows_in_table_info=sample_rows_in_table_info,
    )


