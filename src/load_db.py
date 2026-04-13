"""Create SQLite schema and load transformed tables."""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd

from config import DB_PATH, SQL_DIR

logger = logging.getLogger(__name__)


def init_schema(conn: sqlite3.Connection) -> None:
    schema_file = SQL_DIR / "schema.sql"
    sql = schema_file.read_text(encoding="utf-8")
    conn.executescript(sql)
    conn.commit()


def load_dataframes(loans: pd.DataFrame, wdi: pd.DataFrame, reset: bool = True) -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if reset and DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    try:
        init_schema(conn)
        if not loans.empty:
            loans.to_sql("student_loan_portfolio", conn, if_exists="append", index=False)
        if not wdi.empty:
            keep = [
                c
                for c in [
                    "country_code",
                    "country_name",
                    "indicator_code",
                    "indicator_name",
                    "year",
                    "value",
                    "log_gdp_per_capita",
                ]
                if c in wdi.columns
            ]
            wdi[keep].to_sql("wdi_indicators", conn, if_exists="append", index=False)
        conn.commit()
        logger.info("SQLite database written to %s", DB_PATH)
    finally:
        conn.close()
