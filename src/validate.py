"""Post-load validation checks (counts, nulls, referential sanity)."""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass

from config import DB_PATH

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    name: str
    passed: bool
    detail: str


def run_validations() -> list[ValidationResult]:
    results: list[ValidationResult] = []
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM student_loan_portfolio")
        n_loans = cur.fetchone()[0]
        results.append(
            ValidationResult(
                "student_loan_portfolio rowcount",
                n_loans > 0,
                f"rows={n_loans}",
            )
        )

        cur.execute("SELECT COUNT(*) FROM student_loan_portfolio WHERE value IS NULL")
        nulls = cur.fetchone()[0]
        results.append(
            ValidationResult(
                "loan values mostly populated",
                nulls < max(1, n_loans * 0.05),
                f"null value rows={nulls} of {n_loans}",
            )
        )

        cur.execute("SELECT COUNT(*) FROM wdi_indicators")
        n_wdi = cur.fetchone()[0]
        results.append(
            ValidationResult(
                "wdi_indicators rowcount",
                n_wdi > 0,
                f"rows={n_wdi}",
            )
        )

        cur.execute(
            """
            SELECT indicator_code, COUNT(DISTINCT country_code), MIN(year), MAX(year)
            FROM wdi_indicators
            GROUP BY indicator_code
            """
        )
        for row in cur.fetchall():
            results.append(
                ValidationResult(
                    f"wdi coverage {row[0]}",
                    True,
                    f"countries={row[1]} year_range={row[2]}..{row[3]}",
                )
            )
    finally:
        conn.close()

    for r in results:
        logger.info("[%s] %s: %s", "OK" if r.passed else "FAIL", r.name, r.detail)
    return results


def all_passed(results: list[ValidationResult]) -> bool:
    return all(r.passed for r in results)
