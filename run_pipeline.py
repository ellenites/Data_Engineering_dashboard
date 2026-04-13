#!/usr/bin/env python3
"""
End-to-end ETL: extract NSLDS + WDI, transform, load SQLite, validate.
Run from project root: python run_pipeline.py
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from clean_transform import clean_wdi, parse_nslsd_portfolio_by_loan_type
from config import DATA_PROCESSED
from extract import download_nslsd_portfolio, resolve_wdi_dataframe
from load_db import load_dataframes
from validate import all_passed, run_validations


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    loans_path = download_nslsd_portfolio(force=False)
    loans = parse_nslsd_portfolio_by_loan_type(loans_path)

    wdi_raw, provenance = resolve_wdi_dataframe(prefer_api=True)
    wdi = clean_wdi(wdi_raw)
    logging.info("WDI provenance: %s", provenance)

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    loans.to_csv(DATA_PROCESSED / "student_loan_portfolio.csv", index=False)
    wdi.to_csv(DATA_PROCESSED / "wdi_indicators.csv", index=False)

    load_dataframes(loans, wdi, reset=True)
    results = run_validations()
    lines = ["Validation report\n" + "=" * 40]
    for r in results:
        lines.append(f"[{'PASS' if r.passed else 'FAIL'}] {r.name}: {r.detail}")
    (DATA_PROCESSED / "validation_report.txt").write_text("\n".join(lines), encoding="utf-8")

    if not all_passed(results):
        logging.error("One or more validation checks reported failure.")
        return 1
    logging.info("Pipeline completed successfully. DB at data/pipeline.db")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
