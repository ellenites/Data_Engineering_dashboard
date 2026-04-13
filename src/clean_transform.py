"""Clean NSLDS Excel and normalize WDI rows; build derived columns."""
from __future__ import annotations

import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _metric_slug(header: str) -> str:
    h = (header or "").replace("\n", " ").strip().lower()
    if "unique" in h and "recipient" in h:
        return "unique_recipients_mm"
    if "recipient" in h:
        return "recipients_mm"
    if "dollar" in h or "outstanding" in h:
        return "dollars_outstanding_bn"
    return re.sub(r"[^a-z0-9]+", "_", h).strip("_") or "unknown"


def parse_nslsd_portfolio_by_loan_type(path: Path, source_name: str | None = None) -> pd.DataFrame:
    """
    Parse FSA 'Portfolio by Loan Type' XLS layout:
    - Row 5: loan type labels on even columns (2,4,6,...)
    - Row 6: metric labels for each pair of columns
    - Row 7+: fiscal year (col0), quarter (col1), values
    """
    raw = pd.read_excel(path, header=None, engine="xlrd")
    loan_row = raw.iloc[5]
    metric_row = raw.iloc[6]
    data = raw.iloc[7:].copy()
    data.columns = range(raw.shape[1])

    records: list[dict] = []
    for _, row in data.iterrows():
        year = row.get(0)
        quarter = row.get(1)
        if pd.isna(year):
            continue
        try:
            fy = int(float(year))
        except (TypeError, ValueError):
            continue
        q = None if pd.isna(quarter) else str(quarter).strip()

        for col in range(2, raw.shape[1], 2):
            loan = loan_row.get(col)
            if pd.isna(loan) or loan is None:
                continue
            m_a = metric_row.get(col)
            m_b = metric_row.get(col + 1)
            v_a = row.get(col)
            v_b = row.get(col + 1)
            loan_type = str(loan).strip()
            for m_raw, v in ((m_a, v_a), (m_b, v_b)):
                if m_raw is None or (isinstance(m_raw, float) and pd.isna(m_raw)):
                    continue
                metric = _metric_slug(str(m_raw))
                val = np.nan
                if v is not None and not (isinstance(v, float) and pd.isna(v)):
                    try:
                        val = float(v)
                    except (TypeError, ValueError):
                        val = np.nan
                records.append(
                    {
                        "fiscal_year": fy,
                        "quarter": q,
                        "loan_type": loan_type,
                        "metric": metric,
                        "value": val,
                        "source_file": source_name or path.name,
                    }
                )

    df = pd.DataFrame(records)
    if df.empty:
        return df

    # Derived: fiscal period label and total outstanding proxy (sum of dollars by year/quarter)
    df["fiscal_period"] = df["fiscal_year"].astype(str) + "_" + df["quarter"].fillna("NA")
    dollars = df[df["metric"] == "dollars_outstanding_bn"].copy()
    agg = (
        dollars.groupby(["fiscal_year", "quarter"], dropna=False)["value"]
        .sum()
        .reset_index()
        .rename(columns={"value": "total_outstanding_bn_all_types"})
    )
    df = df.merge(agg, on=["fiscal_year", "quarter"], how="left")

    logger.info("NSLDS parsed rows=%s years=%s..%s", len(df), df["fiscal_year"].min(), df["fiscal_year"].max())
    return df


def clean_wdi(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with missing keys; coerce types; add log GDP per capita for charts."""
    out = df.copy()
    required = {"country_code", "indicator_code", "year"}
    missing = required - set(out.columns)
    if missing:
        raise ValueError(f"WDI missing columns: {missing}")
    out = out.dropna(subset=["country_code", "indicator_code", "year"])
    out["country_code"] = out["country_code"].astype(str).str.upper().str.strip()
    out["indicator_code"] = out["indicator_code"].astype(str).str.strip()
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out = out[out["year"].notna()]
    if "value" in out.columns:
        out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.drop_duplicates(subset=["country_code", "indicator_code", "year"], keep="last")
    if "indicator_name" not in out.columns:
        out["indicator_name"] = out["indicator_code"]

    mask_gdp = out["indicator_code"].str.contains("GDP.PCAP", na=False)
    out.loc[mask_gdp, "log_gdp_per_capita"] = np.log10(out.loc[mask_gdp, "value"].clip(lower=1.0))

    logger.info("WDI cleaned rows=%s countries=%s", len(out), out["country_code"].nunique())
    return out
