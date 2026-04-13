"""Download / locate raw datasets (NSLDS portfolio, WDI JSON)."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from config import (
    DATA_RAW,
    NSLDS_DOWNLOAD_URL,
    NSLDS_LOCAL_NAME,
    ROOT,
    WB_COUNTRIES,
    WB_DATE_RANGE,
    WB_INDICATORS,
)

logger = logging.getLogger(__name__)


def _ensure_raw_dir() -> Path:
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    return DATA_RAW


def download_nslsd_portfolio(force: bool = False) -> Path:
    """Fetch Federal Student Aid 'Portfolio by Loan Type' (XLS) from studentaid.gov."""
    _ensure_raw_dir()
    dest = DATA_RAW / NSLDS_LOCAL_NAME
    env = os.environ.get("NSLDS_LOCAL_PATH")
    if env:
        p = Path(env)
        if not p.is_absolute():
            p = ROOT / p
        if p.exists():
            logger.info("Using NSLDS_LOCAL_PATH=%s", p)
            return p
    if dest.exists() and not force:
        logger.info("NSLDS file already present: %s", dest)
        return dest
    logger.info("Downloading NSLDS portfolio from %s", NSLDS_DOWNLOAD_URL)
    try:
        r = requests.get(NSLDS_DOWNLOAD_URL, timeout=120)
        r.raise_for_status()
        dest.write_bytes(r.content)
        logger.info("Saved %s (%s bytes)", dest, dest.stat().st_size)
        return dest
    except Exception as e:
        logger.warning("Download failed (%s). Place the XLS under data/raw/%s or set NSLDS_LOCAL_PATH.", e, NSLDS_LOCAL_NAME)
        if dest.exists():
            logger.info("Using existing file at %s", dest)
            return dest
        raise


def _fetch_world_bank_pages(url: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    page = 1
    while True:
        u = f"{url}&page={page}" if "?" in url else f"{url}?page={page}"
        r = requests.get(u, timeout=120)
        r.raise_for_status()
        payload = r.json()
        if not isinstance(payload, list) or len(payload) < 2:
            break
        meta, rows = payload[0], payload[1]
        if not rows:
            break
        out.extend(rows)
        pages = int(meta.get("pages", 1)) if isinstance(meta, dict) else 1
        if page >= pages:
            break
        page += 1
    return out


def fetch_wdi_from_world_bank_api() -> pd.DataFrame:
    """Pull selected WDI-style rows from the public World Bank API (JSON)."""
    frames = []
    for code, name in WB_INDICATORS:
        url = (
            f"https://api.worldbank.org/v2/country/{WB_COUNTRIES}/indicator/{code}"
            f"?format=json&per_page=20000&date={WB_DATE_RANGE}"
        )
        logger.info("Fetching World Bank indicator %s", code)
        rows = _fetch_world_bank_pages(url)
        for row in rows:
            val = row.get("value")
            if val is None:
                continue
            frames.append(
                {
                    "country_code": (row.get("countryiso3code") or "").strip(),
                    "country_name": (row.get("country", {}) or {}).get("value"),
                    "indicator_code": code,
                    "indicator_name": name,
                    "year": int(row["date"]) if row.get("date") else None,
                    "value": float(val),
                }
            )
    if not frames:
        return pd.DataFrame(
            columns=[
                "country_code",
                "country_name",
                "indicator_code",
                "indicator_name",
                "year",
                "value",
            ]
        )
    return pd.DataFrame(frames)


def load_wdi_json(path: Path) -> pd.DataFrame:
    """Load normalized WDI JSON (array of flat records). Empty strings -> NA."""
    text = path.read_text(encoding="utf-8")
    data = json.loads(text)
    if not isinstance(data, list):
        raise ValueError("Expected a JSON array of records")
    df = pd.DataFrame(data)
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].replace("", pd.NA)
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df


def resolve_wdi_dataframe(prefer_api: bool = True) -> tuple[pd.DataFrame, str]:
    """
    Returns (dataframe, provenance_label).
    Priority: WDI_JSON_PATH env -> API (if prefer_api) -> bundled sample.
    """
    env_path = os.environ.get("WDI_JSON_PATH")
    if env_path:
        p = Path(env_path)
        if not p.is_absolute():
            p = ROOT / p
        logger.info("Loading WDI from WDI_JSON_PATH=%s", p)
        return load_wdi_json(p), f"file:{p}"

    sample = DATA_RAW / "wdi_indicators_sample.json"
    if prefer_api:
        try:
            df = fetch_wdi_from_world_bank_api()
            if not df.empty:
                return df, "world_bank_api"
        except Exception as e:
            logger.warning("World Bank API fetch failed (%s); using sample JSON", e)

    if sample.exists():
        logger.info("Using bundled sample %s", sample)
        return load_wdi_json(sample), f"file:{sample}"

    raise FileNotFoundError(
        "No WDI data: set WDI_JSON_PATH or add data/raw/wdi_indicators_sample.json"
    )
