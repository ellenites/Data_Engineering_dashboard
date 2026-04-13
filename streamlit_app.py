"""
Streamlit dashboard (run after `python run_pipeline.py`):
  python -m streamlit run streamlit_app.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import math

import pandas as pd
import plotly.express as px
import sqlite3
import streamlit as st

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import DB_PATH

st.set_page_config(page_title="Intern DE Challenge — Loan & WDI", layout="wide")
st.title("Federal student loans & World Development Indicators")
st.caption("SQLite source: `data/pipeline.db` — NSLDS portfolio by loan type + WDI-style indicators.")

if not DB_PATH.exists():
    st.error("Database not found. Run `python run_pipeline.py` from the project root first.")
    st.stop()

conn = sqlite3.connect(DB_PATH)
loans = pd.read_sql("SELECT * FROM student_loan_portfolio", conn)
wdi = pd.read_sql("SELECT * FROM wdi_indicators", conn)
conn.close()

tab1, tab2, tab3 = st.tabs(["Student loan portfolio", "WDI comparison", "Key insights"])

with tab1:
    st.subheader("Outstanding balances by loan type (billions USD)")
    dollars = loans[loans["metric"] == "dollars_outstanding_bn"].copy()
    fig1 = px.line(
        dollars,
        x="fiscal_year",
        y="value",
        color="loan_type",
        markers=True,
        title="Dollars outstanding (NSLDS / Federal Student Aid)",
    )
    st.plotly_chart(fig1, use_container_width=True)

    st.subheader("Recipients (millions) — selected types")
    rec = loans[loans["metric"] == "recipients_mm"].copy()
    fig2 = px.area(
        rec,
        x="fiscal_year",
        y="value",
        color="loan_type",
        title="Borrower counts by loan program",
    )
    st.plotly_chart(fig2, use_container_width=True)

with tab2:
    c1, c2 = st.columns(2)
    ind = sorted(wdi["indicator_name"].dropna().unique())
    choice = c1.selectbox("Indicator", ind, index=0)
    countries = st.multiselect(
        "Countries",
        sorted(wdi["country_name"].dropna().unique()),
        default=sorted(wdi["country_name"].dropna().unique())[:3],
    )
    sub = wdi[(wdi["indicator_name"] == choice) & (wdi["country_name"].isin(countries))]
    fig3 = px.line(
        sub,
        x="year",
        y="value",
        color="country_name",
        markers=True,
        title=choice,
    )
    st.plotly_chart(fig3, use_container_width=True)

    gdp = wdi[wdi["indicator_code"].str.contains("GDP.PCAP", na=False)]
    if not gdp.empty:
        st.subheader("GDP per capita (log10 scale) — cross-country spread")
        gdp = gdp.copy()
        gdp["log10_gdp"] = pd.to_numeric(gdp["value"], errors="coerce").apply(
            lambda x: math.log10(x) if x and x > 0 else None
        )
        fig4 = px.line(
            gdp,
            x="year",
            y="log10_gdp",
            color="country_name",
            markers=True,
            title="log10 GDP per capita (current US$)",
        )
        st.plotly_chart(fig4, use_container_width=True)

with tab3:
    st.markdown(
        """
1. **Portfolio concentration:** Direct loan programs (Stafford, Grad PLUS, Parent PLUS, Consolidation) dominate 
   outstanding dollars; trends by `fiscal_year` show how balances evolved across aid programs.
2. **Scale of participation:** Recipient counts (`recipients_mm` and `unique_recipients_mm` for combined Stafford) 
   illustrate how many borrowers sit under each program — useful for comparing volume vs. balance intensity.
3. **International education effort:** `SE.XPD.TOTL.GD.ZS` compares public education effort as a share of GDP across 
   countries — highlights different policy choices independent of income level.
4. **Income context:** GDP per capita (`NY.GDP.PCAP.CD`) places countries on a comparable prosperity scale; 
   pairing with education spending supports narrative (not causal) interpretation.
5. **Data quality reality:** WDI often has **missing years** (empty strings in source JSON or null API values); 
   the pipeline coerces types and drops invalid keys while preserving NA for analysis transparency.
        """
    )
