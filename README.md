# Intern Data Engineering / Data Analyst Challenge

Small end-to-end pipeline: Federal Student Aid Portfolio by Loan Type (NSLDS-related publication on [data.gov](https://catalog.data.gov/dataset/national-student-loan-data-system-722b0), file sourced from [studentaid.gov](https://studentaid.gov/)) plus World Development Indicators-style data. JSON indicator series are loaded via the World Bank public API (same codes as in the [Kaggle WDI JSON](https://www.kaggle.com/datasets/michaellang/world-bank-world-development-indicators-json) dataset); you can instead point `WDI_JSON_PATH` at a normalized extract from Kaggle.

## Quick start

```bash
cd intern_de_challenge
pip install -r requirements.txt
python run_pipeline.py
python -m streamlit run streamlit_app.py
```

Outputs:

- `data/pipeline.db`  SQLite warehouse
- `data/processed/*.csv` flat files for sharing or BI tools
- `sql/schema.sql`  DDL

## 1. Data extraction

| Source | Method |
|--------|--------|
| NSLDS portfolio | Download XLS from `config.NSLDS_DOWNLOAD_URL` or use `data/raw/PortfoliobyLoanType.xls` / `NSLDS_LOCAL_PATH` |
| WDI | Prefer World Bank API (`src/extract.py`); fallback to `data/raw/wdi_indicators_sample.json`; or set `WDI_JSON_PATH` to your Kaggle-normalized JSON |

Preview / integrity: `run_pipeline.py` logs row counts and year ranges; `src/validate.py` asserts non-empty tables and reasonable null rates.

## 2. Cleaning and transformations

| Step | Purpose |
|------|---------|
| Skip title rows in XLS; read row 5–6 as multi-column headers | Recover true loan-type / metric grid |
| Melt to long format (`loan_type`, `metric`, `value`) | Tidy model for SQL + plotting |
| Map header text to metric slugs (`dollars_outstanding_bn`, `recipients_mm`, `unique_recipients_mm`) | Consistent joins and filters |
| `fiscal_period`, `total_outstanding_bn_all_types` | Derived context for dashboards |
| WDI: replace `""` with NA, `to_numeric`, drop duplicate keys | Handles Kaggle-style empty strings |
| `log_gdp_per_capita` for GDP per capita | Stable cross-country visualization |

## 3. Data loading

- **SQLite** file `data/pipeline.db` (see `sql/schema.sql`)
- **Validation:** row counts, indicator coverage by `indicator_code`and null share on loan `value`

## Kaggle WDI JSON

The bundled `data/raw/wdi_indicators_sample.json` demonstrates the expected shape when API access is blocked.
