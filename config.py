"""Paths and constants for the intern DE challenge pipeline."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSED = ROOT / "data" / "processed"
SQL_DIR = ROOT / "sql"
DB_PATH = ROOT / "data" / "pipeline.db"

NSLDS_DOWNLOAD_URL = (
    "https://studentaid.gov/sites/default/files/fsawg/datacenter/library/PortfoliobyLoanType.xls"
)
NSLDS_LOCAL_NAME = "PortfoliobyLoanType.xls"

# World Bank API (same indicators as in sample; full Kaggle JSON can replace sample file)
WB_INDICATORS = [
    ("SE.XPD.TOTL.GD.ZS", "Expenditure on education, total (% of GDP)"),
    ("NY.GDP.PCAP.CD", "GDP per capita (current US$)"),
]
WB_COUNTRIES = "USA;CHN;DEU;ETH;IND"
WB_DATE_RANGE = "2010:2022"

# If set, load this JSON instead of API/sample (e.g. Kaggle extract)
WDI_JSON_PATH = None  # overridden by env WDI_JSON_PATH in extract module
