-- Federal student loan portfolio (long / tidy format)
CREATE TABLE IF NOT EXISTS student_loan_portfolio (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fiscal_year INTEGER NOT NULL,
    quarter TEXT,
    loan_type TEXT NOT NULL,
    metric TEXT NOT NULL,
    value REAL,
    fiscal_period TEXT,
    total_outstanding_bn_all_types REAL,
    source_file TEXT,
    loaded_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_slp_year ON student_loan_portfolio(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_slp_loan ON student_loan_portfolio(loan_type);

-- World Development Indicators (normalized rows; mirrors API / tidy WDI)
CREATE TABLE IF NOT EXISTS wdi_indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_code TEXT NOT NULL,
    country_name TEXT,
    indicator_code TEXT NOT NULL,
    indicator_name TEXT,
    year INTEGER NOT NULL,
    value REAL,
    log_gdp_per_capita REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE (country_code, indicator_code, year)
);

CREATE INDEX IF NOT EXISTS idx_wdi_country_year ON wdi_indicators(country_code, year);
CREATE INDEX IF NOT EXISTS idx_wdi_indicator ON wdi_indicators(indicator_code);

-- Pipeline run metadata
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    step TEXT NOT NULL,
    status TEXT NOT NULL,
    detail TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
