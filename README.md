# Databricks Calendar Dimension

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A comprehensive calendar dimension table generator for Databricks, featuring US federal holidays, business day calculations, and FRED recession indicators.

## Overview

This project creates and maintains a calendar dimension table (`common.reference.dim_calendar`) that provides:

- **Date identifiers**: date_key, full_date, calendar_date, date_string
- **Calendar components**: day/week/month/quarter/year attributes
- **Period flags**: weekend, weekday, holiday, business day, month/quarter/year end
- **Relative calculations**: days remaining in period, prior year/month dates
- **Reporting helpers**: YTD/QTD/MTD flags, trading day counts
- **Economic indicators**: FRED USRECD recession indicator

## Features

- ✅ US Federal holidays via `holidays` library (includes observed dates)
- ✅ Eastern timezone (America/New_York) alignment
- ✅ FRED API integration for recession indicators
- ✅ Delta Lake with MERGE for incremental updates
- ✅ Databricks Secrets integration for API keys

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Python 3.9+
- FRED API key ([get one free](https://fred.stlouisfed.org/docs/api/api_key.html))

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/dmkern03/Calendar-API.git
cd Calendar-API
```

### 2. Configure FRED API Secret

Create a secret scope and add your FRED API key:

```bash
databricks secrets create-scope fred-api
databricks secrets put-secret fred-api api-key --string-value "YOUR_API_KEY"
```

### 3. Run Setup

Import and run `notebooks/01_setup.py` to create:
- `common` catalog
- `reference` schema
- `dim_calendar` table

### 4. Schedule Daily Refresh

Import `notebooks/02_daily_refresh.py` and schedule it as a daily job.

## Project Structure

```
databricks-calendar-dimension/
├── src/                    # Source code
│   ├── generate_dim_calendar.py
│   └── utils/
├── sql/                    # DDL and validation queries
├── notebooks/              # Databricks notebooks
├── tests/                  # Unit tests
├── config/                 # Configuration files
└── docs/                   # Documentation
```

## Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `START_DATE` | Calendar start date | 1854-12-01 |
| `END_DATE` | Calendar end date | Current date (Eastern) |
| `TARGET_TABLE` | Target Delta table | common.reference.dim_calendar |
| `TIMEZONE` | Timezone for all dates | America/New_York |
| `SECRET_SCOPE` | Databricks secret scope | fred-api |
| `SECRET_KEY` | Secret key for FRED API | api-key |

## Data Dictionary

See [docs/data_dictionary.md](docs/data_dictionary.md) for complete field descriptions.

## Architecture

See [docs/architecture.md](docs/architecture.md) for solution architecture details.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Commit your changes (`git commit -m 'Add new feature'`)
4. Push to the branch (`git push origin feature/new-feature`)
5. Open a Pull Request

## Testing

Run the test suite:

```bash
pip install -r requirements.txt
pytest tests/ -v
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [holidays](https://github.com/vacanza/python-holidays) library for US federal holiday detection
- [FRED API](https://fred.stlouisfed.org/) for recession indicator data
- [Delta Lake](https://delta.io/) for reliable data storage
