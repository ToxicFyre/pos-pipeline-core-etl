# Examples

This directory contains runnable example scripts demonstrating how to use the POS Core ETL package with the new domain-oriented API.

## Prerequisites

Before running any example, ensure you have:

1. **Installed the package**: `pip install -e .` (or `pip install pos-core-etl` for production)
2. **Created `utils/sucursales.json`**: Branch configuration file (see main README for format)
3. **Set environment variables** (required for online extraction):
   - `WS_BASE`: Base URL of your POS instance
   - `WS_USER`: Username for authentication
   - `WS_PASS`: Password for authentication

   **Example:**
   ```bash
   export WS_BASE="https://your-pos-instance.com"
   export WS_USER="your_username"
   export WS_PASS="your_password"
   ```

## Example Scripts

### 1. `payments_full_etl.py`
**Full payments ETL pipeline** using the new API.

- Downloads raw payments from Wansoft API (Bronze)
- Cleans into fact_payments_ticket (Silver/Core)
- Aggregates into mart_payments_daily (Gold/Mart)
- Runs QA checks on the result

**Usage:**
```bash
python examples/payments_full_etl.py
```

### 2. `sales_week_by_group.py`
**Sales data at different grains** using the new API.

- Downloads and cleans sales data
- Shows data at all three grains:
  - `grain="item"`: Core fact (item-line level)
  - `grain="ticket"`: Ticket-level mart
  - `grain="group"`: Category pivot mart

**Usage:**
```bash
python examples/sales_week_by_group.py
```

### 3. `payments_forecast.py`
**Forecasting workflow** using the new API.

- Loads historical payments data
- Generates 13-week forecasts
- Creates deposit schedule for cash flow planning

**Usage:**
```bash
python examples/payments_forecast.py
```

## Quick Start

```python
from pathlib import Path
from pos_core import DataPaths
from pos_core.payments import marts as payments_marts
from pos_core.sales import core as sales_core
from pos_core.sales import marts as sales_marts

# Configure paths
paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))

# Get payment data (daily mart)
payments_df = payments_marts.fetch_daily(paths, "2025-01-01", "2025-01-31")

# Get sales data (item-line core fact)
sales_df = sales_core.fetch(paths, "2025-01-01", "2025-01-31")

# Get sales data at ticket grain
ticket_df = sales_marts.fetch_ticket(paths, "2025-01-01", "2025-01-31")
```

## Grain Reference

**Payments:**
- `payments.core.fetch()`: Core fact (fact_payments_ticket) - ticket × payment method
- `payments.marts.fetch_daily()`: Daily mart (mart_payments_daily) - sucursal × date

**Sales:**
- `sales.core.fetch()`: Core fact (fact_sales_item_line) - item/modifier line
- `sales.marts.fetch_ticket()`: Ticket mart (mart_sales_by_ticket) - one row per ticket
- `sales.marts.fetch_group()`: Group mart (mart_sales_by_group) - category pivot

For more details, see the main [README.md](../README.md) in the project root.
