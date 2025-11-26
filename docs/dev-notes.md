# Development Notes - ETL API Refactor

## Target API

The goal of this refactor is to create a clean, lean API that matches how we think about ETL workflows.

### ETL Configs
- `SalesETLConfig`
- `PaymentsETLConfig`

### Stage Commands
- `download_sales(...)`, `clean_sales(...)`, `aggregate_sales(...)`
- `download_payments(...)`, `clean_payments(...)`, `aggregate_payments(...)`

### Query / Pandas Creator
- `get_sales(..., level="ticket|group|day", refresh=False)`
- `get_payments(..., refresh=False)`
- `get_payments_forecast(..., horizon_weeks=13, refresh=False)`

Everything we build should support this set of calls.

## Constraints

To keep the codebase lean and prevent overbuilding:

1. **No new public function unless I have a real use for it right now.**
2. **One concept → one way to do it.**
3. **Public API uses simple types**: strings, lists, DataFrames, dataclasses.
4. **If it feels too clever, don't do it.**

Check new code against this list before adding it.

## Current Public API Inventory

### From `pos_core.etl`
- `PaymentsETLConfig`, `PaymentsPaths`
- `build_payments_dataset`

### From `pos_core.forecasting`
- `ForecastConfig`, `ForecastResult`
- `run_payments_forecast`

### From `pos_core.qa`
- `PaymentsQAResult`
- `run_payments_qa`

## Internal Modules

These are treated as internal (not re-exported from `__init__`):
- `pos_core.etl.a_extract.*`
- `pos_core.etl.b_transform.*`
- `pos_core.etl.c_load.*`
- Helper modules: `pos_core.etl.utils`, `pos_core.etl.branch_config`

## API Decisions

### Keep as-is
- `build_payments_dataset` - Will wrap internally later using stage functions

### Wrap
- Will create `download_payments`, `clean_payments`, `aggregate_payments` that call existing internals
- Will create `download_sales`, `clean_sales`, `aggregate_sales` that call existing internals

### Mark as internal
- All functions in `a_extract`, `b_transform`, `c_load` are already internal (not exported)

## Periodic Lean Audit

Every few weeks, ask:

1. Is there any public function nobody is using?
2. Is there any duplicated logic between stage and query functions?
3. Could two similar functions be merged into one with a simple parameter?

