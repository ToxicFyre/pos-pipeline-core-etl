<!-- 0a881211-868c-42f4-9ec2-1f70f88e053a 6eaacf71-5a09-4e93-b10c-deb2476a056c -->
# POS Core ETL – Modernization Plan

## Goal

Turn `pos-pipeline-core-etl` into a clean Python package `pos_core` that:

- Installs with `pip install -e .`
- Exposes small, explicit APIs for ETL, forecasting, and QA
- Does **not** assume a global `data/` folder or global config
- Can be used by another repo that owns dev/prod config, Telegram, scheduling, etc.

**Work in phases. Finish each phase in a working state (code compiles) before starting the next.**

---

## Phase 1 – Packaging and Imports

### 1.1 Create `pyproject.toml`

Create `pyproject.toml` at repo root with:

- `[build-system]` using `setuptools.build_meta`
- `[project]`:
        - `name = "pos-core-etl"`
        - `version = "0.1.0"`
        - `requires-python = ">=3.10"`
        - `dependencies` including: `pandas`, `numpy`, `requests`, `beautifulsoup4`, `openpyxl`, `statsmodels`
        - Optional extra `"dev"` with `pytest` (and optionally `black`, `ruff`, `mypy`)
- `[tool.setuptools.packages.find]` with `where = ["src"]`
- Basic `pytest` config under `[tool.pytest.ini_options]`

### 1.2 Initialize Package Root

In `src/pos_core/__init__.py`:

- Add short docstring
- Set `__version__ = "0.1.0"`

Ensure these files exist (can be empty for now):

- `src/pos_core/etl/__init__.py`
- `src/pos_core/forecasting/__init__.py`
- `src/pos_core/qa/__init__.py`

### 1.3 Standardize Imports

Replace all imports:

- `pos_etl` → `pos_core.etl`
- `pos_forecasting` → `pos_core.forecasting`
- `pos_qa` → `pos_core.qa`

Remove any `sys.path` hacks; rely on normal package imports under `src/`.

**Files to update:**

- `src/pos_core/etl/**/*.py` (29 files with `pos_etl` imports)
- `src/pos_core/forecasting/**/*.py` (uses `pos_forecasting` and `pos_etl`)
- `src/pos_core/qa/**/*.py` (uses `pos_etl` and `pos_qa`)

### 1.4 Sanity Check

Run `python -m compileall src` and fix any import errors.

---

## Phase 2 – ETL Public API with Explicit `data_root`

**Goal:** Expose a function that runs the payments ETL when given a date range and a `data_root` (no hard-coded `data/` folder inside the package).

### 2.1 Create `src/pos_core/etl/api.py`

Define path and config dataclasses:

```python
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
import pandas as pd

@dataclass
class PaymentsPaths:
    raw_payments: Path
    clean_payments: Path
    proc_payments: Path
    sucursales_json: Path

@dataclass
class PaymentsETLConfig:
    paths: PaymentsPaths
    chunk_size_days: int = 180
    excluded_branches: List[str] = field(default_factory=lambda: ["CEDIS"])

    @classmethod
    def from_data_root(
        cls,
        data_root: Path,
        sucursales_json: Optional[Path] = None,
        chunk_size_days: int = 180,
    ) -> "PaymentsETLConfig":
        if sucursales_json is None:
            sucursales_json = data_root.parent / "utils" / "sucursales.json"
        return cls(
            paths=PaymentsPaths(
                raw_payments=data_root / "a_raw" / "payments" / "batch",
                clean_payments=data_root / "b_clean" / "payments" / "batch",
                proc_payments=data_root / "c_processed" / "payments",
                sucursales_json=sucursales_json,
            ),
            chunk_size_days=chunk_size_days,
        )
```

Add helper:

```python
def ensure_dirs(config: PaymentsETLConfig) -> None:
    for p in (
        config.paths.raw_payments,
        config.paths.clean_payments,
        config.paths.proc_payments,
    ):
        p.mkdir(parents=True, exist_ok=True)
```

Define main API function:

```python
def build_payments_dataset(
    start_date: str,
    end_date: str,
    config: PaymentsETLConfig,
    branches: List[str] | None = None,
    steps: List[str] | None = None,
) -> pd.DataFrame:
    """
    High-level entry point for the payments ETL.

    Uses existing pipeline logic but:
  - does not assume any hard-coded data directories,
  - uses config.paths.* for all I/O,
  - calls ensure_dirs(config) before writing,
  - returns the final aggregated payments DataFrame.
    """
    ...
```

### 2.2 Refactor Existing Pipeline

Take logic from `src/pos_core/etl/build_payments_dataset.py` and move it into `build_payments_dataset` in `api.py`.

**Key changes:**

- Replace hard-coded `Path("data") / ...` with `config.paths.*`
- Replace `subprocess.run([... "python", "-m", ...])` calls with direct imports and function calls:
        - Make `pos_excel_payments_cleaner.py` expose `clean_payments_directory(input_dir, output_dir, recursive=True)`
        - Make `aggregate_payments_by_day.py` expose `aggregate_payments(clean_dir, proc_path)`
        - Make `HTTP_extraction.py` expose `download_payments_report(...)`

### 2.3 Keep Minimal CLI

In `src/pos_core/etl/build_payments_dataset.py`, keep only:

```python
if __name__ == "__main__":
    # parse CLI args
    # build config = PaymentsETLConfig.from_data_root(Path.cwd() / "data")
    # call build_payments_dataset(...)
```

This CLI is for manual use only. The main consumer will be other Python code.

### 2.4 Export ETL API

In `src/pos_core/etl/__init__.py`:

```python
from .api import PaymentsPaths, PaymentsETLConfig, build_payments_dataset

__all__ = ["PaymentsPaths", "PaymentsETLConfig", "build_payments_dataset"]
```

### 2.5 Compile Again

Run `python -m compileall src`.

---

## Phase 3 – Forecasting Public API without Side Effects

**Goal:** Pure forecasting API that operates on a payments DataFrame and returns a result; no Telegram, no file writes.

### 3.1 Create `src/pos_core/forecasting/api.py`

```python
from dataclasses import dataclass
import pandas as pd

@dataclass
class ForecastResult:
    forecast: pd.DataFrame
    deposit_schedule: pd.DataFrame

def run_payments_forecast(
    payments_df: pd.DataFrame,
    horizon_days: int = 7,
    config: dict | None = None,
) -> ForecastResult:
    """
    Core forecasting API.

    Uses existing ARIMA/forecasting logic but:
  - does not read environment variables,
  - does not send Telegram messages,
  - does not write files to disk.
    """
    ...
```

### 3.2 Refactor `forecasting/pipeline.py`

- Move the core forecasting logic into `run_payments_forecast` in `api.py`
- Keep `pipeline.py` only for CLI usage:
        - Load aggregated payments from disk
        - Call `run_payments_forecast`
        - Print or format results for interactive use

### 3.3 Keep Formatters Pure

`formatters/console.py` and `formatters/telegram.py` should:

- Take a `ForecastResult`
- Return formatted text or tables
- **Not** send Telegram messages or touch the network

### 3.4 Export Forecasting API

In `src/pos_core/forecasting/__init__.py`:

```python
from .api import ForecastResult, run_payments_forecast

__all__ = ["ForecastResult", "run_payments_forecast"]
```

Compile again.

---

## Phase 4 – QA Public API

**Goal:** An in-memory QA function that takes an aggregated payments DataFrame and returns structured results.

### 4.1 Create `src/pos_core/qa/api.py`

```python
from dataclasses import dataclass
import pandas as pd

@dataclass
class PaymentsQAResult:
    summary: dict
    missing_days: pd.DataFrame | None
    duplicate_days: pd.DataFrame | None
    zscore_anomalies: pd.DataFrame | None
    zero_method_flags: pd.DataFrame | None

def run_payments_qa(
    payments_df: pd.DataFrame,
    level: int = 4,
) -> PaymentsQAResult:
    """
    Run the existing QA checks (from qa_payments.py) in memory,
    without reading or writing any files.
    """
    ...
```

### 4.2 Refactor `qa_payments.py`

- Move the validation logic into helpers used by `run_payments_qa`
- Keep the current CLI only under `if __name__ == "__main__":` for manual CSV-based runs

### 4.3 Export QA API

In `src/pos_core/qa/__init__.py`:

```python
from .api import PaymentsQAResult, run_payments_qa

__all__ = ["PaymentsQAResult", "run_payments_qa"]
```

Compile again.

---

## Phase 5 – Optional Cleanups (After APIs Are Stable)

Once the above phases work:

### 5.1 Directory Renames

Rename:

- `a_extract/ → extract/`
- `b_transform/ → transform/`
- `c_load/ → load/`

Update all imports accordingly.

### 5.2 Add CLI Entrypoints

Add small CLI entrypoints in `pyproject.toml`:

```toml
[project.scripts]
pos-etl = "pos_core.etl.build_payments_dataset:main"
pos-forecast = "pos_core.forecasting.pipeline:main"
pos-qa = "pos_core.qa.qa_payments:main"
```

These are thin wrappers around the APIs.

### 5.3 Add Minimal Tests

Create `tests/` suite:

- Import smoke tests for `pos_core.etl`, `pos_core.forecasting`, `pos_core.qa`
- Tiny fake `payments_df` to run through forecasting and QA

### 5.4 Add Examples

Add `examples/` showing how another repo would:

- Build a `PaymentsETLConfig.from_data_root(...)`
- Call `build_payments_dataset`
- Feed the result into `run_payments_forecast` and `run_payments_qa`

---

## Key Principles

1. **No Global Config**: All config passed explicitly as parameters
2. **No Side Effects in Core**: Core APIs don't send Telegram, write files, or read env vars
3. **Explicit `data_root`**: Runner repo owns data directory decisions
4. **Phased Approach**: Each phase compiles and works before moving to next
5. **Programmatic APIs First**: CLIs are thin wrappers, not the main interface

---

## Files to Create/Modify

### New Files

- `pyproject.toml`
- `src/pos_core/etl/api.py`
- `src/pos_core/forecasting/api.py`
- `src/pos_core/qa/api.py`
- `tests/` (minimal smoke tests)
- `examples/` (usage examples)

### Modified Files

- `src/pos_core/__init__.py` (add version)
- `src/pos_core/etl/__init__.py` (export API)
- `src/pos_core/forecasting/__init__.py` (export API)
- `src/pos_core/qa/__init__.py` (export API)
- All files with `pos_etl`/`pos_forecasting`/`pos_qa` imports (standardize to `pos_core.*`)
- `src/pos_core/etl/build_payments_dataset.py` (refactor to use API, keep CLI)
- `src/pos_core/forecasting/pipeline.py` (refactor to use API, keep CLI)
- `src/pos_core/qa/qa_payments.py` (refactor to use API, keep CLI)
- Cleaner/aggregator modules (expose functions, not just CLI)

### Optional (Phase 5)

- Directory renames: `a_extract/`, `b_transform/`, `c_load/`