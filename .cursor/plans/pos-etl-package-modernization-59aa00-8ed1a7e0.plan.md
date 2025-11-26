<!-- 8ed1a7e0-02f0-4a34-951e-60116810a58e 72931d72-e2d9-4950-ad8d-2a690f916764 -->
# POS ETL Package Modernization Plan

## Overview

Transform the existing POS ETL codebase into a production-ready Python package (`pos-core-etl`) that follows modern Python packaging standards, provides clean APIs, and maintains all existing functionality while improving maintainability and usability.

## Current State Analysis

### Strengths

- Well-structured modular architecture (ETL, Forecasting, QA)
- Comprehensive functionality (extraction, transformation, aggregation, forecasting, QA)
- Good documentation in docstrings
- Incremental processing capabilities
- Branch code window handling

### Issues to Address

1. **Inconsistent imports**: Uses `pos_etl`, `pos_forecasting`, `pos_qa` instead of `pos_core.etl`, `pos_core.forecasting`, `pos_core.qa`
2. **Hardcoded paths**: Configuration assumes project root structure
3. **Subprocess orchestration**: Uses subprocess calls instead of direct function calls
4. **Missing package root**: No `src/pos_core/__init__.py` with version and exports
5. **No modern packaging**: Missing `pyproject.toml` and proper metadata
6. **External dependencies**: `utils.telegram_notifier` is project-specific
7. **Path manipulation**: Uses `sys.path` hacks instead of proper imports

## Package Structure

```
pos-core-etl/
├── pyproject.toml              # Modern Python packaging (PEP 517/518)
├── README.md                   # Comprehensive documentation
├── LICENSE                     # MIT License
├── .gitignore
├── src/
│   └── pos_core/
│       ├── __init__.py         # Package root with version and top-level exports
│       ├── etl/
│       │   ├── __init__.py     # Export main functions
│       │   ├── extract/        # Rename a_extract → extract
│       │   ├── transform/      # Rename b_transform → transform
│       │   ├── load/           # Rename c_load → load
│       │   ├── pipeline.py     # Rename build_payments_dataset.py
│       │   ├── config.py       # Refactor for configurability
│       │   └── utils.py
│       ├── forecasting/
│       │   ├── __init__.py     # Export main functions
│       │   └── ... (existing structure)
│       ├── qa/
│       │   ├── __init__.py     # Export main functions
│       │   └── ... (existing structure)
│       └── config/             # NEW: Centralized configuration
│           ├── __init__.py
│           └── settings.py    # Configuration management
├── tests/                      # NEW: Test suite
│   ├── __init__.py
│   ├── test_etl/
│   ├── test_forecasting/
│   └── test_qa/
├── examples/                   # NEW: Usage examples
│   ├── basic_etl.py
│   ├── forecasting_example.py
│   └── qa_example.py
└── docs/                       # NEW: Documentation
    ├── installation.md
    ├── configuration.md
    └── api_reference.md
```

## Implementation Steps

### 1. Modern Package Configuration (`pyproject.toml`)

Create `pyproject.toml` following PEP 517/518 standards:

```toml
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "pos-core-etl"
version = "0.1.0"
description = "Point of Sale ETL pipeline with forecasting and QA capabilities"
readme = "README.md"
requires-python = ">=3.8"
license = {text = "MIT"}
authors = [
    {name = "ToxicFyre", email = "..."}
]
keywords = ["etl", "pos", "forecasting", "data-pipeline"]
classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
]

dependencies = [
    "pandas>=1.3.0",
    "numpy>=1.20.0",
    "requests>=2.25.0",
    "beautifulsoup4>=4.9.0",
    "statsmodels>=0.12.0",
    "openpyxl>=3.0.0",
]

[project.optional-dependencies]
telegram = ["python-telegram-bot>=20.0"]  # Optional Telegram support
dev = [
    "pytest>=7.0",
    "pytest-cov>=4.0",
    "black>=23.0",
    "ruff>=0.1.0",
    "mypy>=1.0",
]

[project.scripts]
pos-etl = "pos_core.etl.pipeline:main"
pos-forecast = "pos_core.forecasting.pipeline:main"
pos-qa = "pos_core.qa.qa_payments:main"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
```

### 2. Package Root Initialization (`src/pos_core/__init__.py`)

Create package root with version and clean exports:

```python
"""POS Core ETL - Point of Sale data processing and forecasting."""

__version__ = "0.1.0"

# Top-level API exports
from pos_core.etl import build_payments_dataset
from pos_core.forecasting import generate_forecasts
from pos_core.qa import run_qa

__all__ = [
    "__version__",
    "build_payments_dataset",
    "generate_forecasts",
    "run_qa",
]
```

### 3. Standardize All Imports

**Current**: `from pos_etl import config`

**Target**: `from pos_core.etl import config` or `from pos_core.etl.config import ...`

Update all imports across:

- `src/pos_core/etl/**/*.py` (29 files with `pos_etl` imports)
- `src/pos_core/forecasting/**/*.py` (uses `pos_forecasting` and `pos_etl`)
- `src/pos_core/qa/**/*.py` (uses `pos_etl` and `pos_qa`)

### 4. Refactor Configuration Management

**Current**: Hardcoded paths relative to `ROOT = Path(**file**).resolve().parents[2]`

**Target**: Explicit configuration with dataclass, no global singleton:

**New `src/pos_core/etl/config.py`** (refactored):

```python
"""Configuration for POS ETL pipeline."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

@dataclass
class PaymentsPaths:
    """Path configuration for payments ETL stages."""
    raw_payments: Path
    clean_payments: Path
    proc_payments: Path
    sucursales_json: Path

@dataclass
class PaymentsETLConfig:
    """Configuration for payments ETL pipeline."""
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
        """Create config from data root directory.
        
        Args:
            data_root: Root directory containing a_raw, b_clean, c_processed
            sucursales_json: Path to sucursales.json (defaults to data_root/../utils/sucursales.json)
            chunk_size_days: Maximum days per HTTP request chunk
        """
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
    
    @classmethod
    def default(cls) -> "PaymentsETLConfig":
        """Create default config from current working directory.
        
        Convenience method for CLI usage. For programmatic usage,
        prefer from_data_root() with explicit path.
        """
        from pathlib import Path
        import os
        data_root = Path(os.getcwd()) / "data"
        return cls.from_data_root(data_root)
```

**Key principles**:

- No global singleton state
- Explicit `config` parameter passed to functions
- `from_data_root()` helper for easy construction
- `default()` for CLI convenience only
- Runner repo owns data_root decisions

### 5. Refactor Pipeline Orchestration

**Current**: Uses subprocess calls (`subprocess.run([python_exe, "-m", "pos_etl.b_transform.pos_excel_payments_cleaner", ...])`)

**Target**: Direct function calls:

**Update `src/pos_core/etl/pipeline.py`** (renamed from `build_payments_dataset.py`):

```python
# Instead of:
# run_cmd([python_exe, "-m", "pos_etl.b_transform.pos_excel_payments_cleaner", ...])

# Use:
from pos_core.etl.transform.payments_cleaner import clean_payments_directory
clean_payments_directory(input_dir=raw_payments_root, output_dir=clean_payments_dir, recursive=True)
```

**Refactor cleaners to expose functions** (not just CLI):

- `pos_excel_payments_cleaner.py` → Add `clean_payments_directory()` function
- `aggregate_payments_by_day.py` → Add `aggregate_payments()` function
- Similar for other modules

### 6. Make External Dependencies Optional

**Current**: `from utils.telegram_notifier import ...` (project-specific)

**Target**: Optional dependency with interface:

**New `src/pos_core/forecasting/notifiers.py`**:

```python
"""Notification backends for forecasting results."""

from typing import Optional, Protocol

class Notifier(Protocol):
    def send(self, message: str) -> bool: ...

def get_notifier(backend: str = "console") -> Optional[Notifier]:
    """Factory for notification backends."""
    if backend == "console":
        return ConsoleNotifier()
    elif backend == "telegram":
        try:
            from pos_core.extras.telegram import TelegramNotifier
            return TelegramNotifier()
        except ImportError:
            return None
    return None
```

Move Telegram functionality to optional extra or separate package.

### 7. Rename Directories for Clarity

- `a_extract/` → `extract/` (remove prefix, more Pythonic)
- `b_transform/` → `transform/`
- `c_load/` → `load/`

Update all imports and references.

### 8. Create Clean Public APIs

**ETL Module** (`src/pos_core/etl/__init__.py`):

```python
"""ETL pipeline for POS data processing."""

from pos_core.etl.pipeline import build_payments_dataset
from pos_core.etl.extract import download_payments_report
from pos_core.etl.transform import clean_payments_file
from pos_core.etl.load import aggregate_payments_by_day

__all__ = [
    "build_payments_dataset",
    "download_payments_report",
    "clean_payments_file",
    "aggregate_payments_by_day",
]
```

**Forecasting Module** (`src/pos_core/forecasting/__init__.py`):

```python
"""Time series forecasting for POS payment data."""

from pos_core.forecasting.pipeline import generate_forecasts
from pos_core.forecasting.models import LogARIMAModel, ForecastModel

__all__ = [
    "generate_forecasts",
    "LogARIMAModel",
    "ForecastModel",
]
```

**QA Module** (`src/pos_core/qa/__init__.py`):

```python
"""Quality assurance for POS data."""

from pos_core.qa.qa_payments import run_qa, QAResult

__all__ = ["run_qa", "QAResult"]
```

### 9. Environment Variable Support

Support configuration via environment variables:

- `POS_DATA_DIR`: Override data directory
- `POS_SUCURSALES_PATH`: Override sucursales.json path
- `WS_BASE`, `WS_USER`, `WS_PASS`: POS API credentials (existing)
- `POS_LOG_LEVEL`: Logging level

### 10. CLI Entry Points

Define CLI commands in `pyproject.toml`:

- `pos-etl`: ETL pipeline
- `pos-forecast`: Forecasting pipeline
- `pos-qa`: QA checks

Usage:

```bash
pos-etl --start 2023-01-01 --end 2023-12-31
pos-forecast
pos-qa --file aggregated_payments_daily.csv
```

### 11. Testing Structure

Create test suite:

- `tests/test_etl/`: Unit tests for extraction, transformation, loading
- `tests/test_forecasting/`: Model and pipeline tests
- `tests/test_qa/`: QA validation tests
- `tests/fixtures/`: Sample data files
- Mock POS API responses for testing

### 12. Documentation

- **README.md**: Installation, quick start, basic usage
- **docs/installation.md**: Detailed installation guide
- **docs/configuration.md**: Configuration options
- **docs/api_reference.md**: API documentation
- **examples/**: Working code examples

## Usage Examples

### Programmatic API

```python
from pos_core import build_payments_dataset, generate_forecasts, run_qa
from pos_core.config import POSConfig
from pathlib import Path
from datetime import date

# Configure data directory
config = POSConfig(data_dir=Path("/path/to/data"))
pos_core.config.set_config(config)

# Run ETL pipeline
build_payments_dataset(
    global_start=date(2023, 1, 1),
    global_end=date(2023, 12, 31),
    max_days_per_chunk=180,
)

# Generate forecasts
forecasts, historical_df, last_date = generate_forecasts()

# Run QA
results, csv_path, sales_csv = run_qa(
    file_name="aggregated_payments_daily.csv",
    sample_months_n=3,
)
```

### CLI Usage

```bash
# Install package
pip install pos-core-etl

# Run ETL
pos-etl --start 2023-01-01 --end 2023-12-31

# Generate forecasts
pos-forecast

# Run QA
pos-qa --file aggregated_payments_daily.csv --sample-months 5
```

### Configuration

```python
# Via environment variables
export POS_DATA_DIR=/custom/data/path
export POS_SUCURSALES_PATH=/custom/sucursales.json

# Via code
from pos_core.config import POSConfig, set_config
set_config(POSConfig(data_dir=Path("/custom/path")))
```

## Best Practices Applied

1. **Modern Packaging**: PEP 517/518 with `pyproject.toml`
2. **Namespace Packages**: Clean `pos_core.*` namespace
3. **Optional Dependencies**: Telegram as optional extra
4. **Configuration Management**: Environment variables + programmatic API
5. **Direct Function Calls**: No subprocess overhead
6. **Clean APIs**: Well-defined public interfaces
7. **CLI Entry Points**: Standard Python CLI commands
8. **Testing**: Comprehensive test suite structure
9. **Documentation**: README + detailed docs + examples
10. **Type Hints**: Maintain existing type annotations

## Migration Path

1. Create `pyproject.toml` and package structure
2. Add `src/pos_core/__init__.py` with version
3. Create centralized config system
4. Refactor imports (automated with find/replace + verification)
5. Rename directories and update references
6. Refactor subprocess calls to direct function calls
7. Create public APIs in `__init__.py` files
8. Add CLI entry points
9. Create tests and examples
10. Update documentation

## Files to Modify

### New Files

- `pyproject.toml`
- `src/pos_core/__init__.py`
- `src/pos_core/config/__init__.py`
- `src/pos_core/config/settings.py`
- `README.md` (comprehensive)
- `tests/**/*.py` (test suite)
- `examples/*.py` (usage examples)
- `docs/*.md` (documentation)

### Modified Files

- All files in `src/pos_core/etl/` (imports, subprocess → function calls)
- All files in `src/pos_core/forecasting/` (imports, optional dependencies)
- All files in `src/pos_core/qa/` (imports)
- `src/pos_core/etl/config.py` (refactor or deprecate)
- `src/pos_core/etl/build_payments_dataset.py` → `pipeline.py` (rename + refactor)

### Directory Renames

- `src/pos_core/etl/a_extract/` → `src/pos_core/etl/extract/`
- `src/pos_core/etl/b_transform/` → `src/pos_core/etl/transform/`
- `src/pos_core/etl/c_load/` → `src/pos_core/etl/load/`

## Success Criteria

- Package installable via `pip install pos-core-etl`
- All imports use `pos_core.*` namespace
- Configuration is flexible and environment-aware
- No subprocess calls (direct function calls)
- CLI commands work: `pos-etl`, `pos-forecast`, `pos-qa`
- Programmatic API is clean and well-documented
- Tests pass
- Documentation is comprehensive
- Backward compatibility maintained where possible