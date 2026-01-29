# Post-Change Quality Control Procedure

> **AI Assistants**: Read this document IMMEDIATELY after making any code changes to ensure quality control standards are met.

## Quick Reference Commands

```powershell
# Run these commands from the project root (in order)
python -m ruff check src/ tests/
python -m ruff format src/ tests/
python -m mypy src/pos_core/
python -m pytest tests/ -m "not live" -v --tb=short
```

---

## 1. Linting with Ruff

### Check for Lint Errors

```powershell
python -m ruff check src/ tests/
```

**Expected Output**: `All checks passed!`

### Common Issues to Fix

| Error Code | Issue | Fix |
|------------|-------|-----|
| `F401` | Unused import | Remove the import |
| `F841` | Unused variable | Prefix with `_` or remove |
| `ARG001` | Unused argument | Add `# noqa: ARG001` comment or prefix with `_` |
| `RUF005` | Use `[*list, item]` | Replace `list + [item]` with `[*list, item]` |
| `RUF059` | Unused unpacked var | Prefix with `_` (e.g., `_suggested, blob = ...`) |

### Auto-Fix (When Safe)

```powershell
python -m ruff check --fix src/ tests/
```

---

## 2. Code Formatting with Ruff

### Check Formatting

```powershell
python -m ruff format --check src/ tests/
```

### Apply Formatting

```powershell
python -m ruff format src/ tests/
```

**Project Settings** (from `pyproject.toml`):
- Line length: 100 characters
- Quote style: double quotes
- Indent style: spaces
- Line ending: LF

---

## 3. Type Checking with Mypy

### Run Type Check

```powershell
python -m mypy src/pos_core/
```

**Expected Output**: `Success: no issues found in X source files`

### Type Annotation Requirements

The project enforces strict typing for `pos_core.*` modules:

- `disallow_untyped_defs = true`
- `disallow_incomplete_defs = true`
- `check_untyped_defs = true`
- `disallow_untyped_calls = true`

**All functions MUST have**:
- Return type annotations
- Parameter type annotations
- Use `from __future__ import annotations` for forward references

### Common Type Patterns

```python
from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pos_core.config import DataPaths

def my_function(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> pd.DataFrame:
    ...
```

---

## 4. Running Tests with Pytest

### Run Unit Tests (Default - No Live Credentials)

```powershell
python -m pytest tests/ -m "not live" -v --tb=short
```

**Expected Output**: `X passed, Y deselected`

### Run All Tests (Requires WS_BASE, WS_USER, WS_PASS)

```powershell
python -m pytest tests/ -v --tb=short
```

### Test Markers

| Marker | Description |
|--------|-------------|
| `@pytest.mark.live` | Requires real API credentials |
| No marker | Runs with synthetic data |

### If Tests Fail

1. **Read the traceback carefully**
2. **Check if it's a pre-existing failure** (run `git stash` and retest)
3. **Fix the issue** if introduced by your changes
4. **Do NOT skip or ignore failures**

---

## 5. Verification Checklist

After completing all checks, verify:

- [ ] `ruff check` passes with no errors
- [ ] `ruff format --check` passes (no files need formatting)
- [ ] `mypy` passes with no issues
- [ ] `pytest -m "not live"` passes all tests
- [ ] No new warnings introduced

---

## 6. Domain Module Standards

When creating or modifying domain modules (like `pos_core/payments/`, `pos_core/sales/`, `pos_core/transfers/`):

### Required Module Structure

```
pos_core/<domain>/
├── __init__.py          # Exports: raw, core, marts
├── metadata.py          # StageMetadata, read/write/should_run_stage
├── extract.py           # download_<domain>()
├── transform.py         # clean_<domain>()
├── aggregate.py         # aggregate_to_<mart>()
├── raw.py               # fetch(), load() for bronze layer
├── core.py              # fetch(), load() for silver layer
└── marts.py             # fetch_<mart>(), load_<mart>() for gold layer
```

### Required DataPaths Properties

In `src/pos_core/paths.py`:
- `raw_<domain>` → `data/a_raw/<domain>/batch`
- `clean_<domain>` → `data/b_clean/<domain>/batch`
- `mart_<domain>` → `data/c_processed/<domain>`
- Update `ensure_dirs()` to include new paths

### API Function Signatures

```python
# raw.py / core.py
def fetch(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
    *,
    mode: str = "missing",  # or "force"
) -> pd.DataFrame | None:
    ...

def load(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> pd.DataFrame:
    ...
```

---

## 7. Documentation Updates

If your changes affect the public API:

1. **Update module docstrings** in `__init__.py`
2. **Update `pos_core/__init__.py`** docstring if adding new domains
3. **Check `docs/api-reference/`** for outdated documentation

---

## 8. Pre-Commit Hooks (Optional)

The project has pre-commit hooks configured. To use them:

```powershell
# Install pre-commit
pip install pre-commit

# Install hooks
pre-commit install

# Run manually
pre-commit run --all-files
```

**Hooks run**:
- `ruff` (on commit)
- `ruff-format` (on commit)
- `mypy` (on commit)
- `pytest` (on push only)

---

## 9. CI/CD Pipeline

GitHub Actions runs on push/PR to `main` and `develop`:

1. **Matrix testing**: Python 3.10, 3.11, 3.12
2. **pytest**: All tests
3. **mypy**: Type checking (continue-on-error)
4. **ruff**: Linting

Ensure your changes pass locally before pushing.

---

## 10. Common Mistakes to Avoid

| Mistake | Consequence | Prevention |
|---------|-------------|------------|
| Skipping `ruff format` | CI fails | Always run format after check |
| Missing type annotations | mypy errors | Add types to all functions |
| Unused imports | ruff F401 | Remove or use `# noqa: F401` in `__init__.py` only |
| Forgetting `ensure_dirs()` update | Runtime errors | Always update when adding paths |
| Not running tests | Breaking changes slip through | Run pytest before committing |

---

## Quick Troubleshooting

### "ruff not found"

```powershell
pip install ruff
# or
python -m pip install ruff
```

### "mypy not found"

```powershell
pip install mypy types-requests
```

### Tests timeout

```powershell
# Increase timeout or run specific test
python -m pytest tests/test_specific.py -v --timeout=300
```

### Import errors in tests

```powershell
# Ensure package is installed in editable mode
pip install -e .[dev]
```

---

## Summary: The QC Stack

```
┌─────────────────────────────────────────────┐
│  1. ruff check src/ tests/                  │  ← Lint errors
├─────────────────────────────────────────────┤
│  2. ruff format src/ tests/                 │  ← Code formatting
├─────────────────────────────────────────────┤
│  3. mypy src/pos_core/                      │  ← Type checking
├─────────────────────────────────────────────┤
│  4. pytest tests/ -m "not live" --tb=short  │  ← Unit tests
└─────────────────────────────────────────────┘
```

**All four must pass before considering changes complete.**
