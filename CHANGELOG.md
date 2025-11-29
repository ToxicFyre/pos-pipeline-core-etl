# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

### Added

- **ETL Pipeline**: Extract, transform, and load POS payment and sales data
  - High-level `build_payments_dataset()` API for payments ETL
  - Low-level utilities for sales detail ETL
  - Incremental processing with smart date range chunking
  - Multi-branch support with code window tracking

- **Forecasting**: ARIMA-based time series forecasting
  - `run_payments_forecast()` API for generating forecasts
  - Automatic hyperparameter selection
  - Cash flow deposit schedule generation
  - Support for multiple metrics and branches

- **Quality Assurance**: Automated data validation
  - `run_payments_qa()` API for data quality checks
  - Missing days detection
  - Duplicate detection
  - Statistical anomaly detection (z-score)
  - Zero method flag detection

- **Public API**: Clean, stable API surface
  - Domain-specific exceptions (`PosAPIError`, `ConfigError`, `DataQualityError`)
  - Type hints throughout
  - Comprehensive docstrings

- **Documentation**: Complete documentation
  - README with examples
  - mkdocs-based documentation site
  - API reference
  - Runnable example scripts

- **Developer Experience**:
  - mypy type checking configuration
  - ruff linting configuration
  - GitHub Actions CI workflow
  - Examples directory with runnable scripts

### Changed

- Initial public release

### Security

- `.gitignore` configured to prevent committing secrets and sensitive data
- Environment variable-based configuration for credentials

---

## [Unreleased]

### Added

- **Query Functions**: High-level query functions for intelligent data access
  - `get_sales()` - Retrieve sales data with automatic ETL stage execution based on metadata
  - `get_payments()` - Retrieve payments data with automatic ETL stage execution based on metadata
  - `get_payments_forecast()` - Generate payments forecasts with automatic data preparation
  - All query functions support metadata-aware stage execution (only runs missing/outdated stages)
  - Support for `refresh` parameter to force re-running all ETL stages
  - Support for multiple aggregation levels in sales queries (ticket, group, day)

- **Configuration API**: Enhanced configuration with explicit path management
  - `PaymentsETLConfig.from_data_root()` - Factory method to create config from data root path
  - `PaymentsETLConfig.from_root()` - Alias for consistency with SalesETLConfig
  - `PaymentsPaths` dataclass for centralized path configuration
  - Support for configurable `data_root` instead of hardcoded directories

- **Branch Configuration Utilities**: New module for branch code window management
  - `CodeWindow` dataclass for representing time windows when branch codes are valid
  - `load_branch_segments_from_json()` - Load branch code windows from configuration files
  - Separated into `branch_config.py` to avoid circular import issues

### Changed

- **ETL API**: Refactored payments ETL to use explicit configuration objects
  - `build_payments_dataset()` now accepts `PaymentsETLConfig` instead of hardcoded paths
  - All ETL stages now use configurable paths from `PaymentsETLConfig.paths`
  - Improved error handling with explicit exceptions for missing files and invalid configurations
  - Query functions automatically manage ETL stage execution based on metadata

### Deprecated

- (Future deprecations will be listed here)

### Removed

- (Future removals will be listed here)

### Fixed

- (Future fixes will be listed here)

### Security

- (Future security updates will be listed here)

---

## Release Notes Template

When adding a new release, use this structure:

```markdown
## [X.Y.Z] - YYYY-MM-DD

### Added
- New features

### Changed
- Changes to existing functionality

### Deprecated
- Features that will be removed in a future release

### Removed
- Removed features

### Fixed
- Bug fixes

### Security
- Security updates
```

