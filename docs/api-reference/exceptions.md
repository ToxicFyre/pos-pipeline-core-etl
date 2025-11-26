# Exceptions API Reference

The package defines custom exceptions that are part of the public API. All exceptions inherit from `PosAPIError` for easy catching.

## `PosAPIError`

Base exception for all POS Core ETL errors.

This is the base class for all domain-specific exceptions in the package. Users can catch this exception to handle any POS Core ETL error.

```python
from pos_core.exceptions import PosAPIError

try:
    # Some POS Core ETL operation
    pass
except PosAPIError as e:
    # Handle any POS Core ETL error
    print(f"POS Core ETL error: {e}")
```

## `ConfigError`

Raised when there is a configuration error.

This exception is raised when:
- Invalid configuration values are provided
- Required configuration is missing
- Configuration files cannot be loaded or parsed

```python
from pos_core.exceptions import ConfigError

try:
    # Configuration operation
    pass
except ConfigError as e:
    # Handle configuration error
    print(f"Configuration error: {e}")
```

## `DataQualityError`

Raised when data quality checks fail.

This exception is raised when:
- Required columns are missing from input data
- Data validation fails
- Data quality checks detect critical issues

```python
from pos_core.exceptions import DataQualityError

try:
    # Data quality operation
    pass
except DataQualityError as e:
    # Handle data quality error
    print(f"Data quality error: {e}")
```

