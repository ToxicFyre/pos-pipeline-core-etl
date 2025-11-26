"""Domain-specific exceptions for POS Core ETL.

This module defines custom exceptions that are part of the public API.
All exceptions inherit from PosAPIError for easy catching.
"""


class PosAPIError(Exception):
    """Base exception for all POS Core ETL errors.

    This is the base class for all domain-specific exceptions in the package.
    Users can catch this exception to handle any POS Core ETL error.
    """

    pass


class ConfigError(PosAPIError):
    """Raised when there is a configuration error.

    This exception is raised when:
    - Invalid configuration values are provided
    - Required configuration is missing
    - Configuration files cannot be loaded or parsed
    """

    pass


class DataQualityError(PosAPIError):
    """Raised when data quality checks fail.

    This exception is raised when:
    - Required columns are missing from input data
    - Data validation fails
    - Data quality checks detect critical issues
    """

    pass

