# Installation

This guide covers installing POS Core ETL and setting up your development environment.

## Requirements

- **Python**: 3.10 or higher
- **Operating System**: Windows, macOS, or Linux

## Production Installation

Install from PyPI:

```bash
pip install pos-core-etl
```

## Development Installation

### 1. Clone the Repository

```bash
git clone https://github.com/ToxicFyre/pos-pipeline-core-etl.git
cd pos-pipeline-core-etl
```

### 2. Install with Development Dependencies

```bash
pip install -e .[dev]
```

This installs the package in editable mode with all development dependencies including:
- Testing tools (pytest)
- Code quality tools (ruff, mypy)
- Documentation tools (mkdocs)

### Using Conda (Recommended)

If you're using Anaconda or Miniconda:

```bash
# Create environment
conda create -n pos-etl python=3.10
conda activate pos-etl

# Install package
pip install -e .[dev]
```

## Verify Installation

Test that the package is installed correctly:

```python
from pos_core import DataPaths
from pos_core.payments import core, marts
from pos_core.sales import core as sales_core

print("POS Core ETL installed successfully!")
```

## Dependencies

The package requires the following dependencies (installed automatically):

- **pandas** >= 1.3.0 - Data manipulation
- **numpy** >= 1.20.0 - Numerical computing
- **requests** >= 2.25.0 - HTTP client for data extraction
- **beautifulsoup4** >= 4.9.0 - HTML parsing
- **statsmodels** >= 0.12.0 - Time series forecasting
- **openpyxl** >= 3.0.0 - Excel file handling

## Next Steps

After installation:

1. **[Configure your environment](configuration.md)** - Set up branch configuration and credentials
2. **[Try the quickstart](quickstart.md)** - Run your first ETL pipeline
3. **[Explore examples](examples.md)** - See complete working examples
