# Installation

## Requirements

- Python 3.10 or higher
- POS system with Wansoft-style HTTP exports (see [Concepts](concepts.md) for details)

## Install from PyPI

```bash
pip install pos-core-etl
```

## Install for Development

```bash
git clone https://github.com/ToxicFyre/pos-pipeline-core-etl.git
cd pos-pipeline-core-etl
pip install -e .[dev]
```

## Dependencies

The package requires:
- pandas >= 1.3.0
- numpy >= 1.20.0
- requests >= 2.25.0
- beautifulsoup4 >= 4.9.0
- statsmodels >= 0.12.0
- openpyxl >= 3.0.0

Optional dependencies (for development):
- pytest >= 7.0
- mypy >= 1.0.0
- ruff >= 0.1.0
- black >= 23.0.0

## Next Steps

1. Create your `sucursales.json` configuration file (see [Configuration](configuration.md))
2. Set up your data directory structure (see [Concepts](concepts.md))
3. Try the [Quickstart](quickstart.md) guide

