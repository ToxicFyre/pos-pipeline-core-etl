"""Core (Silver+) layer - Granular, modeled tables.

This layer produces granular, structured data models:
- One row per ticket line / order item
- One row per branch/day at the most granular level
- Preserves individual transaction details

Data directory mapping:
    data/c_processed/core/ → Core models (Silver+) - Granular POS models.
    Currently: data/c_processed/ contains both core and marts outputs.

The core layer bridges staging (clean) data with marts (aggregated) data.
It provides the granular models that marts consume for aggregation.
"""

from pos_core.etl.core.sales_by_ticket import aggregate_by_ticket

__all__ = [
    "aggregate_by_ticket",
]
