"""Core (Silver+) layer - Atomic fact tables at the most granular business grain.

This layer represents the **atomic facts** - the most granular meaningful units
of data that the business cares about. These are NOT aggregations, but rather
the cleaned and normalized representations of the source data at its natural grain.

Grain Definitions (Ground Truth)
--------------------------------
1. **Payments Core Fact** (``fact_payments_ticket``):
   - Grain: ticket × payment method
   - One row per payment line on a ticket
   - The POS payments export does not expose deeper item-level payment data
   - Located in: staging layer output (``b_clean/payments/``)

2. **Sales Core Fact** (``fact_sales_item_line``):
   - Grain: item/modifier line
   - One row per item or modifier on a ticket
   - Multiple rows can share the same ``ticket_id`` / ``order_id``
   - Located in: staging layer output (``b_clean/sales/``)

Data Flow
---------
- **Bronze (Raw)**: ``a_raw/`` → Direct Wansoft exports, unchanged
- **Silver (Staging)**: ``b_clean/`` → Cleaned tables containing core facts
- **Silver+ (Core)**: The staging output IS the core fact for both domains
- **Gold (Marts)**: ``c_processed/`` → All aggregations beyond core grain

Key Rule
--------
- For **sales**: anything aggregated beyond item/modifier line is **gold**, not silver/core
- For **payments**: ticket × payment method is the atomic fact (silver/core)

Note: The ``aggregate_by_ticket`` function has been moved to the **marts** layer
since it produces ticket-level aggregates (beyond the item-line core grain).
It is re-exported here for backwards compatibility.
"""

# Re-export aggregate_by_ticket from marts for backwards compatibility
# NOTE: This function is now in marts/ because ticket-level aggregation
# is a mart (Gold layer), not a core fact. The core sales fact is at
# item/modifier line grain (the staging output).
from pos_core.etl.marts.sales_by_ticket import aggregate_by_ticket

__all__ = [
    "aggregate_by_ticket",  # Backwards-compat re-export from marts
]
