# =============================================================================
# silver_expect.py
# Centralised data quality expectation rules for Silver layer tables.
#
# Structure per table:
#   {
#       "expect":          { rule_name: condition, ... },   # warn only
#       "expect_or_drop":  { rule_name: condition, ... },   # drop bad rows
#       "expect_or_fail":  { rule_name: condition, ... },   # fail the pipeline
#   }
# =============================================================================

SILVER_ORDERS_EXPECTATIONS = {
    "expect": {
        "valid_order_id": "order_id IS NOT NULL",
        "valid_date":     "order_date IS NOT NULL",
    },
    "expect_or_drop": {
        "positive_quantity": "quantity > 0",
        "positive_price":    "unit_price > 0",
    },
    "expect_or_fail": {},
}

SILVER_CUSTOMERS_EXPECTATIONS = {
    "expect": {
        "valid_email": "email LIKE '%@%'",
    },
    "expect_or_drop": {},
    "expect_or_fail": {
        "customer_id_not_null": "customer_id IS NOT NULL",
    },
}
