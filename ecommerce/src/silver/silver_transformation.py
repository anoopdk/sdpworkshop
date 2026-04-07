import os
import sys

# ---------------------------------------------------------------------------
# Make the repo-root "config" package importable regardless of working dir
# ---------------------------------------------------------------------------
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, "/Workspace/dev/ecommerce/files")

from pyspark import pipelines as sdp
from pyspark.sql.functions import *
from pyspark.sql.types import *

from config.expect.silver_expect import *
from utils.helper_expect import *
# ---------------------------------------------------------------------------
# Environment setup
# ---------------------------------------------------------------------------
catalog_env = os.getenv("catalog_env", "dev")
CATALOG     = f"ecommerce_{catalog_env}"
SCHEMA      = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"


# ---------------------------------------------------------------------------
# Decorator Factory: Apply Expectations Cleanly
# ---------------------------------------------------------------------------
# def apply_expectations(expectations: dict):
#     """
#     Converts expectation config into chained SDP decorators.
#     """
#     def decorator(fn):
#         for name, condition in expectations.get("expect", {}).items():
#             fn = sdp.expect(name, condition)(fn)

#         for name, condition in expectations.get("expect_or_drop", {}).items():
#             fn = sdp.expect_or_drop(name, condition)(fn)

#         for name, condition in expectations.get("expect_or_fail", {}).items():
#             fn = sdp.expect_or_fail(name, condition)(fn)

#         return fn

#     return decorator


# ---------------------------------------------------------------------------
# SILVER TABLE 1: Cleaned Orders
# ---------------------------------------------------------------------------
@sdp.materialized_view(
    name=f"{CATALOG}.silver.silver_orders",
    comment="Cleaned and enriched orders. Invalid records are dropped.",
    table_properties={"quality": "silver"}
)
@apply_expectations(SILVER_ORDERS_EXPECTATIONS)
def silver_orders():
    return (
        spark.read.table(f"{CATALOG}.{SCHEMA}.bronze_order")
        .withColumn("total_amount", col("quantity") * col("unit_price"))
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("order_year", year(col("order_date")))
        .withColumn("order_month", month(col("order_date")))
        .select(
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "total_amount",
            "order_status", "order_date", "order_year", "order_month",
            "payment_method", "shipping_city"
        )
    )


# ---------------------------------------------------------------------------
# SILVER TABLE 2: Cleaned Customers
# ---------------------------------------------------------------------------
@sdp.materialized_view(
    name=f"{CATALOG}.silver.silver_customers",
    comment="Cleaned customer profiles with standardised fields.",
    table_properties={"quality": "silver"}
)
@apply_expectations(SILVER_CUSTOMERS_EXPECTATIONS)
def silver_customers():
    return (
        spark.read.table(f"{CATALOG}.{SCHEMA}.bronze_customer")
        .withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
        .withColumn("tier", upper(col("tier")))
        .withColumn("city", initcap(col("city")))
        .select(
            "customer_id", "customer_name", "email",
            "signup_date", "tier", "city", "is_active"
        )
    )


# ---------------------------------------------------------------------------
# SILVER TABLE 3: SCD Type-2 Customer Dimension
# ---------------------------------------------------------------------------

# Declare the target table
sdp.create_streaming_table(
    name=f"{CATALOG}.silver.silver_customer_dim_scd2",
    comment="SCD Type 2 for customers. Tracks full history of changes."
)

# Define CDC flow
sdp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.silver_customer_dim_scd2",
    source=f"{CATALOG}.{SCHEMA}.bronze_customer_cdc",
    keys=["customer_id"],
    sequence_by=col("cdc_timestamp"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "cdc_timestamp"],
    stored_as_scd_type=2
)

# import os
# from pyspark import pipelines as sdp
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# catalog_env = os.getenv("catalog_env", "dev")
# CATALOG     = f"ecommerce_{catalog_env}"
# SCHEMA      = "bronze"
# VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"

# @sdp.materialized_view(
#     name=f"{CATALOG}.silver.silver_orders",
#     comment="Cleaned and enriched orders. Invalid records are dropped.",
#     table_properties={"quality": "silver"}
# )
# @sdp.expect("valid_order_id", "order_id IS NOT NULL")
# @sdp.expect_or_drop("positive_quantity", "quantity > 0")
# @sdp.expect_or_drop("positive_price", "unit_price > 0")
# @sdp.expect("valid_date", "order_date IS NOT NULL")
# def silver_orders():
#     return (
#         spark.read.table(f"{CATALOG}.{SCHEMA}.bronze_order")
#         .withColumn("total_amount", col("quantity") * col("unit_price"))
#         .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
#         .withColumn("order_year", year(col("order_date")))
#         .withColumn("order_month", month(col("order_date")))
#         .select(
#             "order_id", "customer_id", "product_id",
#             "quantity", "unit_price", "total_amount",
#             "order_status", "order_date", "order_year", "order_month",
#             "payment_method", "shipping_city"
#         )
#     )

# # --- SILVER TABLE 2: Cleaned Customers ---
# @sdp.materialized_view(
#     name=f"{CATALOG}.silver.silver_customers",
#     comment="Cleaned customer profiles with standardised fields.",
#     table_properties={"quality": "silver"}
# )
# @sdp.expect_or_fail("customer_id_not_null", "customer_id IS NOT NULL")
# @sdp.expect("valid_email", "email LIKE '%@%'")
# def silver_customers():
#     return (
#         spark.read.table(f"{CATALOG}.{SCHEMA}.bronze_customer")
#         .withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
#         .withColumn("tier", upper(col("tier")))
#         .withColumn("city", initcap(col("city")))
#         .select(
#             "customer_id", "customer_name", "email",
#             "signup_date", "tier", "city", "is_active"
#         )
#     )

# # Declare the target table first
# sdp.create_streaming_table(
#     name=f"{CATALOG}.silver.silver_customer_dim_scd2",
#     comment="SCD Type 2 for customers. Tracks full history of changes."
# )

# # Then define the CDC flow
# sdp.create_auto_cdc_flow(
#     target=f"{CATALOG}.silver.silver_customer_dim_scd2",
#     source=f"{CATALOG}.{SCHEMA}.bronze_customer_cdc",
#     keys=["customer_id"],
#     sequence_by=col("cdc_timestamp"),
#     apply_as_deletes=expr("operation = 'DELETE'"),
#     except_column_list=["operation", "cdc_timestamp"],
#     stored_as_scd_type=2
# )
