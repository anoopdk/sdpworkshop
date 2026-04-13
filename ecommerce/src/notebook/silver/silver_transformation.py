import os
import sys

# ---------------------------------------------------------------------------
# Make the repo-root "config" package importable regardless of working dir
# ---------------------------------------------------------------------------
sys.path.insert(0, "/Workspace/dev/ecommerce/files")

from pyspark import pipelines as sdp
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

from src.config.silver.expect.silver_expect import *
from utils.helper_expect import *
from src.config.common.tags.silver_tags import BASE_PIPELINE_TAGS
# ---------------------------------------------------------------------------
# Environment setup
# ---------------------------------------------------------------------------
catalog_env = os.getenv("catalog_env", "dev")
CATALOG     = f"ecommerce_{catalog_env}"
SCHEMA      = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"

table_props = {
                **BASE_PIPELINE_TAGS,
                "delta.enableRowTracking": "true",
                "delta.enableChangeDataFeed": "true",
            }

# ===========================================================================
# LAYER 1: FAB-5 STAGE TABLES (From Bronze)
# ===========================================================================
# FAB-5 Implementation= Clean + Deduplicate + Late-arrival handling + Standardize + Audit + Surrogate key


# ---------------------------------------------------------------------------
# ORDERS STAGE (clean/dedup/standardize/audit/surrogate key)
# ---------------------------------------------------------------------------
@sdp.materialized_view(
    name=f"{CATALOG}.silver.silver_orders_stage",
    comment="FAB-5 stage for orders from bronze_order.",
    table_properties=table_props,
)
@apply_expectations(SILVER_ORDERS_EXPECTATIONS)
def silver_orders_stage():
    base = (
        spark.read.table(f"{CATALOG}.{SCHEMA}.bronze_order")
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("order_status", lower(trim(col("order_status"))))
        .withColumn("payment_method", lower(trim(col("payment_method"))))
        .withColumn("shipping_city", initcap(trim(col("shipping_city"))))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("total_amount", col("quantity") * col("unit_price"))
        .withColumn("order_year", year(col("order_date")))
        .withColumn("order_month", month(col("order_date")))
        .withColumn("_event_ts", to_timestamp(col("order_date")))
        .withColumn("_updated_at", col("_ingest_ts"))
        .withColumn("_inserted_at", current_timestamp())
    )

    window_dedup = Window.partitionBy("order_id", "_event_ts").orderBy(
        desc("_ingest_ts"), desc("_order_index")
    )

    return (
        base
        .withColumn("_record_number", row_number().over(window_dedup))
        .filter(col("_record_number") == 1)
        .withColumn(
            "order_sk",
            sha2(
                concat_ws(
                    "||",
                    col("order_id"),
                    col("_event_ts").cast("string"),
                    col("_ingest_ts").cast("string"),
                ),
                256,
            ),
        )
        .select(
            "order_sk",
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "total_amount",
            "order_status",
            "order_date",
            "order_year",
            "order_month",
            "payment_method",
            "shipping_city",
            "_event_ts",
            "_ingest_ts",
            "_updated_at",
            "_inserted_at",
            "_file_mod_time",
            "_order_index",
            "_record_number",
        )
    )


# ---------------------------------------------------------------------------
# CUSTOMERS STAGE (clean/dedup/standardize/audit/surrogate key)
# ---------------------------------------------------------------------------
@sdp.materialized_view(
    name=f"{CATALOG}.silver.silver_customers_stage",
    comment="FAB-5 stage for customers from bronze_customer.",
    table_properties=table_props,
)
@apply_expectations(SILVER_CUSTOMERS_EXPECTATIONS)
def silver_customers_stage():
    base = (
        spark.read.table(f"{CATALOG}.{SCHEMA}.bronze_customer")
        .withColumn("customer_name", initcap(trim(col("customer_name"))))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
        .withColumn("tier", upper(trim(col("tier"))))
        .withColumn("city", initcap(trim(col("city"))))
        .withColumn("is_active", col("is_active").cast("boolean"))
        .withColumn("_event_ts", coalesce(to_timestamp(col("signup_date")), col("_ingest_ts")))
        .withColumn("_updated_at", col("_ingest_ts"))
        .withColumn("_inserted_at", current_timestamp())
    )

    window_dedup = Window.partitionBy("customer_id", "_event_ts").orderBy(
        desc("_ingest_ts"), desc("_cust_index")
    )

    return (
        base
        .withColumn("_record_number", row_number().over(window_dedup))
        .filter(col("_record_number") == 1)
        .withColumn(
            "customer_sk",
            sha2(
                concat_ws(
                    "||",
                    col("customer_id"),
                    col("_event_ts").cast("string"),
                    col("_ingest_ts").cast("string"),
                ),
                256,
            ),
        )
        .select(
            "customer_sk",
            "customer_id",
            "customer_name",
            "email",
            "signup_date",
            "tier",
            "city",
            "is_active",
            "_event_ts",
            "_ingest_ts",
            "_updated_at",
            "_inserted_at",
            "_file_mod_time",
            "_cust_index",
            "_record_number",
        )
    )


# ---------------------------------------------------------------------------
# PRODUCTS STAGE (clean/dedup/standardize/audit/surrogate key)
# ---------------------------------------------------------------------------
@sdp.materialized_view(
    name=f"{CATALOG}.silver.silver_products_stage",
    comment="FAB-5 stage for product CDC from bronze_product.",
    table_properties=table_props,
)
def silver_products_stage():
    base = (
        spark.read.table(f"{CATALOG}.{SCHEMA}.bronze_product")
        .withColumn("operation", upper(trim(col("operation"))))
        .withColumn("product_name", initcap(trim(col("product_name"))))
        .withColumn("category", initcap(trim(col("category"))))
        .withColumn("brand", upper(trim(col("brand"))))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("stock_quantity", col("stock_quantity").cast("int"))
        .withColumn("is_active", col("is_active").cast("boolean"))
        .withColumn("cdc_timestamp", to_timestamp(col("cdc_timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("_event_ts", col("cdc_timestamp"))
        .withColumn("_updated_at", col("_ingest_ts"))
        .withColumn("_inserted_at", current_timestamp())
    )

    window_dedup = Window.partitionBy("product_id", "_event_ts").orderBy(
        desc("_ingest_ts"), desc("_prod_index")
    )

    return (
        base
        .withColumn("_record_number", row_number().over(window_dedup))
        .filter(col("_record_number") == 1)
        .withColumn(
            "product_sk",
            sha2(
                concat_ws(
                    "||",
                    col("product_id"),
                    col("_event_ts").cast("string"),
                    col("_ingest_ts").cast("string"),
                ),
                256,
            ),
        )
        .select(
            "product_sk",
            "product_id",
            "product_name",
            "category",
            "brand",
            "unit_price",
            "stock_quantity",
            "is_active",
            "operation",
            "cdc_timestamp",
            "_event_ts",
            "_ingest_ts",
            "_updated_at",
            "_inserted_at",
            "_file_mod_time",
            "_prod_index",
            "_record_number",
        )
    )


# ===========================================================================
# LAYER 2: SCD2 TABLES (Current + History)
# ===========================================================================

sdp.create_streaming_table(
    name=f"{CATALOG}.silver.silver_orders",
    comment="Orders SCD2 table with history and surrogate key.",
    table_properties=table_props,
    cluster_by=["order_id", "order_date"],
)

sdp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.silver_orders",
    source=f"{CATALOG}.silver.silver_orders_stage",
    keys=["order_id"],
    sequence_by=struct(col("_event_ts"), col("_ingest_ts"), col("_order_index")),
    except_column_list=["_record_number"],
    stored_as_scd_type=2,
)

sdp.create_streaming_table(
    name=f"{CATALOG}.silver.silver_customers",
    comment="Customers SCD2 table with history and surrogate key.",
    table_properties=table_props,
    cluster_by=["customer_id"],
)

sdp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.silver_customers",
    source=f"{CATALOG}.silver.silver_customers_stage",
    keys=["customer_id"],
    sequence_by=struct(col("_event_ts"), col("_ingest_ts"), col("_cust_index")),
    except_column_list=["_record_number"],
    stored_as_scd_type=2,
)

sdp.create_streaming_table(
    name=f"{CATALOG}.silver.silver_products",
    comment="Products SCD2 table with history and surrogate key from CDC feed.",
    table_properties=table_props,
    cluster_by=["product_id"],
)

sdp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.silver_products",
    source=f"{CATALOG}.silver.silver_products_stage",
    keys=["product_id"],
    sequence_by=struct(col("_event_ts"), col("_ingest_ts"), col("_prod_index")),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["_record_number"],
    stored_as_scd_type=2,
)

# ===========================================================================
# LAYER 3: DERIVED FACT TABLES (Built on Silver Tables)
# ===========================================================================

sdp.create_streaming_table(
    name=f"{CATALOG}.silver.silver_order_customer_fact",
    comment="Append-only derived fact combining current order + customer records.",
    table_properties=table_props,
    cluster_by=["customer_id", "order_date"],
)


@sdp.append_flow(
    target=f"{CATALOG}.silver.silver_order_customer_fact",
    name="silver_order_customer_fact_append_flow",
    comment="Facts are append_flow by design: immutable analytical events with no in-place updates.",
)
def silver_order_customer_fact_append_flow():
    # Read current dimension snapshots inline to avoid extra helper MVs and reduce compute overhead.
    orders = spark.readStream.table(f"{CATALOG}.silver.silver_orders").where(col("__END_AT").isNull())
    customers = spark.read.table(f"{CATALOG}.silver.silver_customers").where(col("__END_AT").isNull())

    return (
        orders.alias("o")
        .join(customers.alias("c"), on="customer_id", how="left")
        .withColumn(
            "order_amount_band",
            when(col("o.total_amount") >= lit(1000), lit("HIGH"))
            .when(col("o.total_amount") >= lit(300), lit("MEDIUM"))
            .otherwise(lit("LOW")),
        )
        .withColumn("customer_tenure_days", datediff(col("o.order_date"), col("c.signup_date")))
        .withColumn("is_high_value_order", col("o.total_amount") >= lit(1000))
        .withColumn("_fact_load_id", current_timestamp())
        .select(
            col("o.order_sk"),
            col("o.order_id"),
            col("o.customer_id"),
            col("o.product_id"),
            col("o.quantity"),
            col("o.unit_price"),
            col("o.total_amount"),
            col("o.order_status"),
            col("o.order_date"),
            col("o.order_year"),
            col("o.order_month"),
            col("o.payment_method"),
            col("o.shipping_city"),
            col("c.customer_sk"),
            col("c.customer_name"),
            col("c.email"),
            col("c.signup_date"),
            col("c.tier"),
            col("c.city").alias("customer_city"),
            col("c.is_active").alias("customer_is_active"),
            col("order_amount_band"),
            col("customer_tenure_days"),
            col("is_high_value_order"),
            col("_fact_load_id"),
        )
    )


sdp.create_streaming_table(
    name=f"{CATALOG}.silver.silver_order_customer_product_fact",
    comment="Append-only derived fact combining current order + customer + product records.",
    table_properties=table_props,
    cluster_by=["product_id", "order_date"],
)


@sdp.append_flow(
    target=f"{CATALOG}.silver.silver_order_customer_product_fact",
    name="silver_order_customer_product_fact_append_flow",
    comment="Facts are append_flow by design: keep an immutable timeline and simplify downstream gold aggregation.",
)
def silver_order_customer_product_fact_append_flow():
    # Inline current snapshots keep the graph simpler than maintaining dedicated current-state MVs.
    orders = spark.readStream.table(f"{CATALOG}.silver.silver_orders").where(col("__END_AT").isNull())
    customers = spark.read.table(f"{CATALOG}.silver.silver_customers").where(col("__END_AT").isNull())
    products = spark.read.table(f"{CATALOG}.silver.silver_products").where(col("__END_AT").isNull())

    return (
        orders.alias("o")
        .join(customers.alias("c"), on="customer_id", how="left")
        .join(products.alias("p"), on="product_id", how="left")
        .withColumn("line_amount", col("o.quantity") * col("o.unit_price"))
        .withColumn(
            "product_stock_status",
            when(col("p.stock_quantity") <= lit(0), lit("OUT_OF_STOCK"))
            .when(col("p.stock_quantity") < lit(20), lit("LOW_STOCK"))
            .otherwise(lit("IN_STOCK")),
        )
        .withColumn("is_price_mismatch", abs(col("o.unit_price") - col("p.unit_price")) > lit(0.01))
        .withColumn("_fact_load_id", current_timestamp())
        .select(
            col("o.order_sk"),
            col("c.customer_sk"),
            col("p.product_sk"),
            col("o.order_id"),
            col("o.customer_id"),
            col("o.product_id"),
            col("o.order_date"),
            col("o.order_status"),
            col("o.quantity"),
            col("o.unit_price").alias("order_unit_price"),
            col("line_amount"),
            col("c.customer_name"),
            col("c.tier"),
            col("c.city").alias("customer_city"),
            col("p.product_name"),
            col("p.category"),
            col("p.brand"),
            col("p.unit_price").alias("product_unit_price"),
            col("p.stock_quantity"),
            col("product_stock_status"),
            col("is_price_mismatch"),
            col("_fact_load_id"),
        )
    )