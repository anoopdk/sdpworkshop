import os
from pyspark import pipelines as sdp
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

catalog_env = os.getenv("catalog_env", "dev")
CATALOG     = f"ecommerce_{catalog_env}"
SCHEMA      = "silver"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"

gold_table_props = {
    "quality": "gold",
    "layer": "gold",
    "pipelines.autoOptimize.managed": "true",
}


def _completed_or_shipped(df):
    return df.filter(col("order_status").isin("completed", "shipped"))

@sdp.materialized_view(
    name=f"{CATALOG}.gold.gold_daily_revenue",
    comment="Daily revenue mart by city and payment method from silver fact tables.",
    table_properties=gold_table_props,
)
@sdp.expect("has_revenue", "daily_revenue > 0")
@sdp.expect("valid_order_date", "order_date IS NOT NULL")
def gold_daily_revenue():
    orders = _completed_or_shipped(
        spark.read.table(f"{CATALOG}.{SCHEMA}.silver_order_customer_fact")
    )

    return (
        orders
        .groupBy("order_date", "shipping_city", "payment_method")
        .agg(
            sum("total_amount").alias("daily_revenue"),
            countDistinct("order_id").alias("order_count"),
            avg("total_amount").alias("avg_order_value"),
            sum("quantity").alias("total_items_sold")
        )
    )


# --- GOLD TABLE 2: Customer 360 View ---
@sdp.materialized_view(
    name=f"{CATALOG}.gold.gold_customer_360",
    comment="Customer 360 mart with purchase behavior, value segments, and recency metrics.",
    table_properties=gold_table_props,
)
def gold_customer_360():
    customer_base = (
        spark.read.table(f"{CATALOG}.{SCHEMA}.silver_customers")
        .where(col("__END_AT").isNull())
        .select(
            "customer_id",
            "customer_sk",
            "customer_name",
            "email",
            "signup_date",
            "tier",
            "city",
            "is_active",
        )
    )

    order_metrics = (
        spark.read.table(f"{CATALOG}.{SCHEMA}.silver_order_customer_fact")
        .filter(col("order_status") == "completed")
        .groupBy("customer_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("total_amount").alias("lifetime_value"),
            avg("total_amount").alias("avg_order_value"),
            min("order_date").alias("first_order_date"),
            max("order_date").alias("last_order_date"),
            sum(when(col("is_high_value_order"), lit(1)).otherwise(lit(0))).alias("high_value_order_count"),
        )
    )

    return (
        customer_base
        .join(order_metrics, "customer_id", "left")
        .withColumn("total_orders", coalesce(col("total_orders"), lit(0)))
        .withColumn("lifetime_value", coalesce(col("lifetime_value"), lit(0.0)))
        .withColumn("avg_order_value", coalesce(col("avg_order_value"), lit(0.0)))
        .withColumn("high_value_order_count", coalesce(col("high_value_order_count"), lit(0)))
        .withColumn("days_since_last_order",
            datediff(current_date(), col("last_order_date")))
        .withColumn("customer_segment",
            when(col("lifetime_value") > 5000, "high_value")
            .when(col("lifetime_value") > 1000, "medium_value")
            .when(col("lifetime_value") > 0, "low_value")
            .otherwise("no_purchases"))
    )


# --- GOLD TABLE 3: Top Products ---
@sdp.materialized_view(
    name=f"{CATALOG}.gold.gold_top_products",
    comment="Product performance mart ranked by revenue, demand, and stock coverage.",
    table_properties=gold_table_props,
)
def gold_top_products():
    fact = _completed_or_shipped(
        spark.read.table(f"{CATALOG}.{SCHEMA}.silver_order_customer_product_fact")
    )

    return (
        fact
        .groupBy("product_id", "product_name", "category", "brand")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("line_amount").alias("total_revenue"),
            sum("quantity").alias("total_quantity"),
            avg("order_unit_price").alias("avg_selling_price"),
            avg("product_unit_price").alias("avg_catalog_price"),
            countDistinct("customer_id").alias("unique_buyers"),
            max("stock_quantity").alias("latest_stock_quantity"),
        )
        .withColumn("revenue_rank",
            dense_rank().over(Window.orderBy(desc("total_revenue"))))
    )


@sdp.materialized_view(
    name=f"{CATALOG}.gold.gold_executive_kpis",
    comment="Executive KPI snapshot for revenue, orders, customers, and product coverage.",
    table_properties=gold_table_props,
)
def gold_executive_kpis():
    fact = _completed_or_shipped(
        spark.read.table(f"{CATALOG}.{SCHEMA}.silver_order_customer_product_fact")
    )

    return fact.agg(
        countDistinct("order_id").alias("orders_count"),
        sum("line_amount").alias("gross_revenue"),
        avg("line_amount").alias("avg_order_value"),
        countDistinct("customer_id").alias("active_customers"),
        countDistinct("product_id").alias("active_products"),
        sum(when(col("product_stock_status") == "OUT_OF_STOCK", lit(1)).otherwise(lit(0))).alias("out_of_stock_order_lines"),
        current_timestamp().alias("kpi_computed_at"),
    )


@sdp.materialized_view(
    name=f"{CATALOG}.gold.gold_order_customer_product_pit_fact",
    comment="Point-in-time order-line fact with temporal SCD2 joins across orders, customers, and products.",
    table_properties=gold_table_props,
)
@sdp.expect("has_order_id", "order_id IS NOT NULL")
@sdp.expect("has_customer_dim", "customer_sk IS NOT NULL")
@sdp.expect("has_product_dim", "product_sk IS NOT NULL")
def gold_order_customer_product_pit_fact():
    # Complex temporal join: map each current order to the correct customer/product SCD2 version
    # valid at the order event timestamp.
    orders = (
        spark.read.table(f"{CATALOG}.{SCHEMA}.silver_orders")
        .where(col("__END_AT").isNull())
        .where(col("order_status").isin("completed", "shipped"))
        .alias("o")
    )

    customers = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_customers").alias("c")
    products = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_products").alias("p")

    order_sequence = struct(
        col("o._event_ts"),
        col("o._ingest_ts"),
        col("o._file_mod_time"),
        col("o._source_file"),
    )

    customer_temporal_condition = (
        (col("o.customer_id") == col("c.customer_id"))
        & (col("c.__START_AT") <= order_sequence)
        & (col("c.__END_AT").isNull() | (col("c.__END_AT") > order_sequence))
    )

    product_temporal_condition = (
        (col("o.product_id") == col("p.product_id"))
        & (col("p.__START_AT") <= order_sequence)
        & (col("p.__END_AT").isNull() | (col("p.__END_AT") > order_sequence))
    )

    return (
        orders
        .join(customers, customer_temporal_condition, "left")
        .join(products, product_temporal_condition, "left")
        .withColumn("line_amount", col("o.quantity") * col("o.unit_price"))
        .withColumn("catalog_price_delta", col("o.unit_price") - col("p.unit_price"))
        .withColumn(
            "price_match_flag",
            when(abs(col("o.unit_price") - col("p.unit_price")) <= lit(0.01), lit("MATCH")).otherwise(lit("MISMATCH")),
        )
        .withColumn(
            "tenure_bucket",
            when(datediff(col("o.order_date"), col("c.signup_date")) < 30, lit("NEW"))
            .when(datediff(col("o.order_date"), col("c.signup_date")) < 180, lit("GROWING"))
            .otherwise(lit("ESTABLISHED")),
        )
        .withColumn(
            "stock_risk_flag",
            when(col("p.stock_quantity") <= lit(0), lit("OOS"))
            .when(col("p.stock_quantity") < lit(20), lit("LOW"))
            .otherwise(lit("OK")),
        )
        .withColumn("_gold_load_ts", current_timestamp())
        .select(
            col("o.order_sk"),
            col("o.order_id"),
            col("o.order_date"),
            col("o._event_ts").alias("order_event_ts"),
            col("o.customer_id"),
            col("o.product_id"),
            col("o.order_status"),
            col("o.payment_method"),
            col("o.shipping_city"),
            col("o.quantity"),
            col("o.unit_price").alias("order_unit_price"),
            col("line_amount"),
            col("c.customer_sk"),
            col("c.customer_name"),
            col("c.tier"),
            col("c.city").alias("customer_city"),
            col("c.signup_date"),
            col("p.product_sk"),
            col("p.product_name"),
            col("p.category"),
            col("p.brand"),
            col("p.unit_price").alias("catalog_unit_price"),
            col("p.stock_quantity"),
            col("catalog_price_delta"),
            col("price_match_flag"),
            col("tenure_bucket"),
            col("stock_risk_flag"),
            col("_gold_load_ts"),
        )
    )
