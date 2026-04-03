from pyspark import pipelines as sdp
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

CATALOG     = "ecommerce"
SCHEMA      = "silver"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"

@sdp.materialized_view(
    name="ecommerce.gold.gold_daily_revenue",
    comment="Daily revenue by city and payment method. Powers the revenue dashboard.",
    table_properties={"quality": "gold"}
)
@sdp.expect("has_revenue", "daily_revenue > 0")
def gold_daily_revenue():
    return (
        spark.read.table(f"{CATALOG}.{SCHEMA}.silver_orders")
        .filter(col("order_status").isin("completed", "shipped"))
        .groupBy("order_date", "shipping_city", "payment_method")
        .agg(
            sum("total_amount").alias("daily_revenue"),
            count("order_id").alias("order_count"),
            avg("total_amount").alias("avg_order_value"),
            sum("quantity").alias("total_items_sold")
        )
    )


# --- GOLD TABLE 2: Customer 360 View ---
@sdp.materialized_view(
    name="ecommerce.gold.gold_customer_360",
    comment="Unified customer profile with order history metrics.",
    table_properties={"quality": "gold"}
)
def gold_customer_360():
    order_metrics = (
        spark.read.table(f"{CATALOG}.{SCHEMA}.silver_orders")
        .filter(col("order_status") == "completed")
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("lifetime_value"),
            avg("total_amount").alias("avg_order_value"),
            min("order_date").alias("first_order_date"),
            max("order_date").alias("last_order_date")
        )
    )
    return (
        spark.read.table(f"{CATALOG}.{SCHEMA}.silver_customers")
        .join(order_metrics, "customer_id", "left")
        .withColumn("total_orders", coalesce(col("total_orders"), lit(0)))
        .withColumn("lifetime_value", coalesce(col("lifetime_value"), lit(0.0)))
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
    name="ecommerce.gold.gold_top_products",
    comment="Product performance ranking by revenue and order volume.",
    table_properties={"quality": "gold"}
)
def gold_top_products():
    return (
        spark.read.table(f"{CATALOG}.{SCHEMA}.silver_orders")
        .filter(col("order_status").isin("completed", "shipped"))
        .groupBy("product_id")
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_revenue"),
            sum("quantity").alias("total_quantity"),
            avg("unit_price").alias("avg_selling_price"),
            countDistinct("customer_id").alias("unique_buyers")
        )
        .withColumn("revenue_rank",
            dense_rank().over(Window.orderBy(desc("total_revenue"))))
    )
