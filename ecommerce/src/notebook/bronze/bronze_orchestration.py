import os
from pyspark import pipelines as sdp
from pyspark.sql.functions import *
from pyspark.sql.types import *

catalog_env = os.getenv("catalog_env", "dev")
CATALOG     = f"ecommerce_{catalog_env}"
SCHEMA      = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/source_files"

table_props = {
                "delta.enableRowTracking": "true",
                "delta.enableChangeDataFeed": "true",
            }

@sdp.table(
    name=f"{CATALOG}.{SCHEMA}.bronze_order",
    comment="Master bronze orders source table.",
    table_properties=table_props
)
def bronze_order():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_schemas/orders")
        .load(f"{VOLUME_PATH}/orders/")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_order_index", lit(0).cast("long"))
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_file_mod_time", col("_metadata.file_modification_time"))
    )

@sdp.table(
    name=f"{CATALOG}.{SCHEMA}.bronze_customer",
    comment = "Customer table",
    table_properties = table_props
)
def bronze_customer():
    return(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_schemas/customers")
        .load(f"{VOLUME_PATH}/customers/")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_file_mod_time", col("_metadata.file_modification_time"))
        .withColumn("_cust_index", lit(0).cast("long"))
        .withColumn("_source_file", col("_metadata.file_path"))
    )

@sdp.table(
    name=f"{CATALOG}.{SCHEMA}.bronze_product",
    comment="Change Data Capture feed for the Product catalogue dimension. "
            "Contains INSERT / UPDATE / DELETE operations with cdc_timestamp and operation columns.",
    table_properties=table_props
)
def bronze_product():
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_schemas/products")
        .load(f"{VOLUME_PATH}/products/")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_file_mod_time", col("_metadata.file_modification_time"))
        .withColumn("_prod_index", lit(0).cast("long"))
        .withColumn("_source_file", col("_metadata.file_path"))
    )
