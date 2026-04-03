import os
from pyspark import pipelines as sdp
from pyspark.sql.functions import *
from pyspark.sql.types import *

catalog_env = os.getenv("catalog_env", "dev")
CATALOG     = f"ecommerce_{catalog_env}"
SCHEMA      = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"

# Load Order table
@sdp.table( #Decorator, should be followed by a function that returns a streaming or batch dataframe
    name=f"{CATALOG}.{SCHEMA}.bronze_order",
    comment = "Order table",
    table_properties = {"layer": "bronze"}
)
def bronze_order():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_schemas/orders")
        .load(f"{VOLUME_PATH}/orders/")
    )


@sdp.table(
    name=f"{CATALOG}.{SCHEMA}.bronze_customer",
    comment = "Customer table",
    table_properties = {"layer":"bronze"}
)
def bronze_customer():
    return(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_schemas/customers")
        .load(f"{VOLUME_PATH}/customers/")
    )

@sdp.table(
    name=f"{CATALOG}.{SCHEMA}.bronze_customer_cdc",
    comment= "Change Data Capture forCustomer data",
    table_properties = {"layer": "bronze"}
)
def bronze_customer_cdc():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_schemas/customer_cdc")
        .load(f"{VOLUME_PATH}/customer_cdc/")
    )
