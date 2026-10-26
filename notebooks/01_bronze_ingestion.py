# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze ingestion
# MAGIC Reads the three source CSVs with minimal transformation and stores Delta raw tables.
# MAGIC Before running, create a Unity Catalog Volume and upload the CSVs into its `input` folder.

# COMMAND ----------
# Update these values for your workspace.
catalog = "ecommerce"
volume_schema = "default"
volume_name = "project_files"
input_path = f"/Volumes/{catalog}/{volume_schema}/{volume_name}/input"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

# COMMAND ----------
from pyspark.sql import functions as F

def read_source(file_name):
    path = f"{input_path}/{file_name}"
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(path)
        .withColumn("load_timestamp", F.current_timestamp())
        .withColumn("source_file_name", F.lit(file_name))
    )

customers_raw = read_source("customers.csv")
products_raw = read_source("products.csv")
orders_raw = read_source("orders.csv")

# COMMAND ----------
(customers_raw.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(f"{catalog}.bronze.customers_raw"))
(products_raw.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(f"{catalog}.bronze.products_raw"))
(orders_raw.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(f"{catalog}.bronze.orders_raw"))

# COMMAND ----------
display(spark.sql(f"""
SELECT 'customers_raw' AS table_name, COUNT(*) AS row_count FROM {catalog}.bronze.customers_raw
UNION ALL SELECT 'products_raw', COUNT(*) FROM {catalog}.bronze.products_raw
UNION ALL SELECT 'orders_raw', COUNT(*) FROM {catalog}.bronze.orders_raw
"""))
