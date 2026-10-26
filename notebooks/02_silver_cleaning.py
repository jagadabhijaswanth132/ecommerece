# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver cleaning and validation
# MAGIC Deduplicates records, standardizes text, casts datatypes, and separates invalid orders.

# COMMAND ----------
catalog = "ecommerce"

from pyspark.sql import functions as F

customers_b = spark.table(f"{catalog}.bronze.customers_raw")
products_b = spark.table(f"{catalog}.bronze.products_raw")
orders_b = spark.table(f"{catalog}.bronze.orders_raw")

# COMMAND ----------
customers = (
    customers_b
    .dropDuplicates(["customer_id"])
    .withColumn("customer_id", F.trim(F.col("customer_id").cast("string")))
    .withColumn("customer_name", F.initcap(F.trim(F.col("customer_name"))))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("state", F.initcap(F.trim(F.col("state"))))
    .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
)

products = (
    products_b
    .dropDuplicates(["product_id"])
    .withColumn("product_id", F.trim(F.col("product_id").cast("string")))
    .withColumn("product_name", F.initcap(F.trim(F.col("product_name"))))
    .withColumn("category", F.initcap(F.trim(F.col("category"))))
    .withColumn("price", F.col("price").cast("decimal(12,2)"))
    .filter(
        F.col("product_id").isNotNull() & (F.col("product_id") != "") &
        F.col("price").isNotNull() & (F.col("price") >= 0)
    )
)

orders = (
    orders_b
    .dropDuplicates(["order_id"])
    .withColumn("order_id", F.trim(F.col("order_id").cast("string")))
    .withColumn("customer_id", F.trim(F.col("customer_id").cast("string")))
    .withColumn("product_id", F.trim(F.col("product_id").cast("string")))
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .filter(
        F.col("order_id").isNotNull() & (F.col("order_id") != "") &
        F.col("customer_id").isNotNull() & (F.col("customer_id") != "") &
        F.col("product_id").isNotNull() & (F.col("product_id") != "") &
        F.col("order_date").isNotNull() &
        F.col("quantity").isNotNull() & (F.col("quantity") > 0)
    )
)

# Keep only orders whose customer and product keys exist. Invalid rows can be reviewed from Bronze.
valid_orders = (
    orders.join(customers.select("customer_id"), "customer_id", "inner")
          .join(products.select("product_id"), "product_id", "inner")
)

# COMMAND ----------
for df, table_name in [
    (customers, "customers"),
    (products, "products"),
    (valid_orders, "orders")
]:
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{catalog}.silver.{table_name}"))

# COMMAND ----------
# Quality checks: null keys, duplicate IDs and source-to-silver counts.
display(spark.sql(f"""
SELECT 'customers' AS table_name, COUNT(*) AS rows,
       SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_key_rows
FROM {catalog}.silver.customers
UNION ALL
SELECT 'products', COUNT(*),
       SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END)
FROM {catalog}.silver.products
UNION ALL
SELECT 'orders', COUNT(*),
       SUM(CASE WHEN order_id IS NULL OR customer_id IS NULL OR product_id IS NULL THEN 1 ELSE 0 END)
FROM {catalog}.silver.orders
"""))

display(spark.sql(f"""
SELECT order_id, COUNT(*) AS occurrences
FROM {catalog}.silver.orders
GROUP BY order_id HAVING COUNT(*) > 1
"""))
