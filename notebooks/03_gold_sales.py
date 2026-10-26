# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold sales model
# MAGIC Joins Silver orders to customer and product dimensions, calculates sales, and builds reporting summaries.

# COMMAND ----------
catalog = "ecommerce"
from pyspark.sql import functions as F

orders = spark.table(f"{catalog}.silver.orders")
customers = spark.table(f"{catalog}.silver.customers")
products = spark.table(f"{catalog}.silver.products")

sales_detail = (
    orders.alias("o")
    .join(customers.alias("c"), "customer_id", "inner")
    .join(products.alias("p"), "product_id", "inner")
    .select(
        F.col("o.order_id"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
        F.col("c.customer_name"),
        F.col("c.city"),
        F.col("c.state"),
        F.col("o.product_id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("o.quantity"),
        F.col("p.price"),
        (F.col("o.quantity") * F.col("p.price")).cast("decimal(14,2)").alias("sales_amount")
    )
)

(sales_detail.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.gold.sales_detail"))

# COMMAND ----------
daily_sales_summary = (
    sales_detail.groupBy("order_date")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("units_sold"),
        F.sum("sales_amount").alias("total_sales")
    )
    .orderBy("order_date")
)
(daily_sales_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.gold.daily_sales_summary"))

# Additional summaries for analysis
state_sales_summary = (
    sales_detail.groupBy("state")
    .agg(F.countDistinct("order_id").alias("order_count"),
         F.sum("sales_amount").alias("total_sales"))
)
(state_sales_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.gold.state_sales_summary"))

category_sales_summary = (
    sales_detail.groupBy("category")
    .agg(F.sum("quantity").alias("units_sold"),
         F.sum("sales_amount").alias("total_sales"))
)
(category_sales_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.gold.category_sales_summary"))

# COMMAND ----------
display(spark.table(f"{catalog}.gold.sales_detail").orderBy("order_date", "order_id"))
display(spark.table(f"{catalog}.gold.daily_sales_summary"))

# COMMAND ----------
# Final validations
display(spark.sql(f"""
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT order_id) AS distinct_orders,
       SUM(CASE WHEN customer_id IS NULL OR product_id IS NULL THEN 1 ELSE 0 END) AS null_key_rows,
       SUM(CASE WHEN sales_amount != quantity * price THEN 1 ELSE 0 END) AS incorrect_sales_rows
FROM {catalog}.gold.sales_detail
"""))
