# Databricks E-commerce Medallion Project

## Goal
Implement the supplied project brief: ingest `customers.csv`, `products.csv`, and `orders.csv` into Bronze Delta tables, clean and validate them in Silver, and publish business-ready sales tables in Gold.

## Architecture
CSV files → Bronze raw Delta tables → Silver cleaned Delta tables → Gold sales and summary tables → SQL/Power BI reporting.

## Included
- `data/customers.csv` — 10 customers
- `data/products.csv` — 10 products
- `data/orders.csv` — 12 orders
- `notebooks/01_bronze_ingestion.py`
- `notebooks/02_silver_cleaning.py`
- `notebooks/03_gold_sales.py`
- `sql/reporting_queries.sql`

## 1. Create a Unity Catalog Volume
In a Databricks SQL editor or notebook, adjust catalog/schema names if needed and run:
```sql
CREATE CATALOG IF NOT EXISTS ecommerce;
CREATE SCHEMA IF NOT EXISTS ecommerce.bronze;
CREATE SCHEMA IF NOT EXISTS ecommerce.silver;
CREATE SCHEMA IF NOT EXISTS ecommerce.gold;
CREATE SCHEMA IF NOT EXISTS ecommerce.default;
CREATE VOLUME IF NOT EXISTS ecommerce.default.project_files;
```
Upload the three CSVs to the Volume's `input` directory. Expected paths:
`/Volumes/ecommerce/default/project_files/input/customers.csv`
`/Volumes/ecommerce/default/project_files/input/products.csv`
`/Volumes/ecommerce/default/project_files/input/orders.csv`

If your workspace doesn't permit creating a catalog, use a catalog you already have access to and change `catalog` in all notebooks. The Volume's schema/name must match `volume_schema` and `volume_name` in the Bronze notebook.

## 2. Import and run notebooks
Import the three `.py` notebook source files into Databricks Workspace. Run in order:
1. `01_bronze_ingestion`
2. `02_silver_cleaning`
3. `03_gold_sales`

The notebooks use Unity Catalog three-part table names and `/Volumes/...` paths. A Unity Catalog Volume must exist at that path; a normal local path or `dbfs:/` path is not interchangeable with `/Volumes/...`.

## 3. Tables created
| Layer | Tables |
|---|---|
| Bronze | `ecommerce.bronze.customers_raw`, `ecommerce.bronze.products_raw`, `ecommerce.bronze.orders_raw` |
| Silver | `ecommerce.silver.customers`, `ecommerce.silver.products`, `ecommerce.silver.orders` |
| Gold | `ecommerce.gold.sales_detail`, `ecommerce.gold.daily_sales_summary`, `ecommerce.gold.state_sales_summary`, `ecommerce.gold.category_sales_summary` |

## 4. Example reporting
Run queries in `sql/reporting_queries.sql`. Use `ecommerce.gold.sales_detail` as the main Power BI/reporting dataset.

## 5. Validation and assumptions
- Deduplicate customer and product records by their IDs, and order records by `order_id`.
- Trim and standardize customer/product text.
- Convert `order_date` to DATE; `quantity` to positive INT; `price` to non-negative DECIMAL.
- Exclude orders with missing/invalid required fields or customer/product keys that do not exist in Silver dimensions. Bronze remains unchanged for traceability.
- `sales_amount = quantity * price`.
- The sample data is synthetic and uses INR-like numeric prices; currency is not stored as a separate column.
- Bronze uses overwrite for a repeatable student lab. For production incremental ingestion, add file-arrival tracking and idempotent merge logic rather than overwriting the full raw tables.

## 6. Workflow
Create a Databricks Workflow (Lakeflow Job) with three notebook tasks and dependencies:
`Bronze ingestion` → `Silver cleaning` → `Gold sales`.
Use the same compute/environment for all tasks or configure task compute according to your workspace policy.

## 7. Useful troubleshooting
- `Missing cloud file system scheme`: check that the path starts with `/Volumes/<catalog>/<schema>/<volume>/...` and that the Volume exists.
- `PERMISSION_DENIED`: confirm your user/service principal has `USE CATALOG`, `USE SCHEMA`, `READ VOLUME`, and `WRITE VOLUME` where applicable.
- Table not found: run Bronze first and verify the catalog/schema names.
