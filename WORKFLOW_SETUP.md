# Databricks Workflow setup

Create a Lakeflow Job with three Notebook tasks:
1. `Bronze_Ingestion` → notebook `01_bronze_ingestion.py`, no dependencies.
2. `Silver_Cleaning` → notebook `02_silver_cleaning.py`, depends on `Bronze_Ingestion` succeeding.
3. `Gold_Sales` → notebook `03_gold_sales.py`, depends on `Silver_Cleaning` succeeding.

Use a job cluster or serverless jobs compute supported by your workspace. Grant the job identity permission to read the Volume and create/write tables in the target schemas. Run the job and verify the output tables listed in README.md.
