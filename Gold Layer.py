# Databricks notebook source
# DBTITLE 1,Gold Layer — overview
# MAGIC %md
# MAGIC # Gold Layer — star-schema data warehouse
# MAGIC
# MAGIC Builds the Gold warehouse in `retail_data.gold` from the Silver tables: `fact_sales` (57 rows, `line_amount` = quantity × unit_price × (1 − discount/100), negative for returns), `dim_customer` (18), `dim_product` (14, category renamed to `product_category`), `dim_store` (5) and `dim_date` (669 days, 2025-01-01 → 2026-10-31).
# MAGIC
# MAGIC Split from "Bronze Layer to silver" — Bronze ingestion and Silver cleaning live in the "Silver Layer" notebook.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Gold warehouse intro
# MAGIC %md
# MAGIC ## Step 10 — Gold layer: the data warehouse (star schema)
# MAGIC
# MAGIC Gold is a classic star-schema warehouse: one fact table (`fact_sales`) surrounded by dimensions (`dim_customer`, `dim_product`, `dim_store`, `dim_date`). The calendar dimension covers the full sales period 2025-01-01 → 2026-10-31. The main revenue measure is `line_amount` = quantity × unit_price × (1 − discount%) — negative for returns.

# COMMAND ----------

# DBTITLE 1,Build the Gold star schema
# Each layer consumes only the layer before it - read back from Silver
silver_customers = spark.read.table('retail_data.silver.customers')
silver_products = spark.read.table('retail_data.silver.products')
silver_stores = spark.read.table('retail_data.silver.stores')
silver_sales = spark.read.table('retail_data.silver.sales')

# Dimensions
(
    silver_products.withColumnRenamed('category', 'product_category')
    .write.mode('overwrite').option('overwriteSchema', 'true')
    .saveAsTable('retail_data.gold.dim_product')
)
(
    silver_stores
    .write.mode('overwrite').option('overwriteSchema', 'true')
    .saveAsTable('retail_data.gold.dim_store')
)
(
    silver_customers
    .write.mode('overwrite').option('overwriteSchema', 'true')
    .saveAsTable('retail_data.gold.dim_customer')
)

# Calendar dimension over the full sales period
_dim_date = spark.sql("""
    SELECT explode(sequence(to_date('2025-01-01'), to_date('2026-10-31'), interval 1 day)) AS full_date
""").select(
    F.col('full_date'),
    F.year('full_date').alias('year'),
    F.quarter('full_date').alias('quarter'),
    F.month('full_date').alias('month'),
    F.date_format('full_date', 'MMMM').alias('month_name'),
    F.dayofmonth('full_date').alias('day'),
    F.date_format('full_date', 'EEEE').alias('day_name'),
    F.dayofweek('full_date').isin(1, 7).alias('is_weekend')
)
(
    _dim_date.write.mode('overwrite').option('overwriteSchema', 'true')
    .saveAsTable('retail_data.gold.dim_date')
)
print(f'gold.dim_date: {_dim_date.count()} days')

# Fact table with the revenue measure (negative amounts = returned items)
gold_fact = silver_sales.withColumn(
    'line_amount',
    F.round(F.col('quantity') * F.col('unit_price') * (1 - F.col('discount') / 100), 2)
)
(
    gold_fact.write.mode('overwrite').option('overwriteSchema', 'true')
    .saveAsTable('retail_data.gold.fact_sales')
)
gold_fact.display()

# COMMAND ----------

# DBTITLE 1,Warehouse verification intro
# MAGIC %md
# MAGIC ### Verify the warehouse
# MAGIC
# MAGIC Row counts layer by layer (Bronze raw → Silver clean → Gold star schema) and a smoke test that joins the fact table to a dimension — this same star-schema join is what the dashboard will query.

# COMMAND ----------

# DBTITLE 1,Verify the Gold warehouse
# Layer-by-layer row counts
layers = {
    'retail_data.bronze': ['customers', 'products', 'stores', 'sales'],
    'retail_data.silver': ['customers', 'products', 'stores', 'sales', 'sales_exceptions'],
    'retail_data.gold': ['fact_sales', 'dim_customer', 'dim_product', 'dim_store', 'dim_date']
}
for layer, tables in layers.items():
    for t in tables:
        print(f'{layer}.{t}: {spark.table(f"{layer}.{t}").count()} rows')

# Smoke test: revenue by category through the star schema (this is what the dashboard queries)
spark.sql("""
    SELECT p.product_category,
           COUNT(*) AS transactions,
           ROUND(SUM(f.line_amount), 2) AS revenue
    FROM retail_data.gold.fact_sales f
    JOIN retail_data.gold.dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_category
    ORDER BY revenue DESC
""").display()