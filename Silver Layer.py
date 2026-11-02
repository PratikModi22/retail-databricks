# Databricks notebook source
# DBTITLE 1,Silver Layer — overview
# MAGIC %md
# MAGIC # Silver Layer — Bronze → Silver pipeline
# MAGIC
# MAGIC Raw CSV ingestion into **Bronze** (`retail_data.bronze`) plus all cleaning into **Silver** (`retail_data.silver`): trim, safe casts, mixed-date parsing, case normalisation, dedup, discount fraction → percent, business rules with quarantined exceptions.
# MAGIC
# MAGIC Split from "Bronze Layer to silver" — the Gold star-schema build now lives in the "Gold Layer" notebook.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Read customers.csv (raw)
df =(
    spark.read.option('inferSchema','true')
    .option('header','true')
    .csv('/Workspace/Users/pratikgmodi22@gmail.com/ReatilProject_practise/customers.csv')
    
)

df.printSchema()
df.show()

# COMMAND ----------

# DBTITLE 1,Profile raw customers
df.summary().display()
df.describe().display()

# COMMAND ----------

# DBTITLE 1,Trim customers
df = (
    df.withColumns( {
        'customer_id':F.trim('customer_id'),
        'customer_name':F.trim('customer_name'),
        'email':F.trim('email'),
        'city':F.trim('city'),
        'customer_segment':F.trim('customer_segment'),
        'registration_date':F.trim('registration_date')
     }
    )     
)
df.createOrReplaceTempView('customer_df')

# COMMAND ----------

# DBTITLE 1,Remaining cleaning steps
# MAGIC %md
# MAGIC ## Step 3 — Clean the remaining data issues
# MAGIC
# MAGIC The trimmed customer data still has these problems (intentional, from the README):
# MAGIC
# MAGIC 1. **Mixed date formats** — `registration_date` is mostly `yyyy-MM-dd`, but C009 uses `01/03/2025` (`dd/MM/yyyy`). This is why the earlier `date_format` failed with a cast error.
# MAGIC 2. **Wrong case** — cities like `chennai` / `PUNE` / `LUCKNOW`, segments like `regular` / `NEW`, and names with odd casing.
# MAGIC 3. **Duplicate customers** — C005 and C012 each appear twice with slightly different casing/spacing.
# MAGIC 4. **Missing emails** — C004 and C015 have no email (kept as NULL; email is optional).
# MAGIC
# MAGIC Note: the earlier SQL `UPDATE` failed because DML like `UPDATE` is not supported on temp views in Unity Catalog. Cleaning is done with DataFrame transformations instead.

# COMMAND ----------

# DBTITLE 1,Standardise mixed date formats
# Standardise registration_date: try 'yyyy-MM-dd' first, then 'dd/MM/yyyy' (C009).
# try_to_date() returns NULL when the text does not match the format (to_date would throw an error),
# so coalesce() picks the first pattern that works.
df = df.withColumn(
    'registration_date',
    F.expr("coalesce(try_to_date(registration_date, 'yyyy-MM-dd'), try_to_date(registration_date, 'dd/MM/yyyy'))")
)
df.createOrReplaceTempView('customer_df')

# Rows where the date could not be parsed with either format (should be empty)
df.where(F.col('registration_date').isNull()).display()

# COMMAND ----------

# DBTITLE 1,Standardise case and deduplicate

# Standardise text case, then remove the duplicate customers (C005, C012).
# After trimming + initcap/upper, the duplicate rows become identical, so one copy per customer_id is kept.
df1 = (
    df.withColumns({
        'customer_id': F.upper('customer_id'),
        'customer_name': F.initcap('customer_name'),
        'email': F.lower('email'),
        'city': F.initcap('city'),
        'customer_segment': F.upper('customer_segment')
    })
    .dropDuplicates(['customer_id'])
)
# registration_date is already a proper DATE now, so no date_format is needed.
df1.orderBy('customer_id').display()

# COMMAND ----------

# DBTITLE 1,Verification intro
# MAGIC %md
# MAGIC ## Step 4 — Verify the cleaned Silver data
# MAGIC
# MAGIC Checks: 18 unique customers (no duplicates), all registration dates parsed into a valid range, only the three valid segments (REGULAR / PREMIUM / NEW), and only the two known NULL emails (C004, C015).

# COMMAND ----------

# DBTITLE 1,Final quality checks
# Row count and uniqueness check (expect 18 rows, 18 unique customer_ids)
print(f"Rows: {df1.count()}, Unique customers: {df1.select('customer_id').distinct().count()}")

# Registration date range and any unparseable dates left (expect none NULL)
df1.agg(
    F.min('registration_date').alias('earliest_registration'),
    F.max('registration_date').alias('latest_registration'),
    F.sum(F.col('registration_date').isNull().cast('int')).alias('null_dates')
).display()

# Segment values (expect only REGULAR, PREMIUM, NEW) and rows still missing an email
df1.groupBy('customer_segment').count().orderBy('customer_segment').display()
df1.where(F.col('email').isNull()).orderBy('customer_id').display()

# COMMAND ----------

# DBTITLE 1,Products cleaning intro
# MAGIC %md
# MAGIC ## Step 5 — Clean products.csv
# MAGIC
# MAGIC From the README, products.csv has these intentional problems:
# MAGIC
# MAGIC 1. **Category case/spacing** — `electronics`, ` ELECTRONICS `, ` home appliances ` etc. → trim + Initcap gives one clean value per category.
# MAGIC 2. **Mixed `launch_date` format** — P010 uses `05 May 2025` (`dd MMM yyyy`) while the rest are `yyyy-MM-dd`.
# MAGIC 3. **Missing `product_name`** — P003 (kept as NULL, like the customer emails).
# MAGIC
# MAGIC Also cast `standard_price` from text to a proper DECIMAL.

# COMMAND ----------

# DBTITLE 1,Clean products.csv
# Read products as plain text (Bronze keeps everything as STRING), then clean in one pass
products_df = (
    spark.read.option('header', 'true')
    .csv('/Workspace/Users/pratikgmodi22@gmail.com/ReatilProject_practise/products.csv')
)

products_df = (
    products_df.withColumns({c: F.trim(c) for c in products_df.columns})
    .withColumns({
        'product_id': F.upper('product_id'),
        'product_name': F.initcap('product_name'),
        'category': F.initcap('category'),
        'brand': F.initcap('brand'),
        'standard_price': F.expr("try_cast(standard_price as decimal(10,2))"),
        'launch_date': F.expr("coalesce(try_to_date(launch_date, 'yyyy-MM-dd'), try_to_date(launch_date, 'dd MMM yyyy'))")
    })
)
products_df.orderBy('product_id').display()

# Quality check: expect 14 unique products, and only P003 with a missing name
print(f"Products: {products_df.count()}, unique product_ids: {products_df.select('product_id').distinct().count()}")
products_df.where(F.col('product_name').isNull() | F.col('launch_date').isNull() | F.col('standard_price').isNull()).display()

# COMMAND ----------

# DBTITLE 1,Stores cleaning intro
# MAGIC %md
# MAGIC ## Step 6 — Clean stores.csv
# MAGIC
# MAGIC The cleanest file: the only issue is city capitalisation — S003 (`NOIDA`) and S005 (`ahmedabad`). Trim everything and Initcap the city.

# COMMAND ----------

# DBTITLE 1,Clean stores.csv
# Read stores and fix city casing
stores_df = (
    spark.read.option('header', 'true')
    .csv('/Workspace/Users/pratikgmodi22@gmail.com/ReatilProject_practise/stores.csv')
)

stores_df = (
    stores_df.withColumns({c: F.trim(c) for c in stores_df.columns})
    .withColumns({
        'store_id': F.upper('store_id'),
        'store_name': F.initcap('store_name'),
        'city': F.initcap('city')
    })
)
stores_df.display()

# Quality check: expect 5 stores, 5 unique store_ids, 4 valid regions
print(f"Stores: {stores_df.count()}, unique store_ids: {stores_df.select('store_id').distinct().count()}")
stores_df.select('region').distinct().display()

# COMMAND ----------

# DBTITLE 1,Sales cleaning intro
# MAGIC %md
# MAGIC ## Step 7 — Clean sales.csv (the fact table)
# MAGIC
# MAGIC The messiest file. Intentional issues from the README:
# MAGIC
# MAGIC 1. **Duplicate full rows** — T004 and T031 appear twice (80 rows, 78 unique transactions).
# MAGIC 2. **Numbers stored as text** — quantity `five`/`two`, unit_price `invalid`/`N/A`/`n/a`.
# MAGIC 3. **Mixed date formats** — `dd/MM/yyyy`, `yyyy/MM/dd`, and `Oct 10 2026`; two missing dates.
# MAGIC 4. **Business-rule violations** — zero/negative prices, zero quantities, discounts above 50%, discounts stored as fractions (0.05), invalid statuses (`UNKNOWN`, `Processing`).
# MAGIC 5. **Referential orphans** — product P099, customers C099/C100, store S099.
# MAGIC 6. **Missing `customer_id`** — T009, T044 (valid walk-in sales, kept with NULL).
# MAGIC
# MAGIC Negative quantities with status `Returned` (T010, T034, T057) are **legitimate** and stay. Cleaning is done in stages, and bad rows are quarantined with a reason instead of silently dropped.

# COMMAND ----------

# DBTITLE 1,Sales stage 1 - read and dedup
# Stage 1: read as text, trim everything, remove the duplicate full rows (T004, T031)
sales_raw = (
    spark.read.option('header', 'true')
    .csv('/Workspace/Users/pratikgmodi22@gmail.com/ReatilProject_practise/sales.csv')
)

sales_df = sales_raw.withColumns({c: F.trim(c) for c in sales_raw.columns})

before = sales_df.count()
sales_df = sales_df.dropDuplicates()
print(f"Raw rows: {before}, after dropping duplicate rows: {sales_df.count()}")  # expect 80 -> 78

# COMMAND ----------

# DBTITLE 1,Sales stage 2 intro
# MAGIC %md
# MAGIC ### Stage 2 — Cast numbers and dates
# MAGIC
# MAGIC `try_cast` returns NULL instead of erroring on text like `five` or `N/A`. Four date patterns are tried in turn with `try_to_date` + `coalesce`. Discounts below 1 are fractions, so 0.05 becomes 5 (%). An empty `customer_id` is a valid walk-in sale and is turned into a real NULL.

# COMMAND ----------

# DBTITLE 1,Sales stage 2 - casts and dates
# Stage 2: safe casts, date standardisation, discount percent normalisation
sales_df = sales_df.withColumns({
    'transaction_id': F.upper('transaction_id'),
    'product_id': F.upper('product_id'),
    'store_id': F.upper('store_id'),
    'customer_id': F.expr("nullif(upper(customer_id), '')"),  # '' / lower-case ids -> clean NULL or uppercase
    'quantity': F.expr("try_cast(quantity as int)"),
    'unit_price': F.expr("try_cast(unit_price as decimal(10,2))"),
    'discount': F.expr("try_cast(discount as decimal(5,2))"),
    'transaction_date': F.expr("""
        coalesce(
            try_to_date(transaction_date, 'yyyy-MM-dd'),
            try_to_date(transaction_date, 'dd/MM/yyyy'),
            try_to_date(transaction_date, 'yyyy/MM/dd'),
            try_to_date(transaction_date, 'MMM d yyyy')
        )
    """),
    'payment_method': F.initcap('payment_method'),
    'transaction_status': F.initcap('transaction_status')
})

# Discounts below 1 were stored as fractions (T037: 0.05, T065: 0.1) -> convert to percent
sales_df = sales_df.withColumn(
    'discount',
    F.when(F.col('discount') < 1, F.col('discount') * 100).otherwise(F.col('discount'))
)

# Rows where a number or date could not be parsed (expect: T012, T021, T038, T049, T052, T060, T066)
sales_df.where(
    F.col('quantity').isNull() | F.col('unit_price').isNull() |
    F.col('discount').isNull() | F.col('transaction_date').isNull()
).orderBy('transaction_id').display()

# COMMAND ----------

# DBTITLE 1,Sales stage 3 intro
# MAGIC %md
# MAGIC ### Stage 3 — Business rules and quarantine
# MAGIC
# MAGIC Rules from the README: status must be Completed/Cancelled/Returned; `unit_price > 0`; `quantity != 0`; negative quantity only for returns; discount 0–50; and every non-null FK must exist in the cleaned dimensions. Each failing row gets a `rejection_reason` and goes to the exceptions table — the rest becomes the clean fact table.

# COMMAND ----------

# DBTITLE 1,Sales stage 3 - rules and quarantine
# Stage 3: apply business rules + referential integrity, keep bad rows with a reason
product_ids = [r.product_id for r in products_df.select('product_id').collect()]
customer_ids = [r.customer_id for r in df1.select('customer_id').collect()]
store_ids = [r.store_id for r in stores_df.select('store_id').collect()]

sales_flagged = sales_df.withColumn(
    'rejection_reason',
    F.when(
        F.col('quantity').isNull() | F.col('unit_price').isNull() |
        F.col('discount').isNull() | F.col('transaction_date').isNull(),
        'Unparsable number or missing date'
    )
    .when(~F.col('transaction_status').isin('Completed', 'Cancelled', 'Returned'), 'Invalid transaction_status')
    .when(F.col('unit_price') <= 0, 'unit_price must be > 0')
    .when(F.col('quantity') == 0, 'quantity cannot be 0')
    .when((F.col('quantity') < 0) & (F.col('transaction_status') != 'Returned'),
          'Negative quantity only allowed for Returned')
    .when(~F.col('discount').between(0, 50), 'discount must be 0-50')
    .when(~F.col('product_id').isin(product_ids), 'Unknown product_id')
    .when(F.col('customer_id').isNotNull() & ~F.col('customer_id').isin(customer_ids), 'Unknown customer_id')
    .when(~F.col('store_id').isin(store_ids), 'Unknown store_id')
    .otherwise(None)
)

# Split: exceptions (quarantine) vs the clean fact table
sales_exceptions = sales_flagged.where(F.col('rejection_reason').isNotNull()).orderBy('transaction_id')
fact_sales = sales_flagged.where(F.col('rejection_reason').isNull()).drop('rejection_reason')

print(f"Clean fact rows: {fact_sales.count()}, exception rows: {sales_exceptions.count()}")
sales_exceptions.display()

# COMMAND ----------

# DBTITLE 1,Sales verification intro
# MAGIC %md
# MAGIC ### Verify the cleaned fact table
# MAGIC
# MAGIC Expect: 78 unique transactions after de-duplication, ~21 exceptions (matching the README issue list), dates inside 2025-01-01 → 2026-10-31, discount never above 50, prices positive, and no orphaned keys — cross-checked with anti-joins (a different method from the `isin` rule above).

# COMMAND ----------

# DBTITLE 1,Sales final verification
# Row counts and key uniqueness
print(f"Fact rows: {fact_sales.count()}, unique transaction_ids: {fact_sales.select('transaction_id').distinct().count()}")
print(f"Exception rows: {sales_exceptions.count()} (out of {sales_df.count()} deduped)")

# Value ranges (dates within 2025-01-01 -> 2026-10-31, price > 0, discount <= 50)
fact_sales.agg(
    F.min('transaction_date').alias('earliest'),
    F.max('transaction_date').alias('latest'),
    F.min('unit_price').alias('min_unit_price'),
    F.max('discount').alias('max_discount')
).display()

# Status distribution (expect only Cancelled / Completed / Returned)
fact_sales.groupBy('transaction_status').count().orderBy('transaction_status').display()

# Walk-in sales without a customer (T009, T044) are valid - confirm they survived
fact_sales.where(F.col('customer_id').isNull()).display()

# Referential integrity cross-check with anti-joins (both should be empty)
fact_sales.join(products_df.select('product_id'), 'product_id', 'left_anti').display()
fact_sales.join(stores_df.select('store_id'), 'store_id', 'left_anti').display()

# COMMAND ----------

# DBTITLE 1,Bronze layer intro
# MAGIC %md
# MAGIC ## Step 8 — Bronze layer: raw data into Unity Catalog
# MAGIC
# MAGIC Bronze keeps the CSV data **exactly as it arrived**: every column stays STRING, duplicates and errors included. We only add `source_file` and `ingested_at` columns for lineage. This is the raw copy we never modify.

# COMMAND ----------

# DBTITLE 1,Write Bronze tables
# Create the three-layer schemas in the retail_data catalog
for schema_name in ['bronze', 'silver', 'gold']:
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS retail_data.{schema_name}')

csv_path = '/Workspace/Users/pratikgmodi22@gmail.com/ReatilProject_practise'

# Load each raw CSV as-is (all STRING) with ingestion metadata
for name in ['customers', 'products', 'stores', 'sales']:
    raw = spark.read.option('header', 'true').csv(f'{csv_path}/{name}.csv')
    raw = (
        raw.withColumns({c: F.col(c).cast('string') for c in raw.columns})
        .withColumns({
            'source_file': F.lit(f'{name}.csv'),
            'ingested_at': F.current_timestamp()
        })
    )
    (
        raw.write.mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(f'retail_data.bronze.{name}')
    )
    print(f'retail_data.bronze.{name}: {raw.count()} raw rows')

# COMMAND ----------

# DBTITLE 1,Silver layer intro
# MAGIC %md
# MAGIC ## Step 9 — Silver layer: cleaned tables
# MAGIC
# MAGIC Silver holds the cleaned, typed, de-duplicated DataFrames from the cleaning steps above: customers (18), products (14), stores (5), sales (57 valid transactions) and sales_exceptions (21 quarantined rows with a rejection reason).

# COMMAND ----------

# DBTITLE 1,Write Silver tables
# Silver = the cleaned DataFrames built earlier in this notebook
silver_tables = {
    'customers': df1,
    'products': products_df,
    'stores': stores_df,
    'sales': fact_sales,
    'sales_exceptions': sales_exceptions
}

for name, data in silver_tables.items():
    (
        data.write.mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(f'retail_data.silver.{name}')
    )
    print(f'retail_data.silver.{name}: {data.count()} rows')