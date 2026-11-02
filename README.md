# Synthetic Retail Sales Dataset — Azure Databricks Lakehouse Practice

**Purpose:** RAW SOURCE DATA for an Azure Databricks data engineering assessment. Four small CSV files with **intentionally embedded data-quality problems** for practising raw ingestion (PySpark), Bronze/Silver/Gold architecture, cleaning, casting, schema enforcement, NULL handling, deduplication, referential integrity, joins, star-schema modelling and retail analytics.

> **Do not clean these files.** All errors are intentional and must be preserved in the raw layer. Target storage catalog: `retail_data` (Unity Catalog).

## Contents

| File | Data Rows | Unique Records | Description |
| --- | ---: | ---: | --- |
| `sales.csv` | 80 | 78 transactions | Fact table: point-of-sale transactions, Jan 2025 – Oct 2026, INR |
| `products.csv` | 14 | 14 products | Dimension: Indian retail products with standard prices |
| `customers.csv` | 20 | 18 customers | Dimension: registered customers across Indian cities |
| `stores.csv` | 5 | 5 stores | Dimension: retail stores across Indian regions |

All prices and amounts are in **INR (₹)**.

---

## 1. sales.csv (fact)

| Column | Definition | Expected Type |
| --- | --- | --- |
| `transaction_id` | Unique transaction identifier (T001–T078) | STRING (or INT suffix) — primary key |
| `transaction_date` | Date of purchase (mixed formats!) | DATE (raw: STRING) |
| `product_id` | FK → products.csv | STRING |
| `customer_id` | FK → customers.csv (nullable) | STRING |
| `store_id` | FK → stores.csv | STRING |
| `quantity` | Units sold (negatives = returns, some text values) | INTEGER (raw: STRING) |
| `unit_price` | Price per unit in INR (some invalid text, 0, negative) | DECIMAL(10,2) (raw: STRING) |
| `discount` | Discount percent (0–20 typical; outliers and decimal variants exist) | DECIMAL(5,2) (raw: STRING) |
| `payment_method` | UPI / Credit Card / Debit Card / Cash / Net Banking / EMI / Wallet | STRING |
| `transaction_status` | Completed / Cancelled / Returned (+ invalid values) | STRING |

**Star schema role:** fact table. Grain = one row per transaction line.

## 2. products.csv (dimension)

| Column | Definition | Expected Type |
| --- | --- | --- |
| `product_id` | Unique product identifier (P001–P014) | STRING — primary key |
| `product_name` | Product display name (one missing) | STRING |
| `category` | Product category (inconsistent case/spacing) | STRING |
| `brand` | Brand name | STRING |
| `standard_price` | Standard selling price in INR | DECIMAL(10,2) |
| `launch_date` | Product launch date (mixed formats) | DATE (raw: STRING) |

## 3. customers.csv (dimension)

| Column | Definition | Expected Type |
| --- | --- | --- |
| `customer_id` | Unique customer identifier (C001–C018) | STRING — primary key |
| `customer_name` | Full name (leading/trailing spaces in some rows) | STRING |
| `email` | Email address (missing in 2 rows) | STRING |
| `city` | City (inconsistent capitalisation) | STRING |
| `customer_segment` | Regular / Premium / New (case variants in duplicates) | STRING |
| `registration_date` | Registration date (mixed formats) | DATE (raw: STRING) |

## 4. stores.csv (dimension)

| Column | Definition | Expected Type |
| --- | --- | --- |
| `store_id` | Unique store identifier (S001–S005) | STRING — primary key |
| `store_name` | Store display name | STRING |
| `city` | City (inconsistent capitalisation) | STRING |
| `region` | West / South / North / East | STRING |
| `store_type` | Hypermarket / Supermarket / Departmental / Kirana | STRING |

---

## Intentional Data-Quality Issues

### sales.csv

| # | Issue | Affected Records | Approx. Count |
| --- | --- | --- | ---: |
| 1 | Missing `customer_id` | T009, T044 | 2 |
| 2 | Missing `transaction_date` | T012, T052 | 2 |
| 3 | Duplicate full transaction rows (same `transaction_id`, identical content) | T004 (×2), T031 (×2) | 2 duplicates (4 rows) |
| 4 | `quantity` stored as text | T038 (`five`), T066 (`two`), T048 (`3` — numeric string in text column) | 3 |
| 5 | `unit_price` invalid text | T021 (`invalid`), T049 (`N/A`), T060 (`n/a`) | 3 |
| 6 | Zero `unit_price` | T027 | 1 |
| 7 | Negative `unit_price` | T058 (−149) | 1 |
| 8 | Zero `quantity` | T033, T071 | 2 |
| 9 | Negative `quantity` — legitimate returns (`status = Returned`) | T010 (−1), T034 (−2), T057 (−1) | 3 |
| 10 | Unusually large `discount` (>50%) | T016 (150), T041 (90), T069 (100) | 3 |
| 11 | `discount` stored as decimal fraction instead of percent | T037 (0.05), T065 (0.1) | 2 |
| 12 | Invalid `transaction_status` | T024 (`UNKNOWN`), T055 (`Processing`) | 2 |
| 13 | Nonexistent `product_id` (P099) | T019, T047 | 2 |
| 14 | Nonexistent `customer_id` | T029 (C099), T063 (C100) | 2 |
| 15 | Nonexistent `store_id` (S099) | T036 | 1 |
| 16 | Mixed `transaction_date` formats | T017 (`05/03/2025`), T065 (`2025/08/25`), T077 (`Oct 10 2026`) | 3 |
| 17 | Legitimate cancelled transactions | T007, T049 | 2 |

### products.csv

| # | Issue | Affected Records | Approx. Count |
| --- | --- | --- | ---: |
| 18 | Missing `product_name` | P003 | 1 |
| 19 | `category` case/spacing inconsistency | P002 (`electronics`), P003 (` ELECTRONICS `), P005 (`home & kitchen`), P007 (`groceries`), P009 (`personal care`), P011 (`stationery`), P012 (`Personal Care ` trailing space), P014 (` home appliances ` leading/trailing spaces) | 8 |
| 20 | `launch_date` mixed format | P010 (`05 May 2025`) | 1 |

### customers.csv

| # | Issue | Affected Records | Approx. Count |
| --- | --- | --- | ---: |
| 21 | Missing `email` | C004, C015 | 2 |
| 22 | Duplicate customer records (same `customer_id`, variant casing/spacing) | C005 (×2), C012 (×2) | 2 duplicates (20 rows vs 18 unique) |
| 23 | `customer_name` leading/trailing spaces | C003 (`  Amit Verma`), C005-dup (`Vikram Singh `), C011 (`  Suresh Babu`) | 3 |
| 24 | `city` inconsistent capitalisation | C006 (`chennai`), C009 (`PUNE`), C011 (`LUCKNOW`), C005-dup (`jaipur`), C012-dup (`nagpur`) | 5 |
| 25 | `customer_segment` case variants in duplicates | C005-dup (`regular`), C012-dup (`NEW`) | 2 |
| 26 | `registration_date` mixed format | C009 (`01/03/2025`) | 1 |

### stores.csv

| # | Issue | Affected Records | Approx. Count |
| --- | --- | --- | ---: |
| 27 | `city` inconsistent capitalisation | S003 (`NOIDA`), S005 (`ahmedabad`) | 2 |

**Valid records:** ~85% of rows are clean and processable — enough for both successful processing and exception handling.

---

## Suggested Data-Quality Checks (for the Silver layer)

1. **Schema enforcement / casting:** read all columns as STRING in Bronze; cast in Silver. Expect cast failures on `quantity` (T038, T066), `unit_price` (T021, T049, T060), and `discount` decimals (T037, T065).
2. **Null / completeness checks:** `customer_id IS NULL`, `transaction_date IS NULL`, `product_name IS NULL`, `email IS NULL`.
3. **Duplicates:** `COUNT(*) - COUNT(DISTINCT transaction_id)` on sales; full-row `dropDuplicates()`; duplicate `customer_id` detection in customers.
4. **Referential integrity (anti-joins):** sales LEFT JOIN products/customers/stores on IDs; route orphans (P099, C099, C100, S099) to an exception/quarantine table.
5. **Business-rule validation:**
   - `unit_price > 0` (fails T027, T058)
   - `quantity != 0` (fails T033, T071)
   - `quantity < 0` allowed only when `transaction_status = 'Returned'`
   - `discount BETWEEN 0 AND 50` (percent normalisation needed for T037, T065)
   - `transaction_status IN ('Completed', 'Cancelled', 'Returned')`
   - `transaction_date BETWEEN 2025-01-01 AND 2026-10-31`
6. **Date standardisation:** parse mixed formats (`yyyy-MM-dd`, `dd/MM/yyyy`, `yyyy/MM/dd`, `MMM dd yyyy`, `dd MMM yyyy`) into DATE.
7. **Standardisation:** TRIM names/cities/categories; `INITCAP`/lower-case canonical categories and segments.
8. **Cross-source consistency:** sales `unit_price` vs products `standard_price` (price-drift detection).
9. **Duplicate-dimension resolution:** deduplicate customers on `customer_id` before Gold star-schema build (surrogate keys optional).

## Suggested Star Schema (Gold layer)

* `fact_sales` — cleaned, deduplicated, validated transactions (Completed/Cancelled/Returned)
* `dim_product` — cleaned products (category `product_category`)
* `dim_customer` — cleaned, deduplicated customers
* `dim_store` — cleaned stores
* Optional `dim_date` — calendar dimension over 2025-01-01 → 2026-10-31

## Notes for Ingestion (Bronze → Unity Catalog)

* Load each CSV with `spark.read.csv(..., header=True)` into `retail_data.bronze.*` tables; keep everything STRING to avoid silent inference failures.
* These files live in the workspace folder `ReatilProject_practise` and are downloadable as-is.
* All 78 unique transaction IDs (T001–T078) are represented; the 80 data rows include the 2 duplicate rows.
