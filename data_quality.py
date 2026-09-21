from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

CATALOG = "nugabanktransactions"
GOLD_SCHEMA = "gold"

FACT_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.fact_table"

fact_df = spark.table(FACT_TABLE)

# --------------------------------
# Data quality checks
# --------------------------------

row_count = fact_df.count()

missing_customer_ids = (
    fact_df.filter(col("customer_id").isNull()).count()
)

missing_employee_ids = (
    fact_df.filter(col("employee_id").isNull()).count()
)

missing_transaction_ids = (
    fact_df.filter(col("transaction_id").isNull()).count()
)

print(f"Fact table rows: {row_count}")
print(f"Missing customer IDs: {missing_customer_ids}")
print(f"Missing employee IDs: {missing_employee_ids}")
print(f"Missing transaction IDs: {missing_transaction_ids}")


# --------------------------------
# Fail the job if quality checks fail
# --------------------------------

if row_count == 0:
    raise ValueError("Data quality failed: fact table is empty.")

if missing_customer_ids > 0:
    raise ValueError(
        f"Data quality failed: {missing_customer_ids} customer IDs are missing."
    )

if missing_employee_ids > 0:
    raise ValueError(
        f"Data quality failed: {missing_employee_ids} employee IDs are missing."
    )

if missing_transaction_ids > 0:
    raise ValueError(
        f"Data quality failed: {missing_transaction_ids} transaction IDs are missing."
    )

print("All data quality checks passed.")