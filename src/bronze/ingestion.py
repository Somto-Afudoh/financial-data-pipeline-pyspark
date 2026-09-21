from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

CATALOG = "nugabanktransactions"
BRONZE_SCHEMA = "bronze"
VOLUME = "raw"

RAW_FILE = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME}/nuga_bank_transactions.csv"

# Read raw CSV
nuga_bank_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(RAW_FILE)
)

# Write raw data to Bronze Delta table
(
    nuga_bank_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.nuga_bank_transactions")
)