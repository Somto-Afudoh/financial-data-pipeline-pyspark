from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# --------------------------------------------------
# Configuration
# --------------------------------------------------

CATALOG = "nugabanktransactions"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA= "silver"

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.nuga_bank_transactions"
SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.nuga_bank_transactions_clean"

# --------------------------------------------------
# 1. Read data from Bronze
# --------------------------------------------------

nuga_bank_df = spark.table(BRONZE_TABLE)

# Convert Credit_card_Number to string
nuga_bank_df = nuga_bank_df.withColumn(
    "Credit_Card_Number",
    col("Credit_Card_Number").cast("string")
)

# --------------------------------------------------
# 2. Fill missing values
# --------------------------------------------------

nuga_bank_df_clean = nuga_bank_df.fillna({
    "Customer_Name": "Unknown",
    "Customer_Address": "Unknown",
    "Customer_City": "Unknown",
    "Customer_State": "Unknown",
    "Customer_Country": "Unknown",
    "Company": "Unknown",
    "Job_Title": "Unknown",
    "Email": "Unknown",
    "Phone_Number": "Unknown",
    "Credit_Card_Number": "Unknown",
    "IBAN": "Unknown",
    "Currency_Code": "Unknown",
    "Random_Number": 0.0,
    "Category": "Unknown",
    "Group": "Unknown",
    "Is_Active": "Unknown",
    "Description": "Unknown",
    "Gender": "Unknown",
    "Marital_Status": "Unknown"
})

# --------------------------------------------------
# 3. Remove records without Last_Updated
# --------------------------------------------------
nuga_bank_df_clean = nuga_bank_df_clean.na.drop(
    subset=["Last_Updated"]
)

# --------------------------------------------------
# 4. Write cleaned data to Silver
# --------------------------------------------------

(
    nuga_bank_df_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"Silver table successfully created: {SILVER_TABLE}")