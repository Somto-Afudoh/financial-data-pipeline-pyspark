from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder.getOrCreate()

# Configuration
CATALOG = "nugabanktransactions"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.nuga_bank_transactions_clean"

# Read cleaned Silver data
nuga_bank_df_clean = spark.table(SILVER_TABLE)

# transaction table
transaction = (
    nuga_bank_df_clean
    .select(
        'Transaction_Date', 
        'Amount', 
        'Transaction_Type'
    )
    .withColumn(
        "transaction_id",
        monotonically_increasing_id()
    )
    .select(
        "transaction_id",
        "Transaction_Date",
        "Amount",
        "Transaction_Type"
    )
)

# Save as Gold Delta transaction table


(
    transaction.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{CATALOG}.{GOLD_SCHEMA}.transaction"
    )
)


# customer table
customer = (
    nuga_bank_df_clean
    .select(
        'Customer_Name', 
        'Customer_Address', 
        'Customer_City', 
        'Customer_State',
        'Customer_Country',
        'Email',
        'Phone_Number'
    )
    .distinct()
)
# Create customer id

customer = (
    customer
    .withColumn(
        "customer_id",
        monotonically_increasing_id()
    )
    .select(
        "customer_id",
        "Customer_Name",
        "Customer_Address",
        "Customer_City",
        "Customer_State",
        "Customer_Country",
        "Email",
        "Phone_Number"
    )
)

# Save as Gold Delta customer table

(
    customer.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{CATALOG}.{GOLD_SCHEMA}.customer"
    )
)

# employee table
employee = (
    nuga_bank_df_clean
    .select(
        "Company",
        "Job_Title",
        "Gender",
        "Marital_Status"
    )
    .distinct()
)

employee = (
    employee
    .withColumn(
        "employee_id",
        monotonically_increasing_id()
    )
    .select(
        "employee_id",
        "Company",
        "Job_Title",
        "Gender",
        "Marital_Status"
    )
)

# Save as Gold Delta employee table
    
(
    employee.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{CATALOG}.{GOLD_SCHEMA}.employee"
    )

)

# fact table

fact_table = (
    nuga_bank_df_clean
    .join(
        customer,
        [
            "Customer_Name",
            "Customer_Address",
            "Customer_City",
            "Customer_State",
            "Customer_Country",
            "Email",
            "Phone_Number" 
        ],
        "left"
    )

    .join(
        transaction,
        [
            "Transaction_Date",
            "Amount",
            "Transaction_Type"
        ],
        "left"
    )

    .join(
        employee,
        [
             "Company",
            "Job_Title",
            "Gender",
            "Marital_Status"
        ],
        "left"
    )
    .select(
        "transaction_id",
        "customer_id",
        "employee_id",
        "Credit_Card_Number",
        "IBAN",
        "Currency_Code",
        "Random_Number",
        "Category",
        "Group",
        "Is_Active",
        "Last_Updated",
        "Description"
    )
)

# Save as Gold Delta fact table

(
    fact_table.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        f"{CATALOG}.{GOLD_SCHEMA}.fact_table"
    )
)

