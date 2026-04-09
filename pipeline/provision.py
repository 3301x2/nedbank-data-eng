"""Gold layer: dimensional model from Silver tables."""

from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from pipeline.utils import get_config, get_spark


def _sk(col_name):
    """Surrogate key: first 15 hex chars of SHA-256, converted to BIGINT."""
    return (
        F.conv(F.sha2(F.col(col_name), 256).substr(1, 15), 16, 10)
        .cast("bigint")
    )


def build_dim_customers(spark, silver_path, gold_path):
    """Build dim_customers: surrogate key, age_band derived from dob."""
    df = spark.read.format("delta").load(f"{silver_path}/customers")

    # Derive age_band from dob
    today = date.today()
    df = df.withColumn(
        "age",
        F.floor(F.datediff(F.lit(today), F.col("dob")) / 365.25)
    )
    df = df.withColumn(
        "age_band",
        F.when(F.col("age") >= 65, "65+")
        .when(F.col("age") >= 56, "56-65")
        .when(F.col("age") >= 46, "46-55")
        .when(F.col("age") >= 36, "36-45")
        .when(F.col("age") >= 26, "26-35")
        .when(F.col("age") >= 18, "18-25")
        .otherwise(None)
    )

    df = df.withColumn("customer_sk", _sk("customer_id"))

    # Gold schema
    dim_customers = df.select(
        "customer_sk",
        "customer_id",
        "gender",
        "province",
        "income_band",
        "segment",
        "risk_score",
        "kyc_status",
        "age_band",
    )

    dim_customers.write.format("delta").mode("overwrite").save(
        f"{gold_path}/dim_customers"
    )
    print("[Gold] dim_customers written")
    return dim_customers


def build_dim_accounts(spark, silver_path, gold_path):
    """Build dim_accounts: surrogate key, rename customer_ref to customer_id."""
    df = spark.read.format("delta").load(f"{silver_path}/accounts")

    # Rename customer_ref -> customer_id
    df = df.withColumnRenamed("customer_ref", "customer_id")

    df = df.withColumn("account_sk", _sk("account_id"))

    # Gold schema
    dim_accounts = df.select(
        "account_sk",
        "account_id",
        "customer_id",
        "account_type",
        "account_status",
        "open_date",
        "product_tier",
        "digital_channel",
        "credit_limit",
        "current_balance",
        "last_activity_date",
    )

    dim_accounts.write.format("delta").mode("overwrite").save(
        f"{gold_path}/dim_accounts"
    )
    print("[Gold] dim_accounts written")
    return dim_accounts


def build_fact_transactions(spark, silver_path, gold_path,
                            dim_accounts, dim_customers):
    """Build fact_transactions: resolve surrogate FKs via joins."""
    txn = spark.read.format("delta").load(f"{silver_path}/transactions")

    # Resolve surrogate FKs
    acct_lookup = dim_accounts.select("account_sk", "account_id", "customer_id")
    txn = txn.join(acct_lookup, on="account_id", how="left")

    cust_lookup = dim_customers.select("customer_sk", "customer_id")
    txn = txn.join(cust_lookup, on="customer_id", how="left")

    txn = txn.withColumn("transaction_sk", _sk("transaction_id"))

    txn = txn.withColumn(
        "ingestion_timestamp", F.col("ingestion_timestamp").cast("timestamp")
    )

    # Gold schema
    fact_transactions = txn.select(
        "transaction_sk",
        "transaction_id",
        "account_sk",
        "customer_sk",
        F.col("transaction_date").cast("date"),
        "transaction_timestamp",
        "transaction_type",
        "merchant_category",
        "merchant_subcategory",
        F.col("amount").cast(DecimalType(18, 2)),
        "currency",
        "channel",
        "province",
        "dq_flag",
        "ingestion_timestamp",
    )

    fact_transactions.write.format("delta").mode("overwrite").save(
        f"{gold_path}/fact_transactions"
    )
    print("[Gold] fact_transactions written")


def run_provisioning():
    config = get_config()
    spark = get_spark()

    silver_path = config["output"]["silver_path"]
    gold_path = config["output"]["gold_path"]

    dim_customers = build_dim_customers(spark, silver_path, gold_path)
    dim_accounts = build_dim_accounts(spark, silver_path, gold_path)
    build_fact_transactions(spark, silver_path, gold_path,
                           dim_accounts, dim_customers)

    print("[Gold] provisioning complete")
