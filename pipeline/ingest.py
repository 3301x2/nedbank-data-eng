"""Bronze layer: raw source data into Delta tables, no transformations."""

from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline.utils import get_config, get_spark


def write_delta(df: DataFrame, path: str):
    """Write a DataFrame as a Delta table."""
    df.write.format("delta").mode("overwrite").save(path)


def run_ingestion():
    config = get_config()
    spark = get_spark()

    # Single timestamp for the entire ingestion run
    run_ts = datetime.now(timezone.utc).isoformat()

    bronze_path = config["output"]["bronze_path"]

    # -- Accounts (CSV) --
    accounts = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(config["input"]["accounts_path"])
        .withColumn("ingestion_timestamp", F.lit(run_ts).cast("timestamp"))
    )
    write_delta(accounts, f"{bronze_path}/accounts")
    print("[Bronze] accounts written")

    # -- Customers (CSV) --
    customers = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(config["input"]["customers_path"])
        .withColumn("ingestion_timestamp", F.lit(run_ts).cast("timestamp"))
    )
    write_delta(customers, f"{bronze_path}/customers")
    print("[Bronze] customers written")

    # -- Transactions (JSONL) --
    transactions = (
        spark.read
        .json(config["input"]["transactions_path"])
        .withColumn("ingestion_timestamp", F.lit(run_ts).cast("timestamp"))
    )
    write_delta(transactions, f"{bronze_path}/transactions")
    print("[Bronze] transactions written")

    print("[Bronze] ingestion complete")
