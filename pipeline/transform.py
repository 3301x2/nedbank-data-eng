"""Silver layer: clean, type, dedup, flag Bronze data."""

import glob as globmod
import json
import os
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, IntegerType
from pyspark.sql.window import Window
import yaml

from pipeline.utils import get_config, get_spark


def get_dq_rules():
    """Load DQ rules from config."""
    dq_path = os.environ.get("DQ_RULES", "/data/config/dq_rules.yaml")
    # Fall back to bundled copy if not on mounted volume
    if not os.path.exists(dq_path):
        dq_path = "/app/config/dq_rules.yaml"
    with open(dq_path) as f:
        return yaml.safe_load(f)


def parse_date_flexible(col_name):
    """Try multiple date formats, return null if none match.

    Handles: YYYY-MM-DD, DD/MM/YYYY, unix epoch (integer).
    """
    col = F.col(col_name)
    return (
        F.coalesce(
            # ISO format: 2025-03-22
            F.to_date(col, "yyyy-MM-dd"),
            # UK/SA format: 22/03/2025
            F.to_date(col, "dd/MM/yyyy"),
            # Unix epoch (seconds) - cast int to timestamp then to date
            F.to_date(F.from_unixtime(col.cast("bigint"))),
        )
    )


def transform_customers(spark, bronze_path, silver_path, dq_rules):
    """Clean customers: dedup, type cast dates and ints."""
    df = spark.read.format("delta").load(f"{bronze_path}/customers")

    # Dedup on primary key
    dedup_key = dq_rules["dedup_keys"]["customers"]
    df = df.dropDuplicates([dedup_key])

    # Type casting
    df = (
        df
        .withColumn("dob", parse_date_flexible("dob"))
        .withColumn("risk_score", F.col("risk_score").cast(IntegerType()))
    )

    # Silver schema (PII excluded)
    df = df.select(
        "customer_id", "gender", "province", "income_band",
        "segment", "risk_score", "kyc_status", "dob",
    )

    df.write.format("delta").mode("overwrite").save(f"{silver_path}/customers")
    print("[Silver] customers written")


def transform_accounts(spark, bronze_path, silver_path, dq_rules):
    """Clean accounts: dedup, type cast, drop null PKs."""
    df = spark.read.format("delta").load(f"{bronze_path}/accounts")

    # Drop rows with null primary key (matches null_checks rule for accounts)
    df = df.filter(F.col("account_id").isNotNull())

    # Dedup on primary key
    dedup_key = dq_rules["dedup_keys"]["accounts"]
    df = df.dropDuplicates([dedup_key])

    # Type casting
    df = (
        df
        .withColumn("open_date", parse_date_flexible("open_date"))
        .withColumn("last_activity_date", parse_date_flexible("last_activity_date"))
        .withColumn("credit_limit", F.col("credit_limit").cast(DecimalType(18, 2)))
        .withColumn("current_balance", F.col("current_balance").cast(DecimalType(18, 2)))
    )

    # Silver schema (PII excluded)
    df = df.select(
        "account_id", "customer_ref", "account_type", "account_status",
        "open_date", "product_tier", "digital_channel",
        "credit_limit", "current_balance", "last_activity_date",
    )

    df.write.format("delta").mode("overwrite").save(f"{silver_path}/accounts")
    print("[Silver] accounts written")


def transform_transactions(spark, bronze_path, silver_path, dq_rules):
    """Clean transactions: dedup, type cast, flatten nested, add dq_flag."""
    df = spark.read.format("delta").load(f"{bronze_path}/transactions")

    # Flatten nested structs
    df = (
        df
        .withColumn("province", F.col("location.province"))
        .withColumn("city", F.col("location.city"))
        .withColumn("coordinates", F.col("location.coordinates"))
        .withColumn("device_id", F.col("metadata.device_id"))
        .withColumn("session_id", F.col("metadata.session_id"))
        .withColumn("retry_flag", F.col("metadata.retry_flag"))
        .drop("location", "metadata")
    )

    # -- Handle merchant_subcategory (absent in Stage 1) --
    if "merchant_subcategory" not in df.columns:
        df = df.withColumn("merchant_subcategory", F.lit(None).cast("string"))

    # -- DQ flagging (null = clean; first flag set wins) --
    df = df.withColumn("dq_flag", F.lit(None).cast("string"))

    # Null checks: flag any row missing a required field
    null_fields = dq_rules.get("null_checks", {}).get("transactions", [])
    for field in null_fields:
        df = df.withColumn(
            "dq_flag",
            F.when(
                F.col(field).isNull() & F.col("dq_flag").isNull(),
                F.lit("NULL_REQUIRED"),
            ).otherwise(F.col("dq_flag"))
        )

    # Domain checks: flag values outside the allowed set
    domain_checks = dq_rules.get("domain_checks", {})
    for field, rules in domain_checks.items():
        allowed = rules["allowed"]
        flag_value = rules["dq_flag"]
        df = df.withColumn(
            "dq_flag",
            F.when(
                F.col(field).isNotNull()
                & ~F.col(field).isin(allowed)
                & F.col("dq_flag").isNull(),
                F.lit(flag_value),
            ).otherwise(F.col("dq_flag"))
        )

    # Flag currency variants before normalising
    currency_rules = dq_rules.get("currency_normalisation", {})
    target_currency = currency_rules.get("target_value", "ZAR")
    df = df.withColumn(
        "dq_flag",
        F.when(
            F.col("currency").isNotNull()
            & (F.col("currency") != target_currency)
            & F.col("dq_flag").isNull(),
            F.lit("CURRENCY_VARIANT"),
        ).otherwise(F.col("dq_flag"))
    )
    # Normalise all currency to ZAR
    df = df.withColumn("currency", F.lit(target_currency))

    # Flag type mismatch on amount (if it can't cast to decimal)
    df = df.withColumn("_amount_parsed", F.col("amount").cast(DecimalType(18, 2)))
    df = df.withColumn(
        "dq_flag",
        F.when(
            F.col("_amount_parsed").isNull()
            & F.col("amount").isNotNull()
            & F.col("dq_flag").isNull(),
            F.lit("TYPE_MISMATCH"),
        ).otherwise(F.col("dq_flag"))
    )
    df = df.withColumn("amount", F.col("_amount_parsed")).drop("_amount_parsed")

    # Flag date format issues
    df = df.withColumn("_date_parsed", parse_date_flexible("transaction_date"))
    df = df.withColumn(
        "dq_flag",
        F.when(
            F.col("_date_parsed").isNull()
            & F.col("transaction_date").isNotNull()
            & F.col("dq_flag").isNull(),
            F.lit("DATE_FORMAT"),
        ).otherwise(F.col("dq_flag"))
    )
    df = df.withColumn("transaction_date", F.col("_date_parsed").cast(DateType())).drop("_date_parsed")

    # -- Type casting --
    df = df.withColumn(
        "transaction_timestamp",
        F.to_timestamp(
            F.concat_ws(" ", F.col("transaction_date").cast("string"), F.col("transaction_time"))
        )
    )

    # -- Dedup on transaction_id (keep first by transaction_timestamp) --
    dedup_key = dq_rules["dedup_keys"]["transactions"]
    w = Window.partitionBy(dedup_key).orderBy("transaction_timestamp")
    df = df.withColumn("_row_num", F.row_number().over(w))
    # Flag duplicates before removing them
    df = df.withColumn(
        "dq_flag",
        F.when(
            (F.col("_row_num") > 1) & F.col("dq_flag").isNull(),
            F.lit("DUPLICATE_DEDUPED"),
        ).otherwise(F.col("dq_flag"))
    )
    # Keep only first occurrence
    df = df.filter(F.col("_row_num") == 1).drop("_row_num")

    # -- Referential integrity: flag orphaned accounts --
    accounts = spark.read.format("delta").load(f"{silver_path}/accounts")
    valid_accounts = accounts.select("account_id").distinct()
    df = df.join(
        valid_accounts.withColumn("_has_account", F.lit(True)),
        on="account_id",
        how="left",
    )
    df = df.withColumn(
        "dq_flag",
        F.when(
            F.col("_has_account").isNull() & F.col("dq_flag").isNull(),
            F.lit("ORPHANED_ACCOUNT"),
        ).otherwise(F.col("dq_flag"))
    ).drop("_has_account")

    # Keep ingestion_timestamp for Gold layer

    df.write.format("delta").mode("overwrite").save(f"{silver_path}/transactions")
    print("[Silver] transactions written")


def _count_table(spark, delta_path, tmp_path):
    """Row count of a Delta table via Spark JSON write (no .collect())."""
    (
        spark.read.format("delta").load(delta_path)
        .groupBy()
        .count()
        .write.mode("overwrite")
        .json(tmp_path)
    )
    total = 0
    for fpath in globmod.glob(f"{tmp_path}/part-*.json"):
        with open(fpath) as f:
            for line in f:
                total += json.loads(line).get("count", 0)
    return total


def write_dq_report(spark, config, run_start):
    """Write dq_report.json. Call after provisioning (needs all layer counts)."""
    bronze_path = config["output"]["bronze_path"]
    silver_path = config["output"]["silver_path"]
    gold_path = config["output"]["gold_path"]
    report_path = config["output"]["dq_report_path"]

    dq_rules = get_dq_rules()
    issue_handling = dq_rules.get("issue_handling", {})

    # Source counts (Bronze)
    bronze_txn_count = _count_table(spark, f"{bronze_path}/transactions", "/tmp/cnt_b_txn")
    bronze_acct_count = _count_table(spark, f"{bronze_path}/accounts", "/tmp/cnt_b_acct")
    bronze_cust_count = _count_table(spark, f"{bronze_path}/customers", "/tmp/cnt_b_cust")

    # DQ flag counts from Silver transactions
    (
        spark.read.format("delta").load(f"{silver_path}/transactions")
        .groupBy("dq_flag")
        .count()
        .write.mode("overwrite")
        .json("/tmp/cnt_flags")
    )
    silver_flag_data = {}
    silver_total = 0
    for fpath in globmod.glob("/tmp/cnt_flags/part-*.json"):
        with open(fpath) as jf:
            for line in jf:
                record = json.loads(line)
                silver_flag_data[record.get("dq_flag")] = record["count"]
                silver_total += record["count"]

    # Duplicate count = rows removed by dedup (in Bronze but absent from Silver)
    duplicate_count = max(0, bronze_txn_count - silver_total)

    # Null account_id count = accounts filtered out in transform_accounts
    silver_acct_count = _count_table(spark, f"{silver_path}/accounts", "/tmp/cnt_s_acct")
    null_acct_count = max(0, bronze_acct_count - silver_acct_count)

    # Gold layer record counts
    gold_txn_count = _count_table(spark, f"{gold_path}/fact_transactions", "/tmp/cnt_g_txn")
    gold_acct_count = _count_table(spark, f"{gold_path}/dim_accounts", "/tmp/cnt_g_acct")
    gold_cust_count = _count_table(spark, f"{gold_path}/dim_customers", "/tmp/cnt_g_cust")

    # Build dq_issues array (omit zero-count entries per spec)
    dq_issues = []

    if duplicate_count > 0:
        h = issue_handling.get("DUPLICATE_DEDUPED", {})
        pct = round(duplicate_count / bronze_txn_count * 100, 2) if bronze_txn_count else 0.0
        dq_issues.append({
            "issue_type": h.get("issue_type", "duplicate_transactions"),
            "records_affected": duplicate_count,
            "percentage_of_total": pct,
            "handling_action": h.get("handling_action", "DEDUPLICATED_KEEP_FIRST"),
            "records_in_output": 0,
        })

    for flag, cnt in silver_flag_data.items():
        if flag is None or cnt == 0:
            continue
        h = issue_handling.get(flag, {})
        pct = round(cnt / bronze_txn_count * 100, 2) if bronze_txn_count else 0.0
        dq_issues.append({
            "issue_type": h.get("issue_type", flag.lower()),
            "records_affected": cnt,
            "percentage_of_total": pct,
            "handling_action": h.get("handling_action", "FLAGGED_IN_OUTPUT"),
            "records_in_output": cnt,
        })

    if null_acct_count > 0:
        h = issue_handling.get("NULL_ACCOUNT_ID", {})
        pct = round(null_acct_count / bronze_acct_count * 100, 2) if bronze_acct_count else 0.0
        dq_issues.append({
            "issue_type": h.get("issue_type", "null_account_id"),
            "records_affected": null_acct_count,
            "percentage_of_total": pct,
            "handling_action": h.get("handling_action", "EXCLUDED_NULL_PK"),
            "records_in_output": 0,
        })

    # Detect stage: Stage 3 if stream files are present
    has_stream = bool(globmod.glob("/data/stream/stream_*.jsonl"))
    stage = "3" if has_stream else "2"

    report = {
        "$schema": "nedbank-de-challenge/dq-report/v1",
        "run_timestamp": datetime.fromtimestamp(run_start, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stage": stage,
        "source_record_counts": {
            "accounts_raw": bronze_acct_count,
            "transactions_raw": bronze_txn_count,
            "customers_raw": bronze_cust_count,
        },
        "dq_issues": dq_issues,
        "gold_layer_record_counts": {
            "fact_transactions": gold_txn_count,
            "dim_accounts": gold_acct_count,
            "dim_customers": gold_cust_count,
        },
        "execution_duration_seconds": int(time.time() - run_start),
    }

    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"[Pipeline] dq_report written to {report_path}")


def run_transformation():
    config = get_config()
    dq_rules = get_dq_rules()
    spark = get_spark()

    bronze_path = config["output"]["bronze_path"]
    silver_path = config["output"]["silver_path"]

    # Accounts before transactions (transactions needs accounts for ref integrity)
    transform_customers(spark, bronze_path, silver_path, dq_rules)
    transform_accounts(spark, bronze_path, silver_path, dq_rules)
    transform_transactions(spark, bronze_path, silver_path, dq_rules)

    print("[Silver] transformation complete")
