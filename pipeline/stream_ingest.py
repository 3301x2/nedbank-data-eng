"""Stage 3 -- Streaming extension: process micro-batch JSONL files.

Polls /data/stream/ for JSONL files, processes them in lexicographic
(chronological) order, and writes two stream_gold Delta tables:

  current_balances    -- one row per account_id; upsert/merge
  recent_transactions -- last 50 rows per account; upsert + eviction

All 12 stream files are pre-staged at container start. The loop quiesces
after QUIESCE_TIMEOUT seconds of no new files and then returns.
"""

import glob as globmod
import os
import time
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

from pipeline.utils import get_config, get_spark

QUIESCE_TIMEOUT = 60   # seconds of inactivity before exit
POLL_INTERVAL = 30     # seconds between directory scans


def _parse_events(spark, fpath):
    """Read one stream JSONL file, cast types, normalise currency."""
    return (
        spark.read.json(fpath)
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp(
                F.concat_ws(" ", F.col("transaction_date"), F.col("transaction_time"))
            ),
        )
        .withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))
        .withColumn("currency", F.lit("ZAR"))
    )


def _update_current_balances(spark, events, path, init_balances, now_ts):
    """Upsert per-account running balance. DEBIT/FEE subtract, CREDIT/REVERSAL add."""
    sign_col = (
        F.when(F.col("transaction_type").isin(["CREDIT", "REVERSAL"]), F.lit(1))
        .otherwise(F.lit(-1))
    )

    # Aggregate net delta per account
    batch_agg = (
        events
        .withColumn("_delta", (sign_col * F.col("amount")).cast(DecimalType(18, 2)))
        .groupBy("account_id")
        .agg(
            F.sum("_delta").cast(DecimalType(18, 2)).alias("batch_delta"),
            F.max("transaction_timestamp").alias("max_ts"),
        )
    )

    # Join starting balance for first-time inserts
    source = (
        batch_agg
        .join(
            init_balances.select(
                "account_id",
                F.col("current_balance").cast(DecimalType(18, 2)).alias("init_balance"),
            ),
            on="account_id",
            how="left",
        )
        .withColumn(
            "init_balance",
            F.coalesce(F.col("init_balance"), F.lit(0).cast(DecimalType(18, 2))),
        )
        .withColumn("updated_at", F.lit(now_ts))
    )

    if os.path.exists(f"{path}/_delta_log"):
        dt = DeltaTable.forPath(spark, path)
        (
            dt.alias("t")
            .merge(source.alias("s"), "t.account_id = s.account_id")
            .whenMatchedUpdate(set={
                "current_balance": (
                    F.col("t.current_balance") + F.col("s.batch_delta")
                ).cast(DecimalType(18, 2)),
                "last_transaction_timestamp": F.greatest(
                    F.col("t.last_transaction_timestamp"), F.col("s.max_ts")
                ),
                "updated_at": F.col("s.updated_at"),
            })
            .whenNotMatchedInsert(values={
                "account_id": F.col("s.account_id"),
                "current_balance": (
                    F.col("s.init_balance") + F.col("s.batch_delta")
                ).cast(DecimalType(18, 2)),
                "last_transaction_timestamp": F.col("s.max_ts"),
                "updated_at": F.col("s.updated_at"),
            })
            .execute()
        )
    else:
        os.makedirs(path, exist_ok=True)
        (
            source
            .select(
                "account_id",
                (F.col("init_balance") + F.col("batch_delta"))
                .cast(DecimalType(18, 2))
                .alias("current_balance"),
                F.col("max_ts").alias("last_transaction_timestamp"),
                "updated_at",
            )
            .write.format("delta").mode("overwrite").save(path)
        )


def _update_recent_transactions(spark, events, path, now_ts):
    """Upsert events, then evict rows beyond 50 per account."""
    source = (
        events
        .select(
            "account_id",
            "transaction_id",
            "transaction_timestamp",
            F.col("amount").cast(DecimalType(18, 2)).alias("amount"),
            "transaction_type",
            "channel",
        )
        .withColumn("updated_at", F.lit(now_ts))
    )

    if os.path.exists(f"{path}/_delta_log"):
        dt = DeltaTable.forPath(spark, path)
        (
            dt.alias("t")
            .merge(
                source.alias("s"),
                "t.account_id = s.account_id AND t.transaction_id = s.transaction_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        os.makedirs(path, exist_ok=True)
        source.write.format("delta").mode("overwrite").save(path)

    # Evict rows ranked >50 per account via Delta DELETE
    w = Window.partitionBy("account_id").orderBy(F.col("transaction_timestamp").desc())
    cutoffs = (
        spark.read.format("delta").load(path)
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 50)
        .select("account_id", F.col("transaction_timestamp").alias("cutoff_ts"))
    )
    # No-op if no account has >50 rows yet
    dt = DeltaTable.forPath(spark, path)
    (
        dt.alias("t")
        .merge(cutoffs.alias("c"), "t.account_id = c.account_id")
        .whenMatchedDelete(condition="t.transaction_timestamp < c.cutoff_ts")
        .execute()
    )


def run_stream_ingestion():
    config = get_config()
    spark = get_spark()

    stream_dir = "/data/stream"
    stream_gold = "/data/output/stream_gold"
    current_balances_path = f"{stream_gold}/current_balances"
    recent_txn_path = f"{stream_gold}/recent_transactions"

    # Starting balances from batch pipeline (cached for reuse across files)
    init_balances = spark.read.format("delta").load(
        config["output"]["silver_path"] + "/accounts"
    ).cache()

    processed = set()
    last_new_file_ts = time.time()
    print("[Stream] starting poll loop")

    while True:
        all_files = sorted(globmod.glob(f"{stream_dir}/stream_*.jsonl"))
        new_files = [f for f in all_files if f not in processed]

        if new_files:
            last_new_file_ts = time.time()
            for fpath in new_files:
                print(f"[Stream] processing {os.path.basename(fpath)}")
                now_ts = datetime.now(timezone.utc)
                events = _parse_events(spark, fpath)
                _update_current_balances(
                    spark, events, current_balances_path, init_balances, now_ts
                )
                _update_recent_transactions(spark, events, recent_txn_path, now_ts)
                processed.add(fpath)
                print(f"[Stream] done {os.path.basename(fpath)}")

        if time.time() - last_new_file_ts > QUIESCE_TIMEOUT:
            print(f"[Stream] no new files for {QUIESCE_TIMEOUT}s, stopping")
            break

        time.sleep(POLL_INTERVAL)

    print("[Stream] streaming ingestion complete")
