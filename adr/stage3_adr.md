# Architecture Decision Record: Stage 3 Streaming Extension

**File:** `adr/stage3_adr.md`
**Author:** Prosper Sikhwari
**Date:** 2026-04-09
**Status:** Final

---

## Context

The mobile product team needed near-real-time balance and transaction views for the virtual account app. The daily batch pipeline (Bronze/Silver/Gold) was insufficient because customers expect balance updates within seconds of a transaction. The fintech agreed to provide a streaming feed: 12 pre-staged JSONL micro-batch files in `/data/stream/`, each containing 50-500 transaction events with the same schema as the Stage 2 batch `transactions.jsonl`.

Two new stream_gold Delta tables were required: `current_balances` (one row per account, upserted on each batch) and `recent_transactions` (last 50 per account, evicted on each batch). The SLA was 5 minutes from event timestamp to `updated_at` in the output tables.

Coming into Stage 3, the pipeline was approximately 450 lines of Python across 6 modules (`utils.py`, `ingest.py`, `transform.py`, `provision.py`, `stream_ingest.py`, `run_all.py`) plus two YAML config files. The batch pipeline was stable: all three validation queries passed, the DQ report was spec-compliant, and the Docker image ran in 105 seconds under the 2GB/2vCPU constraints.

---

## Decision 1: How did your existing Stage 1 architecture facilitate or hinder the streaming extension?

**What made Stage 3 easier:**

Three design choices from Stage 1/2 directly reduced the streaming work. First, extracting `get_spark()` with full Delta configuration into `pipeline/utils.py` meant `stream_ingest.py` could import it and get a Delta-capable SparkSession without duplicating 20 lines of builder config. If the session setup had stayed in `ingest.py`, the streaming module would have needed its own copy or a fragile `getOrCreate()` without Delta extensions.

Second, the Silver layer already carried `ingestion_timestamp` and `current_balance` through to the accounts table. When `_update_current_balances` needed a starting balance for new accounts appearing in the stream, it could read Silver accounts directly (`init_balances = spark.read.format("delta").load(silver_path + "/accounts").cache()`) without joining back to Bronze or Gold. This was originally done to fix a different problem (provision.py was re-reading Bronze for `ingestion_timestamp`), but it paid off again for streaming.

Third, the DQ rules in `config/dq_rules.yaml` already defined `currency_normalisation.target_value: ZAR`. The streaming `_parse_events` function applied the same normalisation (`F.lit("ZAR")`) without inventing new logic, keeping batch and stream consistent.

**What made Stage 3 harder:**

The biggest friction was that `run_all.py` was a linear sequence of `try/except` blocks with `sys.exit(1)` on failure. Adding `run_stream_ingestion()` as a fourth stage was trivial, but there was no way to run streaming independently of the batch pipeline. In production, batch and stream would likely run as separate processes or containers. The current design forces the full batch pipeline to complete before streaming starts, which wastes time if only the stream output needs updating.

The second friction point was that `provision.py` generated surrogate keys using `sha2(natural_key)`. The streaming tables (`current_balances`, `recent_transactions`) don't use surrogate keys at all; they're keyed on `account_id` and `(account_id, transaction_id)` directly. This wasn't a bug, but it meant the streaming module had no reuse of the `_sk()` helper and followed a fundamentally different key strategy.

**Code survival rate:**

Approximately 95% of the Stage 1/2 code survived intact. `stream_ingest.py` was written from scratch (~180 lines). `run_all.py` gained 4 lines (the `run_stream_ingestion` call and its `try/except`). No existing module was modified for streaming; all changes were additive.

---

## Decision 2: What design decisions in Stage 1 would you change in hindsight?

**Unified output module.** I would have created a single `pipeline/writers.py` module responsible for all Delta writes, rather than scattering `df.write.format("delta").mode("overwrite").save(path)` across `ingest.py`, `transform.py`, `provision.py`, and `stream_ingest.py`. When Stage 3 required Delta MERGE (not just overwrite), I had to import `DeltaTable` only in `stream_ingest.py`. A central writer module could have provided both `write_overwrite(df, path)` and `merge_upsert(spark, source, path, key_cols)` functions, reducing the 12 separate `.write.format("delta")` calls to 12 calls to a shared abstraction. This would also have been the natural place to add `.option("mergeSchema", "true")` for schema evolution, rather than hoping every individual write site remembers to include it.

**Config-driven Silver schemas.** The explicit `.select()` lists in `transform_customers` and `transform_accounts` are hardcoded in Python. I would have defined the Silver output columns in `config/pipeline_config.yaml` under a `silver_schemas` key:

```yaml
silver_schemas:
  customers: [customer_id, gender, province, income_band, segment, risk_score, kyc_status, dob]
  accounts: [account_id, customer_ref, account_type, account_status, open_date, product_tier, digital_channel, credit_limit, current_balance, last_activity_date]
```

Then each transform function would read its column list from config: `df.select(*config["silver_schemas"]["customers"])`. When Stage 2 added `merchant_subcategory` to transactions, I had to modify Python code instead of just updating a YAML file.

**Separate entry points for batch and stream.** I would have designed `run_all.py` to accept an environment variable (`PIPELINE_MODE=batch|stream|both`) rather than always running everything sequentially. This would allow the scoring system to run batch and stream independently, and would have made local debugging of the streaming loop faster (no need to wait for the full batch pipeline on every test run).

---

## Decision 3: How would you approach this differently if you had known Stage 3 was coming from the start?

If the full three-stage specification had been visible from Day 1, I would have made four structural changes.

**Ingestion as a pluggable interface.** Instead of `ingest.py` reading three hardcoded file paths, I would have designed an ingestion registry: a config block listing each source with its format, path, and reader options. Adding the streaming source would then be a config change (`{format: jsonl, path: /data/stream/, mode: poll}`) rather than a new Python file. The registry pattern also handles the Stage 2 addition of `merchant_subcategory` gracefully, since schema changes are absorbed by the reader config rather than requiring code modifications.

**Delta MERGE from the start.** My batch pipeline uses `mode("overwrite")` everywhere because it processes the full dataset each run. If I had known that Stage 3 required incremental upserts, I would have used Delta MERGE for the Gold layer writes from Stage 1. This means `build_fact_transactions` would merge on `transaction_id` rather than overwriting. The benefit is twofold: the batch pipeline becomes incremental-ready (can process only new Bronze data rather than reprocessing everything), and the streaming pipeline reuses the exact same MERGE logic. The `_update_current_balances` function's MERGE pattern (match on `account_id`, update balance delta, insert with initial balance) would have been a generalization of the batch Gold write, not a separate implementation.

**Shared event parsing.** The `_parse_events` function in `stream_ingest.py` duplicates logic from `transform_transactions`: both flatten nested structs, cast amounts to decimal, and normalise currency to ZAR. If I had known the streaming schema was identical to batch, I would have extracted a shared `parse_transaction_event(df)` function in `utils.py` that both `transform.py` and `stream_ingest.py` could call. This would ensure batch and stream data receive identical type casting and DQ treatment, eliminating the risk of subtle inconsistencies between the two paths.

**Single Gold output module.** Rather than `provision.py` for batch Gold and `stream_ingest.py` for stream Gold, I would have created `pipeline/gold.py` with functions for all five output tables (`dim_customers`, `dim_accounts`, `fact_transactions`, `current_balances`, `recent_transactions`). The batch and stream entry points would both call into this module. This keeps all Gold schema definitions in one place and makes it easy to verify that `current_balances.current_balance` uses the same `DECIMAL(18,2)` type as `dim_accounts.current_balance`.
