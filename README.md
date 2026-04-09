# Nedbank DE Challenge -- Medallion Pipeline

PySpark + Delta Lake pipeline processing fintech transaction, account, and customer data through Bronze/Silver/Gold layers inside a resource-constrained Docker container (2GB RAM, 2 vCPU, read-only filesystem, no network).

## Architecture

```
accounts.csv  ──┐                  ┌── dim_customers (9 fields)
customers.csv ──┼── Bronze ── Silver ── Gold ──┼── dim_accounts  (11 fields)
transactions  ──┘   (raw)    (typed,   (dimensional   └── fact_transactions (15 fields)
   .jsonl                    deduped,    model)
                             flagged)

stream_*.jsonl ── stream_ingest ── stream_gold ──┬── current_balances    (4 fields)
                  (Delta MERGE)                  └── recent_transactions (7 fields)
```

## Pipeline Stages

| Step | Module | What it does |
|---|---|---|
| Ingest | `pipeline/ingest.py` | Reads raw CSV/JSONL into Bronze Delta tables with `ingestion_timestamp`. No transformations. |
| Transform | `pipeline/transform.py` | Types, deduplicates, DQ-flags Bronze into Silver. Explicit `select()` enforces Silver schema contract. |
| Provision | `pipeline/provision.py` | Builds dimensional Gold model. Surrogate keys via `sha2` hash (no global sort). |
| Stream (Stage 3 only) | `pipeline/stream_ingest.py` | Polls `/data/stream/` for micro-batch JSONL. Delta MERGE upserts into `stream_gold/`. |

Entry point: `pipeline/run_all.py` runs all four steps sequentially, writes `dq_report.json` after Gold is complete, then calls `spark.stop()`.

## Key Design Decisions

**Config-driven DQ rules.** All null checks, domain checks, currency normalisation rules, dedup keys, and DQ report issue mappings live in `config/dq_rules.yaml`. No DQ logic is hardcoded in Python.

**Silver as a schema contract.** Silver transforms use explicit `.select()` lists rather than `.drop()`. If Bronze schema drifts (new upstream columns), Silver ignores them by default. PII columns (`id_number`, `first_name`, `last_name`, `mobile_number`) are excluded at this boundary.

**sha2 surrogate keys.** `provision.py` uses `sha2(natural_key, 256)` truncated to 60 bits for surrogate keys. Deterministic (same input always produces the same key), partition-safe (no global sort), and idempotent across re-runs. Collision probability is negligible at contest data scales (~10^-9 at 1M rows).

**No `.collect()` or `.count()` in processing.** The DQ report uses `groupBy().count().write.json()` to write aggregated results to temp files, then reads them back with Python file I/O. This avoids both `.collect()` (Spark action pulling data to driver) and standalone `.count()` (full scan) in the pipeline's hot path.

**gzip compression.** Parquet codec set to gzip instead of snappy. Snappy's native library requires a writable filesystem for temp files; the scoring container runs `--read-only`. gzip is pure Java and works on any filesystem.

**Streaming eviction via Delta DELETE.** `recent_transactions` keeps the 50 most recent rows per account. After each MERGE, the pipeline computes the 50th row's timestamp per account and issues a `whenMatchedDelete` for older rows. This preserves the Delta transaction log (no full overwrite).

## How to Run

```bash
# Build base image (once)
docker build -t nedbank-de-challenge/base:1.0 -f Dockerfile.base .

# Build pipeline image
docker build -t nedbank-de-challenge/pipeline:latest .

# Run with scoring-equivalent constraints
docker run --rm \
  --network=none --memory=2g --memory-swap=2g --cpus=2 \
  --read-only --tmpfs /tmp:rw,size=512m \
  -v "$(pwd)/data/input:/data/input:ro" \
  -v "$(pwd)/data/config:/data/config:ro" \
  -v "$(pwd)/data/output:/data/output" \
  -v "$(pwd)/data/stream:/data/stream:ro" \
  nedbank-de-challenge/pipeline:latest

# Validate with DuckDB
duckdb < docs/validation_queries.sql

# Run local test harness
bash run_tests.sh --stage 2 --data-dir ./data --image nedbank-de-challenge/pipeline:latest
```

## Project Structure

```
Dockerfile                      # Extends nedbank-de-challenge/base:1.0
requirements.txt                # Additional Python dependencies
pipeline/
  run_all.py                    # Entry point -- orchestrates all steps
  utils.py                      # Shared get_config(), get_spark()
  ingest.py                     # Bronze: raw files to Delta
  transform.py                  # Silver: DQ, types, dedup
  provision.py                  # Gold: dimensional model
  stream_ingest.py              # Streaming: micro-batch JSONL to stream_gold
config/
  pipeline_config.yaml          # All paths and Spark settings
  dq_rules.yaml                 # DQ rules, domain checks, issue handling
adr/
  stage3_adr.md                 # Architecture Decision Record for streaming
run_tests.sh                    # Local testing harness (7 checks, bugs fixed)
```

## Spark Configuration

| Setting | Value | Reason |
|---|---|---|
| `master` | `local[2]` | Matches 2-vCPU Docker constraint |
| `spark.driver.memory` | `1g` | Executor memory is ignored in local mode |
| `spark.sql.shuffle.partitions` | `4` | Prevents default 200-partition overhead |
| `spark.sql.parquet.compression.codec` | `gzip` | Read-only filesystem compatibility |
| `java.io.tmpdir` | `/tmp` | All temp writes to the writable tmpfs mount |

## Bugs Found in Starter Kit

**`run_tests.sh` -- DuckDB output parsing broken.** The test harness shipped with the starter kit uses `grep -E '^[0-9]+$'` to extract row counts from DuckDB output. DuckDB renders results as formatted tables with box-drawing characters (e.g. `│ 1000000 │`), so the grep never matches and every DuckDB-dependent check (4, 5, and 7) reports `FAIL` even when the pipeline output is correct. Fixed by adding `-csv -noheader` flags to all `duckdb -c` invocations and switching `awk` field separators to comma-delimited.

**`run_tests.sh` -- DQ report key validation uses wrong schema.** Check 6 validates `dq_report.json` against keys `total_records`, `clean_records`, `flagged_records`, `flag_counts`, but the actual scoring schema (`docs/dq_report_template.json`) requires `source_record_counts`, `dq_issues`, `gold_layer_record_counts`. Fixed to validate against the correct schema.

## AI Disclosure

This pipeline was developed with the assistance of Claude (Anthropic). All code has been reviewed, understood, and can be explained and defended line by line.
