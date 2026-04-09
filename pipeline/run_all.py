"""Pipeline entry point. Runs batch stages then streaming, writes DQ report."""

import sys
import time
import traceback

from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation, write_dq_report
from pipeline.provision import run_provisioning
from pipeline.stream_ingest import run_stream_ingestion
from pipeline.utils import get_config, get_spark


if __name__ == "__main__":
    run_start = time.time()

    try:
        run_ingestion()
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        print(f"[ERROR] Ingestion failed: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        run_transformation()
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        print(f"[ERROR] Transformation failed: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        run_provisioning()
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        print(f"[ERROR] Provisioning failed: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        run_stream_ingestion()
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        print(f"[ERROR] Stream ingestion failed: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        write_dq_report(get_spark(), get_config(), run_start)
    except Exception as e:
        print(f"[WARN] dq_report failed: {e}", file=sys.stderr)
    finally:
        get_spark().stop()
