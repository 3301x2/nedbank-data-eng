"""Shared utilities used by all pipeline stages."""

import glob as globmod
import os
import yaml
from pyspark.sql import SparkSession


def get_config():
    """Load pipeline config from yaml."""
    config_path = os.environ.get(
        "PIPELINE_CONFIG", "/data/config/pipeline_config.yaml"
    )
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_spark():
    """Return a Delta-capable SparkSession (creates or reuses)."""
    jar_dir = os.environ.get("DELTA_JARS", "/opt/delta-jars")
    jars = ",".join(globmod.glob(f"{jar_dir}/*.jar"))

    return (
        SparkSession.builder
        .master("local[2]")
        .appName("nedbank-de-pipeline")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        .config("spark.jars", jars)
        .config("spark.driver.extraJavaOptions",
                "-Djava.io.tmpdir=/tmp -Dorg.xerial.snappy.tempdir=/tmp")
        .config("spark.executor.extraJavaOptions",
                "-Djava.io.tmpdir=/tmp -Dorg.xerial.snappy.tempdir=/tmp")
        .config("spark.sql.parquet.compression.codec", "gzip")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
