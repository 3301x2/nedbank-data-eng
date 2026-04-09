FROM nedbank-de-challenge/base:1.0

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline/ pipeline/
COPY config/ config/

ENV PYTHONPATH=/app
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV SPARK_LOCAL_HOSTNAME=localhost
ENV SPARK_LOCAL_IP=127.0.0.1

# Download Delta jars at build time, copy to a fixed path
RUN python -c "\
from pyspark.sql import SparkSession; \
from delta import configure_spark_with_delta_pip; \
builder = SparkSession.builder.master('local[1]') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog'); \
spark = configure_spark_with_delta_pip(builder).getOrCreate(); \
spark.stop()" 2>/dev/null || true \
    && mkdir -p /opt/delta-jars \
    && cp /root/.ivy2/jars/*.jar /opt/delta-jars/ 2>/dev/null || true \
    && cp /root/.ivy2/cache/io.delta/*/jars/*.jar /opt/delta-jars/ 2>/dev/null || true \
    && cp /root/.ivy2/cache/org.antlr/*/jars/*.jar /opt/delta-jars/ 2>/dev/null || true

# Point Spark directly at the jars (no Ivy needed at runtime)
ENV DELTA_JARS=/opt/delta-jars

CMD ["python", "pipeline/run_all.py"]
