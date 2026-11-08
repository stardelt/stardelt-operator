"""A StreamApp: one Kafka topic in, one Iceberg table out (D42).

Args (set by the operator's builder, never by hand):
  --bootstrap-servers  the Kafka bootstrap address from the source `Connection`
  --topic              the topic to read
  --catalog            the Iceberg catalog name to write through (e.g. `shared`)
  --catalog-uri        the REST catalog endpoint
  --table              `schema.table` in that catalog
  --mode               `merge` (foreachBatch + MERGE) or `append`
  --merge-keys         comma-separated columns for the MERGE's ON, required for `merge`
  --checkpoint         the checkpoint location (s3a://…)
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap-servers", required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--catalog-uri", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--mode", choices=("merge", "append"), default="merge")
    p.add_argument("--merge-keys", default="")
    p.add_argument("--checkpoint", required=True)
    return p.parse_args()


def spark(catalog: str, catalog_uri: str) -> SparkSession:
    return (
        SparkSession.builder.appName("streamapp")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "rest")
        .config(f"spark.sql.catalog.{catalog}.uri", catalog_uri)
        .config(f"spark.sql.catalog.{catalog}.warehouse", catalog)
        .config(
            f"spark.sql.catalog.{catalog}.credential",
            f"{os.environ['CATALOG_CLIENT_ID']}:{os.environ['CATALOG_CLIENT_SECRET']}",
        )
        .config(
            f"spark.sql.catalog.{catalog}.oauth2-server-uri", os.environ["CATALOG_TOKEN_URL"]
        )
        .config(f"spark.sql.catalog.{catalog}.scope", "openid")
        .getOrCreate()
    )


def merge_batch(batch: DataFrame, keys: list[str], target: str) -> None:
    # A Kafka micro-batch can carry the same merge key twice (a producer retry, or two
    # messages for one measurement time). Iceberg's MERGE refuses a source with duplicate
    # ON keys (MERGE_CARDINALITY_VIOLATION) and the checkpoint replays the same batch, so
    # the stream would fail for ever. The sink is a current-state table: keep one row per
    # key and let the MERGE update it. The row with the *newest* Kafka offset wins, so a
    # replayed or superseded message cannot overwrite a newer value.
    window = Window.partitionBy(*keys).orderBy(F.col("_kafka_offset").desc())
    batch = (
        batch.withColumn("_row", F.row_number().over(window))
        .where(F.col("_row") == 1)
        .drop("_row", "_kafka_offset")
    )
    batch.createOrReplaceTempView("batch")
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    columns = batch.columns
    insert = ", ".join(columns)
    values = ", ".join(f"s.{c}" for c in columns)
    update = ", ".join(f"t.{c} = s.{c}" for c in columns if c not in keys)
    batch.sparkSession.sql(
        f"MERGE INTO {target} t USING batch s ON {on} "
        f"WHEN MATCHED THEN UPDATE SET {update} "
        f"WHEN NOT MATCHED THEN INSERT ({insert}) VALUES ({values})"
    )


def main() -> None:
    a = parse_args()
    s = spark(a.catalog, a.catalog_uri)
    target = f"{a.catalog}.{a.table}"
    sink_schema = s.table(target).schema

    raw = (
        s.readStream.format("kafka")
        .option("kafka.bootstrap.servers", a.bootstrap_servers)
        .option("subscribe", a.topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    # Spark's JSON parser returns null for the ISO-8601 timestamps the PEGELONLINE DAG
    # writes (`2026-11-07T12:45:00Z`), and the sink columns are NOT NULL. Read those
    # fields as strings, then parse them with the session parser (which also accepts the
    # fractional seconds in `ingested_at`).
    def json_schema(schema: StructType) -> StructType:
        return StructType(
            [
                StructField(
                    f.name,
                    StringType()
                    if type(f.dataType).__name__ in ("TimestampType", "TimestampNTZType")
                    else f.dataType,
                    f.nullable,
                )
                for f in schema.fields
            ]
        )

    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), json_schema(sink_schema)).alias("r"),
        F.col("offset").alias("_kafka_offset"),
    ).select("r.*", "_kafka_offset")
    columns = [
        F.to_timestamp(
            F.regexp_replace(F.regexp_replace(F.col(f.name), "T", " "), "Z$", "")
        ).alias(f.name)
        if type(f.dataType).__name__ in ("TimestampType", "TimestampNTZType")
        else F.col(f.name)
        for f in sink_schema.fields
    ]
    rows = parsed.select(*columns)
    merge_rows = parsed.select(*columns, F.col("_kafka_offset"))

    if a.mode == "append":
        (
            rows.writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", a.checkpoint)
            .toTable(target)
            .awaitTermination()
        )
        return

    keys = [k for k in a.merge_keys.split(",") if k]
    if not keys:
        raise SystemExit("--mode merge needs --merge-keys")
    merge_rows.writeStream.foreachBatch(
        lambda batch, _id: merge_batch(batch, keys, target)
    ).option("checkpointLocation", a.checkpoint).start().awaitTermination()


if __name__ == "__main__":
    main()
