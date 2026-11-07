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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


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
    rows = raw.select(F.from_json(F.col("value").cast("string"), sink_schema).alias("r")).select("r.*")

    if a.mode == "append":
        rows.writeStream.format("iceberg").outputMode("append").option(
            "checkpointLocation", a.checkpoint
        ).toTable(target)
        return

    keys = [k for k in a.merge_keys.split(",") if k]
    if not keys:
        raise SystemExit("--mode merge needs --merge-keys")
    rows.writeStream.foreachBatch(
        lambda batch, _id: merge_batch(batch, keys, target)
    ).option("checkpointLocation", a.checkpoint).start().awaitTermination()


if __name__ == "__main__":
    main()
