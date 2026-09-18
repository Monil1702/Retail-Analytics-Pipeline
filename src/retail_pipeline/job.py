"""Spark entrypoint for retail transaction processing."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone


def upsert_partition(rows) -> None:
    """Write one Spark partition with a single PostgreSQL transaction."""
    import psycopg

    values = [tuple(row[name] for name in (
        "transaction_id", "customer_id", "product_id", "transaction_ts", "quantity",
        "unit_price", "channel", "region", "updated_at", "sales_amount", "loaded_at"
    )) for row in rows]
    if not values:
        return
    connection = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "retail_analytics"),
        user=os.getenv("POSTGRES_USER", "retail"),
        password=os.environ["POSTGRES_PASSWORD"],
    )
    statement = """
        INSERT INTO analytics.retail_transactions
        (transaction_id, customer_id, product_id, transaction_ts, quantity, unit_price,
         channel, region, updated_at, sales_amount, loaded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO UPDATE SET
          customer_id = EXCLUDED.customer_id,
          product_id = EXCLUDED.product_id,
          transaction_ts = EXCLUDED.transaction_ts,
          quantity = EXCLUDED.quantity,
          unit_price = EXCLUDED.unit_price,
          channel = EXCLUDED.channel,
          region = EXCLUDED.region,
          updated_at = EXCLUDED.updated_at,
          sales_amount = EXCLUDED.sales_amount,
          loaded_at = EXCLUDED.loaded_at
        WHERE EXCLUDED.updated_at >= analytics.retail_transactions.updated_at
    """
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.executemany(statement, values)
    finally:
        connection.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--quality-output", required=True)
    parser.add_argument("--quarantine-output", required=True)
    parser.add_argument("--skip-database", action="store_true")
    return parser


def run(args: argparse.Namespace) -> dict[str, int | str]:
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType, TimestampType

    spark = SparkSession.builder.appName("retail-analytics-pipeline").getOrCreate()
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("product_id", StringType()),
        StructField("transaction_ts", TimestampType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DecimalType(12, 2)),
        StructField("channel", StringType()),
        StructField("region", StringType()),
        StructField("updated_at", TimestampType()),
    ])
    raw = spark.read.option("header", True).schema(schema).csv(args.input)
    invalid_condition = (
        F.col("transaction_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("product_id").isNull()
        | F.col("transaction_ts").isNull()
        | F.col("updated_at").isNull()
        | F.col("quantity").isNull()
        | (F.col("quantity") <= 0)
        | F.col("unit_price").isNull()
        | (F.col("unit_price") < 0)
        | ~F.col("channel").isin("web", "store", "mobile", "marketplace")
    )
    invalid = raw.filter(invalid_condition).withColumn("quarantined_at", F.current_timestamp())
    valid = raw.filter(~invalid_condition)
    window = Window.partitionBy("transaction_id").orderBy(F.col("updated_at").desc())
    accepted = (
        valid.withColumn("row_number", F.row_number().over(window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
        .withColumn("sales_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("loaded_at", F.current_timestamp())
    )
    input_count = raw.count()
    valid_count = valid.count()
    accepted_count = accepted.count()
    invalid_count = invalid.count()
    report = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "input_rows": input_count,
        "accepted_rows": accepted_count,
        "invalid_rows": invalid_count,
        "duplicates_removed": valid_count - accepted_count,
    }
    invalid.write.mode("overwrite").json(args.quarantine_output)
    spark.createDataFrame([(json.dumps(report),)], ["report_json"]).coalesce(1).write.mode("overwrite").text(args.quality_output)

    if not args.skip_database:
        accepted.foreachPartition(upsert_partition)
    spark.stop()
    return report


if __name__ == "__main__":
    run(build_parser().parse_args())
