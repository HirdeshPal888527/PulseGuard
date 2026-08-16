import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "transactions")
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_DB = os.environ.get("PG_DB", "anomaly_engine")
PG_USER = os.environ.get("PG_USER", "anomaly")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "anomaly")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:5432/{PG_DB}"
JDBC_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

Z_THRESHOLD = 3.0

EVENT_SCHEMA = StructType([
    StructField("user_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("amount", DoubleType()),
    StructField("location", StringType()),
    StructField("device_id", StringType()),
    StructField("injected_anomaly", BooleanType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("realtime-anomaly-engine")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3",
        )
        .getOrCreate()
    )


def read_stream(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json("json_str", EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
    )
    return parsed


def compute_windowed_stats(parsed_df):
    return (
        parsed_df
        .withWatermark("event_time", "1 minute")
        .groupBy(
            F.window("event_time", "5 minutes", "10 seconds"),
            F.col("user_id"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.avg("amount").alias("mean_amount"),
            F.stddev_samp("amount").alias("stddev_amount"),
            F.max("amount").alias("max_amount"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "user_id", "event_count", "mean_amount", "stddev_amount", "max_amount",
        )
    )


def write_window_metrics(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    (
        batch_df.write.mode("append")
        .jdbc(JDBC_URL, "window_metrics", properties=JDBC_PROPS)
    )


def write_anomalies(parsed_df, windowed_stats_df, batch_id_holder):
    def _process(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        stats = (
            batch_df.sql_ctx.read
            .jdbc(JDBC_URL, "window_metrics", properties=JDBC_PROPS)
        )
        latest_stats = (
            stats.groupBy("user_id")
            .agg(F.max("window_end").alias("latest_window_end"))
            .join(stats, ["user_id"])
            .where(F.col("window_end") == F.col("latest_window_end"))
            .select("user_id", "mean_amount", "stddev_amount")
        )

        joined = batch_df.join(latest_stats, on="user_id", how="inner")

        scored = joined.withColumn(
            "z_score",
            F.when(
                F.col("stddev_amount") > 0,
                (F.col("amount") - F.col("mean_amount")) / F.col("stddev_amount"),
            ).otherwise(F.lit(0.0)),
        )

        flagged = (
            scored.where(F.abs(F.col("z_score")) > Z_THRESHOLD)
            .select(
                F.col("event_time"),
                "user_id", "amount", "z_score",
                F.col("mean_amount").alias("window_mean"),
                F.col("stddev_amount").alias("window_stddev"),
                "location", "device_id",
            )
        )

        if not flagged.rdd.isEmpty():
            flagged.write.mode("append").jdbc(JDBC_URL, "anomalies", properties=JDBC_PROPS)

    return _process


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    parsed = read_stream(spark)
    windowed = compute_windowed_stats(parsed)

    metrics_query = (
        windowed.writeStream
        .outputMode("update")
        .foreachBatch(write_window_metrics)
        .option("checkpointLocation", "/tmp/checkpoints/window_metrics")
        .trigger(processingTime="10 seconds")
        .start()
    )

    anomaly_query = (
        parsed.writeStream
        .outputMode("append")
        .foreachBatch(write_anomalies(parsed, windowed, {}))
        .option("checkpointLocation", "/tmp/checkpoints/anomalies")
        .trigger(processingTime="10 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
