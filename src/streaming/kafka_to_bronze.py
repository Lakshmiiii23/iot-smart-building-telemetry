"""
Bronze Layer Ingestion: Kafka to Bronze Parquet Storage
Consumes raw IoT sensor readings from Kafka and persists them append-only into
the Bronze Lakehouse layer with Kafka metadata preservation.
Supports:
1. PySpark Structured Streaming engine (distributed scale)
2. Direct Kafka-to-Parquet micro-batch engine (fast local development)
"""

import sys
import os
import json
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_SENSORS,
    KAFKA_CONSUMER_GROUP_BRONZE,
    BRONZE_DIR,
    CHECKPOINTS_DIR,
    STREAMING_TRIGGER_SECONDS
)
from src.utils.logger import get_logger

logger = get_logger("KafkaToBronze")


def run_spark_streaming_ingestion():
    """Ingests from Kafka to Bronze using PySpark Structured Streaming."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
    )
    from pyspark.sql.functions import (
        col, from_json, current_timestamp, to_timestamp
    )
    from src.utils.spark_utils import get_spark_session

    spark = get_spark_session("IoT-Kafka-To-Bronze", include_kafka=True)

    # Define IoT JSON schema
    payload_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("building_id", StringType(), False),
        StructField("floor", IntegerType(), False),
        StructField("room_id", StringType(), False),
        StructField("room_type", StringType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("humidity_pct", DoubleType(), True),
        StructField("occupancy", IntegerType(), True),
        StructField("energy_kw", DoubleType(), True),
        StructField("co2_ppm", DoubleType(), True),
        StructField("simulated_anomaly", StringType(), True)
    ])

    logger.info(f"Connecting PySpark Structured Streaming to Kafka topic '{KAFKA_TOPIC_SENSORS}'...")

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_SENSORS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON value and retain Kafka metadata for data lineage and auditing
    parsed_stream = (
        kafka_stream
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), payload_schema).alias("payload")
        )
        .select(
            "payload.event_id",
            to_timestamp("payload.timestamp").alias("event_timestamp"),
            "payload.building_id",
            "payload.floor",
            "payload.room_id",
            "payload.room_type",
            "payload.temperature_c",
            "payload.humidity_pct",
            "payload.occupancy",
            "payload.energy_kw",
            "payload.co2_ppm",
            "payload.simulated_anomaly",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            current_timestamp().alias("ingestion_timestamp")
        )
    )

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint_dir = CHECKPOINTS_DIR / "bronze_stream"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Writing streaming output to Bronze Parquet at: {BRONZE_DIR}")
    logger.info(f"Checkpoints stored at: {checkpoint_dir}")

    query = (
        parsed_stream.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", str(BRONZE_DIR))
        .option("checkpointLocation", str(checkpoint_dir))
        .trigger(processingTime=STREAMING_TRIGGER_SECONDS)
        .partitionBy("building_id")
        .start()
    )

    query.awaitTermination()


def run_direct_streaming_ingestion(batch_interval: float = 3.0, max_batches: int = None):
    """
    Direct Kafka-to-Parquet micro-batch consumer.
    Provides fast, lightweight ingestion that persists directly to Bronze Parquet.
    """
    from kafka import KafkaConsumer

    logger.info(f"Starting Direct Kafka-to-Bronze consumer for topic '{KAFKA_TOPIC_SENSORS}'...")
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    consumer = KafkaConsumer(
        KAFKA_TOPIC_SENSORS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=KAFKA_CONSUMER_GROUP_BRONZE,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=2000
    )

    batch_count = 0
    total_records = 0

    try:
        while True:
            records_buffer = []
            start_time = time.time()

            # Poll for records
            raw_records = consumer.poll(timeout_ms=1500, max_records=500)
            now_utc = datetime.now(timezone.utc).isoformat()

            for tp, messages in raw_records.items():
                for msg in messages:
                    val = msg.value
                    val["kafka_partition"] = msg.partition
                    val["kafka_offset"] = msg.offset
                    val["kafka_timestamp"] = datetime.fromtimestamp(msg.timestamp / 1000.0, timezone.utc).isoformat()
                    val["ingestion_timestamp"] = now_utc
                    records_buffer.append(val)

            if records_buffer:
                batch_count += 1
                total_records += len(records_buffer)
                df = pd.DataFrame(records_buffer)

                # Ensure timestamps are parsed
                df["event_timestamp"] = pd.to_datetime(df["timestamp"])
                df["ingestion_timestamp"] = pd.to_datetime(df["ingestion_timestamp"])

                # Partition and write per building
                for bldg_id, group in df.groupby("building_id"):
                    bldg_dir = BRONZE_DIR / f"building_id={bldg_id}"
                    bldg_dir.mkdir(parents=True, exist_ok=True)
                    batch_filename = bldg_dir / f"part_{int(time.time()*1000)}_{batch_count}.parquet"
                    group.to_parquet(batch_filename, index=False, engine="pyarrow")

                logger.info(f"Bronze Batch {batch_count}: Ingested {len(records_buffer)} events. Total: {total_records}")
                empty_polls = 0
            else:
                empty_polls = locals().get("empty_polls", 0) + 1
                if max_batches and empty_polls >= 2:
                    logger.info("Kafka topic caught up (no new records). Completing batch ingestion.")
                    break

            if max_batches and batch_count >= max_batches:
                break

            time.sleep(max(0.1, batch_interval - (time.time() - start_time)))

    except KeyboardInterrupt:
        logger.info("Direct Bronze Ingestion stopped by user.")
    finally:
        consumer.close()
        logger.info(f"Direct Ingestion closed. Total Bronze events stored: {total_records}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka to Bronze Lakehouse Ingestion")
    parser.add_argument("--engine", choices=["spark", "direct"], default="direct",
                        help="Ingestion engine: 'spark' for PySpark Structured Streaming, 'direct' for lightweight Kafka consumer")
    parser.add_argument("--batches", type=int, default=None, help="Max batches to process (for testing)")
    args = parser.parse_args()

    if args.engine == "spark":
        run_spark_streaming_ingestion()
    else:
        run_direct_streaming_ingestion(max_batches=args.batches)
