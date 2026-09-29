"""
Spark Session Helper Utility
Configures and initializes PySpark with Kafka connectors, local optimization, and delta/parquet support.
"""

import sys
import os
from pathlib import Path
from pyspark.sql import SparkSession
from src.utils.logger import get_logger

logger = get_logger("SparkUtils")

def get_spark_session(app_name: str = "IoTSmartBuildingPipeline", include_kafka: bool = True) -> SparkSession:
    """
    Initializes a local SparkSession optimized for streaming and batch processing.
    """
    logger.info(f"Initializing SparkSession: '{app_name}' (Include Kafka Jars: {include_kafka})")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")  # Optimal for local development
        .config("spark.default.parallelism", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )

    if include_kafka:
        # Add Spark SQL Kafka connector jar package for Spark 3.5.x
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4"
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession created successfully. Spark Version: {spark.version}")
    return spark
