"""
End-to-End Orchestrator & CLI Runner for IoT Smart Building Telemetry Pipeline
Allows running individual phases or the full end-to-end workflow:
1. Seed SCD Type 2 Room Dimension
2. Produce IoT Telemetry to Kafka
3. Ingest Kafka events into Bronze Parquet
4. Cleanse, detect anomalies & enrich in Silver
5. Compute business KPIs in Gold
6. Launch interactive Streamlit dashboard
"""

import sys
import os
import time
import argparse
from pathlib import Path

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Ensure robust UTF-8 printing on Windows PowerShell
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from src.utils.logger import get_logger

logger = get_logger("PipelineOrchestrator")


def run_seed_dimensions():
    """Seeds the SCD Type 2 room dimension table."""
    from src.batch.scd2_room_dimension import run_scd2_pipeline
    logger.info("Executing Phase: Seed SCD Type 2 Dimensions...")
    run_scd2_pipeline()


def run_producer(ticks: int = 20, interval: float = 1.0):
    """Produces telemetry to Kafka."""
    from src.producer.sensor_simulator import IoTSensorSimulator
    logger.info(f"Executing Phase: IoT Sensor Simulation ({ticks} ticks, {interval}s interval)...")
    simulator = IoTSensorSimulator()
    simulator.run(interval_seconds=interval, max_ticks=ticks)


def run_bronze_ingestion(batches: int = 5):
    """Ingests from Kafka to Bronze layer."""
    from src.streaming.kafka_to_bronze import run_direct_streaming_ingestion
    logger.info(f"Executing Phase: Ingestion to Bronze Lakehouse ({batches} batches)...")
    run_direct_streaming_ingestion(max_batches=batches)


def run_silver_processing():
    """Cleanses, flags anomalies, and enriches in Silver layer."""
    from src.batch.bronze_to_silver import run_silver_pipeline
    logger.info("Executing Phase: Bronze -> Silver Processing & Anomaly Detection...")
    run_silver_pipeline()


def run_gold_aggregations():
    """Computes Gold KPIs and efficiency scorecards."""
    from src.batch.silver_to_gold import run_gold_pipeline
    logger.info("Executing Phase: Silver -> Gold Aggregations...")
    run_gold_pipeline()


def run_demo(ticks: int = 15):
    """Executes a complete end-to-end test and validation cycle."""
    print("=" * 70)
    print("🚀 RUNNING END-TO-END IOT TELEMETRY PIPELINE DEMO")
    print("=" * 70)

    # 1. Seed Dimensions
    run_seed_dimensions()

    # 2. Produce Telemetry
    run_producer(ticks=ticks, interval=0.5)

    # 3. Bronze Ingestion
    run_bronze_ingestion(batches=3)

    # 4. Silver Processing & Anomaly Engine
    run_silver_processing()

    # 5. Gold Aggregations
    run_gold_aggregations()

    print("\n" + "=" * 70)
    print("✨ END-TO-END DEMO COMPLETED SUCCESSFULLY!")
    print("To launch the real-time Streamlit dashboard, run:")
    print("  python run_pipeline.py --dashboard")
    print("=" * 70)


def launch_dashboard():
    """Launches the Streamlit operations dashboard."""
    import subprocess
    cmd = [sys.executable, "-m", "streamlit", "run", "src/dashboard/app.py", "--server.port=8501"]
    logger.info("Launching Streamlit Dashboard on http://localhost:8501...")
    subprocess.run(cmd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IoT Telemetry Pipeline Runner")
    parser.add_argument("--demo", action="store_true", help="Run full end-to-end pipeline demo")
    parser.add_argument("--seed-dim", action="store_true", help="Generate SCD Type 2 dimension")
    parser.add_argument("--produce", action="store_true", help="Run sensor simulator producer")
    parser.add_argument("--bronze", action="store_true", help="Ingest Kafka messages to Bronze")
    parser.add_argument("--silver", action="store_true", help="Run Silver cleansing and anomaly engine")
    parser.add_argument("--gold", action="store_true", help="Compute Gold business aggregations")
    parser.add_argument("--dashboard", action="store_true", help="Launch Streamlit web dashboard")
    parser.add_argument("--ticks", type=int, default=20, help="Number of ticks for simulator")
    args = parser.parse_args()

    if args.demo:
        run_demo(ticks=args.ticks)
    elif args.seed_dim:
        run_seed_dimensions()
    elif args.produce:
        run_producer(ticks=args.ticks)
    elif args.bronze:
        run_bronze_ingestion()
    elif args.silver:
        run_silver_processing()
    elif args.gold:
        run_gold_aggregations()
    elif args.dashboard:
        launch_dashboard()
    else:
        parser.print_help()
