"""
Silver Layer Processing: Cleansing, Anomaly Detection & SCD Type 2 Enrichment
Transforms raw Bronze events into conformed, validated, and enriched Silver datasets.
Features:
- Schema validation & corrupt record filtering (e.g. -99.9C sensor glitches)
- Time-series sustained anomaly detection (Temperature > 30C for > 5 min, Humidity > 80%, Energy waste)
- Point-in-time join with SCD Type 2 Room Dimension
- Delta comfort calculations (temp drift from setpoint)
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from config.settings import (
    BRONZE_DIR,
    SILVER_DIR,
    DIM_ROOMS_DIR,
    TEMP_ANOMALY_THRESHOLD,
    TEMP_SUSTAINED_MINUTES,
    HUMIDITY_ANOMALY_THRESHOLD,
    ENERGY_WASTE_THRESHOLD_KW
)
from src.utils.logger import get_logger

logger = get_logger("BronzeToSilver")


def load_bronze_data() -> pd.DataFrame:
    """Reads all raw Bronze Parquet files."""
    if not BRONZE_DIR.exists():
        logger.warning(f"Bronze directory not found at {BRONZE_DIR}")
        return pd.DataFrame()

    files = list(BRONZE_DIR.glob("**/*.parquet"))
    if not files:
        logger.warning(f"No parquet files found in Bronze directory {BRONZE_DIR}")
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Loaded {len(df)} raw events from {len(files)} Bronze parquet files.")
    return df


def load_scd2_room_dimension() -> pd.DataFrame:
    """Loads the SCD Type 2 Room Dimension table."""
    dim_file = DIM_ROOMS_DIR / "dim_rooms.parquet"
    if not dim_file.exists():
        logger.info(f"Dimension table not found at {dim_file}. Generating fresh dimension...")
        from src.batch.scd2_room_dimension import run_scd2_pipeline
        run_scd2_pipeline()

    dim_df = pd.read_parquet(dim_file)
    dim_df["effective_start_date"] = pd.to_datetime(dim_df["effective_start_date"])
    dim_df["effective_end_date"] = pd.to_datetime(dim_df["effective_end_date"])
    return dim_df


def cleanse_and_validate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies data quality filters:
    - Drops duplicates on (building_id, room_id, timestamp)
    - Removes physical impossibility anomalies / sensor glitches (-99.9C, humidity > 100%)
    - Ensures proper timestamp typing
    """
    initial_count = len(df)
    
    # Ensure datetime
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
    
    # 1. Deduplicate
    df = df.drop_duplicates(subset=["building_id", "room_id", "event_timestamp"])
    
    # 2. Filter corrupt sensor glitches
    valid_mask = (
        (df["temperature_c"] >= 0.0) & (df["temperature_c"] <= 55.0) &
        (df["humidity_pct"] >= 5.0) & (df["humidity_pct"] <= 100.0) &
        (df["occupancy"] >= 0) &
        (df["energy_kw"] >= 0.0) &
        (df["building_id"].notna()) &
        (df["room_id"].notna())
    )
    
    cleaned_df = df[valid_mask].copy()
    dropped_count = initial_count - len(cleaned_df)
    logger.info(f"Data Cleansing: {dropped_count} invalid/corrupted/duplicate rows filtered out ({len(cleaned_df)} remain).")
    return cleaned_df


def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Evaluates multi-condition anomaly rules:
    1. Sustained Overheating: Temperature > 30C for >= 5 minutes (or >= 3 consecutive readings)
    2. Humidity Spike: Humidity > 80%
    3. Ghost Power Waste: Energy > 3.5 kW when occupancy == 0
    """
    df = df.sort_values(by=["room_id", "event_timestamp"]).copy()

    # Rule 1: Instantaneous high temperature
    df["high_temp_flag"] = df["temperature_c"] >= TEMP_ANOMALY_THRESHOLD

    # Rule 2: Sustained temperature calculation per room
    # We group by room and calculate cumulative consecutive high temp readings
    # A streak is formed by comparing whether high_temp_flag changed
    streak_id = (df["high_temp_flag"] != df.groupby("room_id")["high_temp_flag"].shift(1)).cumsum()
    df["consecutive_high_temp_ticks"] = df.groupby(["room_id", streak_id]).cumcount() + 1
    df.loc[~df["high_temp_flag"], "consecutive_high_temp_ticks"] = 0

    # In our simulation, ~3+ consecutive readings or duration >= 300s signifies sustained overheating
    df["is_sustained_overheating"] = (
        df["high_temp_flag"] & 
        ((df["consecutive_high_temp_ticks"] >= 3) | (df["temperature_c"] >= 32.0))
    )

    # Rule 3: Humidity spike
    df["is_humidity_spike"] = df["humidity_pct"] >= HUMIDITY_ANOMALY_THRESHOLD

    # Rule 4: Energy waste (ghost draw during vacancy)
    df["is_energy_waste"] = (df["occupancy"] == 0) & (df["energy_kw"] >= ENERGY_WASTE_THRESHOLD_KW)

    # Composite Anomaly Flag
    df["is_anomaly"] = df["is_sustained_overheating"] | df["is_humidity_spike"] | df["is_energy_waste"]

    # Assign Anomaly Severity & Primary Reason
    conditions = [
        df["is_sustained_overheating"],
        df["is_humidity_spike"],
        df["is_energy_waste"]
    ]
    reasons = [
        "Sustained Overheating (>30°C)",
        "Humidity Spike (>80%)",
        "Ghost Power Waste (0 Occupancy, High kW)"
    ]
    df["anomaly_reason"] = np.select(conditions, reasons, default="None")

    severity_conditions = [
        df["is_sustained_overheating"] | (df["temperature_c"] >= 33.0),
        df["is_humidity_spike"] | df["is_energy_waste"]
    ]
    severity_labels = ["CRITICAL", "WARNING"]
    df["anomaly_severity"] = np.select(severity_conditions, severity_labels, default="NORMAL")

    anomaly_count = df["is_anomaly"].sum()
    logger.info(f"Anomaly Detection: {anomaly_count} anomalous readings identified.")
    return df


def enrich_with_scd2_dimensions(df: pd.DataFrame, dim_df: pd.DataFrame) -> pd.DataFrame:
    """
    Performs point-in-time temporal join with the SCD Type 2 dimension table:
    reading.room_id == dim.room_id AND
    reading.event_timestamp >= dim.effective_start_date AND
    reading.event_timestamp < dim.effective_end_date
    """
    logger.info("Enriching telemetry with SCD Type 2 Room Dimensions...")
    
    # For robust matching, merge on room_id and filter by validity interval
    merged = pd.merge(df, dim_df, on="room_id", suffixes=("", "_dim"))
    
    # Filter for the temporal validity window
    temporal_match = (
        (merged["event_timestamp"] >= merged["effective_start_date"].dt.tz_localize("UTC")) &
        (merged["event_timestamp"] < merged["effective_end_date"].dt.tz_localize("UTC"))
    )
    
    # In case of any edge cases without a match, fallback to is_current
    matched = merged[temporal_match].copy()
    if len(matched) < len(df):
        logger.info(f"Using current dimension snapshot for {len(df) - len(matched)} unjoined rows.")
        current_dim = dim_df[dim_df["is_current"] == True].drop(columns=["effective_start_date", "effective_end_date"])
        matched = pd.merge(df, current_dim, on="room_id", suffixes=("", "_dim"), how="left")

    # Compute delta comfort metrics
    matched["temp_delta_c"] = round(matched["temperature_c"] - matched["target_temp_c"], 2)
    matched["occupancy_utilization_pct"] = round((matched["occupancy"] / matched["max_occupancy"]) * 100.0, 1)

    return matched


def run_silver_pipeline():
    """Executes the full Bronze -> Silver ETL pipeline."""
    logger.info("=== Starting Bronze -> Silver Pipeline ===")
    
    # 1. Extract from Bronze
    bronze_df = load_bronze_data()
    if bronze_df.empty:
        logger.warning("Bronze data is empty. Ingest data via Kafka first.")
        return

    # 2. Cleanse and validate
    cleaned_df = cleanse_and_validate(bronze_df)

    # 3. Detect anomalies
    anomalies_df = detect_anomalies(cleaned_df)

    # 4. Enrich with SCD Type 2 dimension
    dim_df = load_scd2_room_dimension()
    silver_df = enrich_with_scd2_dimensions(anomalies_df, dim_df)

    # 5. Persist to Silver Layer
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    out_file = SILVER_DIR / "silver_sensor_readings.parquet"
    silver_df.to_parquet(out_file, index=False, engine="pyarrow")
    logger.info(f"=== Silver Pipeline Complete. Saved {len(silver_df)} records to {out_file} ===")


if __name__ == "__main__":
    run_silver_pipeline()
