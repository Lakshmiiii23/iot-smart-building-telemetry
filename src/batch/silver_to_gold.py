"""
Gold Layer Aggregations: Business KPIs, Anomaly Reporting & Energy Efficiency
Aggregates conformed Silver telemetry into read-optimized analytical marts:
1. Hourly Building & Floor KPIs (temperatures, power, occupancy)
2. Anomaly Summary & Incident Audit Mart
3. Green Building Energy Efficiency & Waste Index
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import numpy as np

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from config.settings import (
    SILVER_DIR,
    GOLD_BUILDING_KPIS_DIR,
    GOLD_ANOMALY_SUMMARY_DIR,
    GOLD_ENERGY_EFFICIENCY_DIR
)
from src.utils.logger import get_logger

logger = get_logger("SilverToGold")


def load_silver_data() -> pd.DataFrame:
    """Loads cleaned, enriched Silver telemetry."""
    silver_file = SILVER_DIR / "silver_sensor_readings.parquet"
    if not silver_file.exists():
        logger.warning(f"Silver file not found at {silver_file}. Run Bronze-to-Silver ETL first.")
        return pd.DataFrame()

    df = pd.read_parquet(silver_file)
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
    logger.info(f"Loaded {len(df)} records from Silver layer.")
    return df


def compute_hourly_building_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """Computes hourly rollup aggregations by Building and Floor."""
    logger.info("Computing Hourly Building & Floor KPIs...")
    df = df.copy()
    
    # Create 1-hour tumbling window
    df["window_hour"] = df["event_timestamp"].dt.floor("1h")

    kpis = df.groupby(["window_hour", "building_id", "building_name", "floor"]).agg(
        avg_temp_c=("temperature_c", "mean"),
        min_temp_c=("temperature_c", "min"),
        max_temp_c=("temperature_c", "max"),
        avg_humidity_pct=("humidity_pct", "mean"),
        avg_occupancy=("occupancy", "mean"),
        peak_occupancy=("occupancy", "max"),
        total_energy_kwh=("energy_kw", lambda x: np.round(np.sum(x) * (2.0 / 3600.0), 4)), # ~2s sample intervals
        total_readings=("event_id", "count"),
        total_anomalies=("is_anomaly", "sum")
    ).reset_index()

    # Round metrics for presentation
    kpis["avg_temp_c"] = kpis["avg_temp_c"].round(2)
    kpis["avg_humidity_pct"] = kpis["avg_humidity_pct"].round(1)
    kpis["avg_occupancy"] = kpis["avg_occupancy"].round(1)

    return kpis


def compute_anomaly_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Generates an executive incident summary of detected anomalies."""
    logger.info("Computing Anomaly Summary Mart...")
    anomalies = df[df["is_anomaly"] == True].copy()
    if anomalies.empty:
        logger.info("No anomalies detected in Silver dataset.")
        return pd.DataFrame()

    summary = anomalies.groupby(
        ["building_id", "floor", "room_id", "room_type", "anomaly_reason", "anomaly_severity"]
    ).agg(
        incident_count=("event_id", "count"),
        first_detected=("event_timestamp", "min"),
        last_detected=("event_timestamp", "max"),
        peak_temperature_c=("temperature_c", "max"),
        max_power_spike_kw=("energy_kw", "max")
    ).reset_index()

    summary = summary.sort_values(by=["incident_count", "peak_temperature_c"], ascending=False)
    return summary


def compute_energy_efficiency_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes green building efficiency score:
    - Energy per occupant
    - Wasted energy during zero occupancy
    - Comfort compliance % (readings within +/- 1.5C of target setpoint)
    """
    logger.info("Computing Building Energy Efficiency & Waste Scores...")
    df = df.copy()

    # Identify wasted energy (kW during 0 occupancy)
    df["wasted_energy_kw"] = np.where(df["occupancy"] == 0, df["energy_kw"], 0.0)
    
    # Comfort compliance (+/- 1.5C of target temp)
    if "target_temp_c" in df.columns:
        df["in_comfort_band"] = (abs(df["temperature_c"] - df["target_temp_c"]) <= 1.5)
    else:
        df["in_comfort_band"] = (df["temperature_c"] >= 20.0) & (df["temperature_c"] <= 24.0)

    efficiency = df.groupby(["building_id", "building_name"]).agg(
        total_energy_kwh=("energy_kw", lambda x: np.round(np.sum(x) * (2.0 / 3600.0), 3)),
        wasted_energy_kwh=("wasted_energy_kw", lambda x: np.round(np.sum(x) * (2.0 / 3600.0), 3)),
        total_occupant_readings=("occupancy", "sum"),
        comfort_compliance_pct=("in_comfort_band", lambda x: np.round(np.mean(x) * 100.0, 1)),
        total_rooms_monitored=("room_id", "nunique")
    ).reset_index()

    # Calculate Green Efficiency Index (0 - 100)
    waste_ratio = efficiency["wasted_energy_kwh"] / np.maximum(0.001, efficiency["total_energy_kwh"])
    efficiency["waste_pct"] = np.round(waste_ratio * 100.0, 1)
    efficiency["green_score"] = np.round(np.clip(100.0 - (efficiency["waste_pct"] * 0.8) + (efficiency["comfort_compliance_pct"] * 0.2) - 20.0, 0, 100), 1)

    return efficiency


def run_gold_pipeline():
    """Executes the full Silver -> Gold aggregations pipeline."""
    logger.info("=== Starting Silver -> Gold Aggregations Pipeline ===")
    silver_df = load_silver_data()
    if silver_df.empty:
        logger.warning("Silver dataset is empty. Cannot compute Gold layer.")
        return

    # 1. Hourly Building KPIs
    GOLD_BUILDING_KPIS_DIR.mkdir(parents=True, exist_ok=True)
    kpis_df = compute_hourly_building_kpis(silver_df)
    kpis_file = GOLD_BUILDING_KPIS_DIR / "building_hourly_kpis.parquet"
    kpis_df.to_parquet(kpis_file, index=False, engine="pyarrow")
    logger.info(f"Saved {len(kpis_df)} hourly KPI records to {kpis_file}")

    # 2. Anomaly Summary Mart
    GOLD_ANOMALY_SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    anomaly_summary_df = compute_anomaly_summary(silver_df)
    if not anomaly_summary_df.empty:
        anomaly_file = GOLD_ANOMALY_SUMMARY_DIR / "anomaly_summary.parquet"
        anomaly_summary_df.to_parquet(anomaly_file, index=False, engine="pyarrow")
        logger.info(f"Saved {len(anomaly_summary_df)} anomaly incident records to {anomaly_file}")

    # 3. Energy Efficiency Mart
    GOLD_ENERGY_EFFICIENCY_DIR.mkdir(parents=True, exist_ok=True)
    efficiency_df = compute_energy_efficiency_scores(silver_df)
    eff_file = GOLD_ENERGY_EFFICIENCY_DIR / "energy_efficiency.parquet"
    efficiency_df.to_parquet(eff_file, index=False, engine="pyarrow")
    logger.info(f"Saved Energy Efficiency scorecard to {eff_file}")

    logger.info("=== Gold Layer Aggregations Complete ===")


if __name__ == "__main__":
    run_gold_pipeline()
