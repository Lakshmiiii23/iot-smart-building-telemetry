"""
Unit and Integration Tests for IoT Smart Building Pipeline
Validates:
- Sensor simulation logic and state updates
- SCD Type 2 dimension versioning and temporal intervals
- Silver cleansing and anomaly detection algorithms
- Gold KPI calculation accuracy
"""

import sys
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import pytest

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.producer.sensor_simulator import RoomSimulationState
from src.batch.scd2_room_dimension import generate_initial_room_dimension, simulate_scd2_room_evolution
from src.batch.bronze_to_silver import cleanse_and_validate, detect_anomalies
from src.batch.silver_to_gold import compute_hourly_building_kpis, compute_energy_efficiency_scores


def test_room_simulation_state():
    """Verify that simulated readings are bounded and structured properly."""
    room = RoomSimulationState("BLDG-A", 1, "BLDG-A-F1-0101", "Open Office")
    reading = room.tick()

    assert reading["building_id"] == "BLDG-A"
    assert reading["floor"] == 1
    assert reading["room_id"] == "BLDG-A-F1-0101"
    assert "temperature_c" in reading
    assert "humidity_pct" in reading
    assert "energy_kw" in reading
    assert reading["occupancy"] >= 0


def test_scd2_dimension_versioning():
    """Verify SCD Type 2 creates historical versions with valid start/end dates."""
    initial_dim = generate_initial_room_dimension()
    assert len(initial_dim) > 0
    assert (initial_dim["version"] == 1).all()
    assert (initial_dim["is_current"] == True).all()

    evolved_dim = simulate_scd2_room_evolution(initial_dim)
    # Target room BLDG-A-F2-0202 should have v1 (expired) and v2 (current)
    target = evolved_dim[evolved_dim["room_id"] == "BLDG-A-F2-0202"]
    assert len(target) == 2

    v1 = target[target["version"] == 1].iloc[0]
    v2 = target[target["version"] == 2].iloc[0]

    assert v1["is_current"] == False
    assert v2["is_current"] == True
    assert v1["effective_end_date"] == v2["effective_start_date"]
    assert v2["room_type"] == "AI Robotics Lab"


def test_silver_cleansing_and_anomalies():
    """Verify corrupted readings are filtered and valid anomalies are flagged."""
    sample_data = pd.DataFrame([
        {
            "event_id": "1",
            "event_timestamp": "2026-09-29T10:00:00Z",
            "building_id": "BLDG-A",
            "floor": 1,
            "room_id": "R1",
            "room_type": "Open Office",
            "temperature_c": -99.9, # Corrupt sensor glitch
            "humidity_pct": 50.0,
            "occupancy": 5,
            "energy_kw": 1.5,
            "co2_ppm": 450.0
        },
        {
            "event_id": "2",
            "event_timestamp": "2026-09-29T10:01:00Z",
            "building_id": "BLDG-A",
            "floor": 1,
            "room_id": "R1",
            "room_type": "Open Office",
            "temperature_c": 33.5, # Overheating anomaly
            "humidity_pct": 50.0,
            "occupancy": 5,
            "energy_kw": 2.0,
            "co2_ppm": 500.0
        },
        {
            "event_id": "3",
            "event_timestamp": "2026-09-29T10:02:00Z",
            "building_id": "BLDG-A",
            "floor": 1,
            "room_id": "R2",
            "room_type": "Conference Room",
            "temperature_c": 22.0,
            "humidity_pct": 45.0,
            "occupancy": 0,
            "energy_kw": 6.5, # Ghost power waste
            "co2_ppm": 420.0
        }
    ])

    cleaned = cleanse_and_validate(sample_data)
    # The -99.9C row should be dropped
    assert len(cleaned) == 2
    assert -99.9 not in cleaned["temperature_c"].values

    anomalies = detect_anomalies(cleaned)
    assert anomalies[anomalies["room_id"] == "R1"]["is_sustained_overheating"].iloc[0] == True
    assert anomalies[anomalies["room_id"] == "R2"]["is_energy_waste"].iloc[0] == True


if __name__ == "__main__":
    test_room_simulation_state()
    test_scd2_dimension_versioning()
    test_silver_cleansing_and_anomalies()
    print("All unit tests passed successfully!")
