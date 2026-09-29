"""
Slowly Changing Dimension (SCD Type 2) Generator & Manager for Building/Room Metadata.
Tracks changes in room designations, HVAC equipment, target comfort bands, and capacity over time.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

# Add project root to path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from config.settings import DIM_ROOMS_DIR, BUILDINGS
from src.utils.logger import get_logger

logger = get_logger("SCD2RoomDimension")


def generate_initial_room_dimension() -> pd.DataFrame:
    """
    Builds the baseline seed records (Version 1) for all rooms across buildings.
    """
    records = []
    effective_start = "2026-01-01 00:00:00"
    effective_end = "9999-12-31 23:59:59"

    for b_id, b_meta in BUILDINGS.items():
        floors = b_meta["floors"]
        rooms_per_floor = b_meta["rooms_per_floor"]
        for floor_num in range(1, floors + 1):
            for room_idx in range(1, rooms_per_floor + 1):
                room_id = f"{b_id}-F{floor_num}-{floor_num:02d}{room_idx:02d}"
                
                # Determine initial configuration
                if room_idx == 1 and floor_num == 1:
                    rtype = "Cafeteria"
                    dept = "Facilities & Dining"
                    max_occ = 60
                    target_temp = 22.5
                    hvac = f"HVAC-{b_id}-F{floor_num}-COMM"
                    sqft = 2400
                elif room_idx == rooms_per_floor:
                    rtype = "Server Room"
                    dept = "IT Infrastructure"
                    max_occ = 3
                    target_temp = 19.0
                    hvac = f"HVAC-{b_id}-F{floor_num}-PRECISION"
                    sqft = 600
                elif room_idx % 2 == 0:
                    rtype = "Conference Room"
                    dept = "Shared Collaboration"
                    max_occ = 20
                    target_temp = 21.5
                    hvac = f"HVAC-{b_id}-F{floor_num}-UNIT1"
                    sqft = 800
                elif floor_num == floors and room_idx == rooms_per_floor - 1:
                    rtype = "Executive Suite"
                    dept = "Executive Leadership"
                    max_occ = 10
                    target_temp = 22.0
                    hvac = f"HVAC-{b_id}-F{floor_num}-EXEC"
                    sqft = 1200
                else:
                    rtype = "Open Office"
                    dept = "Engineering & Operations"
                    max_occ = 25
                    target_temp = 22.0
                    hvac = f"HVAC-{b_id}-F{floor_num}-UNIT2"
                    sqft = 1500

                records.append({
                    "room_sk": f"{room_id}_v1",
                    "room_id": room_id,
                    "building_id": b_id,
                    "building_name": b_meta["name"],
                    "floor": floor_num,
                    "room_type": rtype,
                    "department": dept,
                    "hvac_unit_id": hvac,
                    "target_temp_c": target_temp,
                    "target_humidity_pct": 50.0,
                    "max_occupancy": max_occ,
                    "area_sqft": sqft,
                    "version": 1,
                    "effective_start_date": effective_start,
                    "effective_end_date": effective_end,
                    "is_current": True
                })

    df = pd.DataFrame(records)
    return df


def simulate_scd2_room_evolution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulates real-world changes (e.g. Room A-F2-0202 converted to high-density Lab,
    HVAC unit upgraded in B-F1-0101) by expiring v1 and inserting v2.
    """
    logger.info("Simulating SCD Type 2 dimension historical change events...")
    df = df.copy()

    # Change Event 1: Upgrade Room BLDG-A-F2-0202 from Conference Room to AI Research Lab
    target_room = "BLDG-A-F2-0202"
    change_timestamp = "2026-06-01 00:00:00"

    mask = (df["room_id"] == target_room) & (df["is_current"] == True)
    if mask.any():
        # 1. Expire existing record
        df.loc[mask, "effective_end_date"] = change_timestamp
        df.loc[mask, "is_current"] = False

        # 2. Insert new Version 2 record
        old_rec = df.loc[mask].iloc[0].to_dict()
        new_rec = old_rec.copy()
        new_rec["room_sk"] = f"{target_room}_v2"
        new_rec["room_type"] = "AI Robotics Lab"
        new_rec["department"] = "Advanced R&D"
        new_rec["hvac_unit_id"] = "HVAC-BLDG-A-F2-HIGH-CAP"
        new_rec["target_temp_c"] = 20.0
        new_rec["max_occupancy"] = 15
        new_rec["version"] = 2
        new_rec["effective_start_date"] = change_timestamp
        new_rec["effective_end_date"] = "9999-12-31 23:59:59"
        new_rec["is_current"] = True

        df = pd.concat([df, pd.DataFrame([new_rec])], ignore_index=True)
        logger.info(f"SCD2 Transition: Room {target_room} evolved to v2 (AI Robotics Lab).")

    return df


def save_room_dimension(df: pd.DataFrame, output_dir: Path = DIM_ROOMS_DIR):
    """Saves the SCD Type 2 dimension table to Parquet."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / "dim_rooms.parquet"
    df.to_parquet(out_file, index=False, engine="pyarrow")
    logger.info(f"SCD Type 2 Room Dimension saved to {out_file} (Total records: {len(df)})")
    return out_file


def run_scd2_pipeline():
    """Initializes and saves the SCD Type 2 dimension table."""
    logger.info("Starting SCD Type 2 Room Dimension Pipeline...")
    df = generate_initial_room_dimension()
    df = simulate_scd2_room_evolution(df)
    save_room_dimension(df)
    logger.info("SCD Type 2 Dimension table ready for Silver Layer joins.")


if __name__ == "__main__":
    run_scd2_pipeline()
