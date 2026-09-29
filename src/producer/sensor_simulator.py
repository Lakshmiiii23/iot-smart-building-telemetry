"""
IoT Smart Building Sensor Simulator & Kafka Producer
Generates high-fidelity streaming telemetry for commercial buildings:
- Temperature, Humidity, Occupancy, Energy (kW), CO2 (ppm)
- Temporal drift + occupancy load coupling
- Injectable anomalies (sustained overheating, humidity spikes, energy waste)
- Stream to Kafka topic 'iot-sensor-readings'
"""

import sys
import os
import json
import time
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional

# Ensure project root is in sys.path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_SENSORS,
    BUILDINGS,
    ROOM_TYPES,
    TEMP_ANOMALY_THRESHOLD,
    HUMIDITY_ANOMALY_THRESHOLD,
    ENERGY_WASTE_THRESHOLD_KW
)


class RoomSimulationState:
    """Maintains continuous physical state for a specific room to simulate realistic drift."""
    def __init__(self, building_id: str, floor: int, room_id: str, room_type: str):
        self.building_id = building_id
        self.floor = floor
        self.room_id = room_id
        self.room_type = room_type
        
        # Room-type specific baseline parameters
        if room_type == "Server Room":
            self.base_temp = 19.0
            self.base_humidity = 40.0
            self.max_occupancy = 3
            self.base_power_kw = 4.0
        elif room_type == "Cafeteria":
            self.base_temp = 22.5
            self.base_humidity = 55.0
            self.max_occupancy = 60
            self.base_power_kw = 5.0
        elif room_type == "Conference Room":
            self.base_temp = 21.5
            self.base_humidity = 48.0
            self.max_occupancy = 25
            self.base_power_kw = 1.2
        else: # Open Office & Executive
            self.base_temp = 22.0
            self.base_humidity = 45.0
            self.max_occupancy = 20
            self.base_power_kw = 1.5

        # Current live state
        self.current_temp = self.base_temp + random.uniform(-0.5, 0.5)
        self.current_humidity = self.base_humidity + random.uniform(-2.0, 2.0)
        self.current_occupancy = random.randint(0, self.max_occupancy // 2)
        
        # Active anomaly state machine
        self.active_anomaly: Optional[str] = None
        self.anomaly_ticks_remaining: int = 0

    def tick(self) -> Dict[str, Any]:
        """Step the room forward by one time interval and emit a reading."""
        now = datetime.now(timezone.utc)
        hour = now.hour

        # Check if an anomaly should be triggered (1.5% chance per tick if not already anomalous)
        if not self.active_anomaly and random.random() < 0.015:
            self.active_anomaly = random.choice([
                "HVAC_OVERHEATING",   # Sustained temperature > 30°C
                "HUMIDITY_SPIKE",     # Humidity > 80%
                "GHOST_POWER_WASTE",  # High energy with 0 occupancy
                "SENSOR_GLITCH"       # Corrupted / outlier reading
            ])
            # Each tick is ~2-3 seconds, so 60-100 ticks simulates several minutes of sustained anomaly
            self.anomaly_ticks_remaining = random.randint(30, 80)

        # 1. Occupancy Dynamics (higher during business hours 8am - 6pm)
        is_business_hours = 8 <= hour <= 18
        if is_business_hours:
            if self.room_type == "Cafeteria" and 11 <= hour <= 14:
                target_occ = random.randint(self.max_occupancy // 2, self.max_occupancy)
            else:
                target_occ = random.randint(1, self.max_occupancy)
        else:
            target_occ = random.randint(0, 2) if self.room_type != "Server Room" else 0
            
        self.current_occupancy = int(0.7 * self.current_occupancy + 0.3 * target_occ)

        # 2. Temperature Dynamics (thermal inertia + human heat load ~0.05C per person)
        human_thermal_load = self.current_occupancy * 0.04
        normal_target_temp = self.base_temp + human_thermal_load + random.uniform(-0.3, 0.3)
        self.current_temp = round(0.85 * self.current_temp + 0.15 * normal_target_temp, 2)

        # 3. Humidity Dynamics
        normal_target_hum = self.base_humidity + (self.current_occupancy * 0.2) + random.uniform(-0.5, 0.5)
        self.current_humidity = round(0.9 * self.current_humidity + 0.1 * normal_target_hum, 1)

        # 4. Energy Consumption (Baseline + HVAC cooling + equipment per occupant)
        cooling_penalty = max(0.0, (self.current_temp - 21.0) * 0.4)
        active_power = self.base_power_kw + (self.current_occupancy * 0.12) + cooling_penalty + random.uniform(-0.1, 0.1)
        energy_kw = round(max(0.1, active_power), 2)

        # 5. CO2 Dynamics (ambient ~420 ppm, rises with occupancy)
        co2_ppm = round(410.0 + (self.current_occupancy * 32.0) + random.uniform(-15.0, 15.0), 1)

        # Apply Injected Anomalies if active
        anomaly_label = None
        if self.active_anomaly:
            if self.active_anomaly == "HVAC_OVERHEATING":
                # Gradual ramp up past 30°C to 34°C
                self.current_temp = round(min(35.5, max(30.8, self.current_temp + 0.3)), 2)
                anomaly_label = "HVAC_OVERHEATING"
            elif self.active_anomaly == "HUMIDITY_SPIKE":
                # Spike past 80% to 92%
                self.current_humidity = round(min(98.0, max(82.5, self.current_humidity + 2.5)), 1)
                anomaly_label = "HUMIDITY_SPIKE"
            elif self.active_anomaly == "GHOST_POWER_WASTE":
                # Room empty but power is 4.5 - 7.0 kW
                self.current_occupancy = 0
                energy_kw = round(random.uniform(ENERGY_WASTE_THRESHOLD_KW + 1.0, 7.5), 2)
                anomaly_label = "GHOST_POWER_WASTE"
            elif self.active_anomaly == "SENSOR_GLITCH":
                # Corrupted / invalid readings for testing data cleansing
                self.current_temp = -99.9
                anomaly_label = "SENSOR_GLITCH"

            self.anomaly_ticks_remaining -= 1
            if self.anomaly_ticks_remaining <= 0:
                self.active_anomaly = None  # Anomaly resolved / HVAC restored

        return {
            "event_id": str(uuid.uuid4()),
            "timestamp": now.isoformat(),
            "building_id": self.building_id,
            "floor": self.floor,
            "room_id": self.room_id,
            "room_type": self.room_type,
            "temperature_c": self.current_temp,
            "humidity_pct": self.current_humidity,
            "occupancy": self.current_occupancy,
            "energy_kw": energy_kw,
            "co2_ppm": co2_ppm,
            "simulated_anomaly": anomaly_label  # For debugging & ground-truth validation
        }


class IoTSensorSimulator:
    """Manages fleet of smart building rooms and publishes telemetry to Kafka."""
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS, topic: str = KAFKA_TOPIC_SENSORS):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.rooms: List[RoomSimulationState] = []
        self._initialize_fleet()
        self.producer = None

    def _initialize_fleet(self):
        """Build the virtual real estate layout across all buildings and floors."""
        for b_id, b_meta in BUILDINGS.items():
            floors = b_meta["floors"]
            rooms_per_floor = b_meta["rooms_per_floor"]
            for floor_num in range(1, floors + 1):
                for room_idx in range(1, rooms_per_floor + 1):
                    # Deterministically distribute room types
                    if room_idx == 1 and floor_num == 1:
                        rtype = "Cafeteria"
                    elif room_idx == rooms_per_floor:
                        rtype = "Server Room"
                    elif room_idx % 2 == 0:
                        rtype = "Conference Room"
                    elif floor_num == floors and room_idx == rooms_per_floor - 1:
                        rtype = "Executive Suite"
                    else:
                        rtype = "Open Office"
                        
                    room_id = f"{b_id}-F{floor_num}-{floor_num:02d}{room_idx:02d}"
                    self.rooms.append(RoomSimulationState(b_id, floor_num, room_id, rtype))
                    
        print(f"🏢 Fleet initialized: {len(self.rooms)} rooms across {len(BUILDINGS)} buildings.")

    def connect_kafka(self):
        """Establish Kafka producer connection with retries."""
        try:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",            # Full ISR durability
                retries=3,
                linger_ms=50           # Small batching for throughput
            )
            print(f" Connected to Kafka cluster at {self.bootstrap_servers}")
        except Exception as e:
            print(f"⚠️ Kafka connection warning: {e}. Simulator can run in dry-run mode.")
            self.producer = None

    def run(self, interval_seconds: float = 2.0, max_ticks: Optional[int] = None, dry_run: bool = False):
        """Continuously simulate sensor ticks and publish events."""
        if not dry_run and self.producer is None:
            self.connect_kafka()

        print(f"🚀 Starting IoT Telemetry Stream (Interval: {interval_seconds}s, Total Rooms: {len(self.rooms)})")
        print("Press Ctrl+C to stop.\n")

        ticks = 0
        total_events_sent = 0
        active_anomalies_count = 0

        try:
            while True:
                batch_start = time.time()
                active_anomalies_count = 0
                
                # Emit readings for a subset or all rooms per tick
                for room in self.rooms:
                    reading = room.tick()
                    if reading["simulated_anomaly"]:
                        active_anomalies_count += 1

                    if self.producer and not dry_run:
                        # Key messages by building_id to ensure per-building partition ordering
                        self.producer.send(
                            self.topic,
                            key=reading["building_id"],
                            value=reading
                        )
                    total_events_sent += 1

                if self.producer and not dry_run:
                    self.producer.flush()

                ticks += 1
                elapsed = time.time() - batch_start
                status = f"Tick {ticks:04d} | Events: {total_events_sent:06d} | Active Anomalies: {active_anomalies_count:02d} | Cycle: {elapsed:.2f}s"
                print(status, end="\r")

                if max_ticks and ticks >= max_ticks:
                    break

                sleep_time = max(0.0, interval_seconds - elapsed)
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            print("\n🛑 Simulation interrupted by user.")
        finally:
            if self.producer:
                self.producer.close()
                print("🔌 Kafka producer connection closed cleanly.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="IoT Smart Building Telemetry Generator")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between tick cycles")
    parser.add_argument("--dry-run", action="store_true", help="Print stats without sending to Kafka")
    parser.add_argument("--ticks", type=int, default=None, help="Max ticks to run before exiting")
    args = parser.parse_args()

    simulator = IoTSensorSimulator()
    simulator.run(interval_seconds=args.interval, max_ticks=args.ticks, dry_run=args.dry_run)
