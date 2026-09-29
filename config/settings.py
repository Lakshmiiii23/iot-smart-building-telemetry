import os
from pathlib import Path

# Base Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CHECKPOINTS_DIR = BASE_DIR / "checkpoints"

# Medallion Lakehouse Paths
BRONZE_DIR = DATA_DIR / "bronze" / "sensor_readings"
SILVER_DIR = DATA_DIR / "silver" / "sensor_readings"
GOLD_BUILDING_KPIS_DIR = DATA_DIR / "gold" / "building_hourly_kpis"
GOLD_ANOMALY_SUMMARY_DIR = DATA_DIR / "gold" / "anomaly_summary"
GOLD_ENERGY_EFFICIENCY_DIR = DATA_DIR / "gold" / "energy_efficiency"
DIM_ROOMS_DIR = DATA_DIR / "dimensions" / "dim_rooms"

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_SENSORS = os.getenv("KAFKA_TOPIC_SENSORS", "iot-sensor-readings")
KAFKA_TOPIC_ANOMALIES = os.getenv("KAFKA_TOPIC_ANOMALIES", "iot-sensor-alerts")
KAFKA_CONSUMER_GROUP_BRONZE = "iot-bronze-consumer-group"

# Simulation Settings
BUILDINGS = {
    "BLDG-A": {"name": "Tech Tower Alpha", "floors": 4, "rooms_per_floor": 6},
    "BLDG-B": {"name": "Innovation Hub Beta", "floors": 3, "rooms_per_floor": 5},
    "BLDG-C": {"name": "Research Wing Gamma", "floors": 3, "rooms_per_floor": 4}
}

ROOM_TYPES = ["Open Office", "Conference Room", "Server Room", "Cafeteria", "Executive Suite"]

# Anomaly Thresholds
TEMP_ANOMALY_THRESHOLD = 30.0    # °C
TEMP_SUSTAINED_MINUTES = 5       # Readings above 30°C for >= 5 minutes
HUMIDITY_ANOMALY_THRESHOLD = 80.0 # %
ENERGY_WASTE_THRESHOLD_KW = 3.5  # kW when room is unoccupied (occupancy == 0)
CO2_ALERT_THRESHOLD = 1200.0     # ppm

# Streaming Micro-Batch Settings
STREAMING_TRIGGER_SECONDS = "5 seconds"
