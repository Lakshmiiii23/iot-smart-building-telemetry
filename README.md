# 📡 IoT Smart Building Telemetry Pipeline

> **An End-to-End Real-Time Streaming & Batch Lakehouse Pipeline using Apache Kafka, PySpark Structured Streaming, Medallion Architecture (Bronze/Silver/Gold), SCD Type 2 Dimension Modeling, and Streamlit.**

---

## 🏗️ 1. Architecture Overview

```
┌─────────────────────────┐
│  IoT Sensor Simulator   │  (Simulates 60+ rooms across 3 buildings with realistic
│ (Python Multi-Sensors)  │   thermal inertia, occupancy drift, and injected anomalies)
└────────────┬────────────┘
             │  JSON telemetry events every 2-3s (Keyed by building_id)
             ▼
┌─────────────────────────┐
│   Apache Kafka Broker   │  Topic: `iot-sensor-readings` (3 Partitions)
│  (Dockerized Port 9092) │  Topic: `iot-sensor-alerts`
└────────────┬────────────┘
             │
             ├──────────────────────────────────────────────────────┐
             ▼ (Streaming Ingestion)                                ▼ (Kafka UI Monitoring)
┌─────────────────────────────────┐                    ┌───────────────────────────────┐
│   PySpark Structured Streaming  │                    │ Kafka UI (Port 8085)          │
│   (Continuous micro-batch WAL)  │                    │ http://localhost:8085         │
└────────────────┬────────────────┘                    └───────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│    🥉 Bronze Layer (Raw Lake)   │  Append-only Parquet storage preserving Kafka metadata
│    `data/bronze/`               │  (topic, partition, offset, ingestion_timestamp)
└────────────────┬────────────────┘
                 │
                 ▼ (Data Validation + Anomaly Engine + SCD2 Join)
┌─────────────────────────────────┐     ┌──────────────────────────────────────────────┐
│    🥈 Silver Layer (Cleaned)    │◀────│ 📋 SCD Type 2 Room Dimension (`dim_rooms`)    │
│    `data/silver/`               │     │ Tracks historical room changes & HVAC units  │
└────────────────┬────────────────┘     └──────────────────────────────────────────────┘
                 │
                 ▼ (Business Aggregations & Efficiency Scoring)
┌─────────────────────────────────┐
│    🥇 Gold Layer (Curated KPIs) │  Hourly building rollups, Anomaly incident audit mart,
│    `data/gold/`                 │  and Green Building Sustainability scorecard
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│  Streamlit Real-Time Dashboard  │  Live floor heatmaps, real-time anomaly alerts feed,
│  (Port 8501)                    │  and energy waste vs occupancy scatter analysis
└─────────────────────────────────┘
```

---

## 🎯 2. Why This Project is a Game-Changer for Your Resume

1. **Streaming + Batch Hybrid (Kappa/Lambda Pattern):** Combines continuous Kafka event streaming with structured lakehouse batch processing.
2. **Medallion Lakehouse Architecture:** Follows the Databricks/Snowflake industry-standard Bronze $\to$ Silver $\to$ Gold pattern.
3. **Slowly Changing Dimensions (SCD Type 2):** Solves the real-world data engineering challenge of tracking evolving metadata (e.g. room repurposing, equipment upgrades) while maintaining historical point-in-time accuracy.
4. **Time-Series Anomaly Detection:** Flags real-time equipment failures (sustained overheating $> 30^\circ\text{C}$ for $> 5$ min, humidity spikes $> 80\%$, and ghost power waste during zero occupancy).
5. **Zero External API Cost / 100% Reproducible:** Self-contained simulation and Dockerized services.

---

## 📂 3. Repository Structure

```
iot-smart-building-telemetry/
│
├── config/
│   └── settings.py               # Central configuration (Kafka, lakehouse paths, thresholds)
│
├── docker/
│   └── docker-compose.yml        # Kafka, Zookeeper, and Kafka UI container definitions
│
├── src/
│   ├── producer/
│   │   └── sensor_simulator.py   # IoT sensor telemetry generator with anomaly injection
│   │
│   ├── streaming/
│   │   └── kafka_to_bronze.py    # PySpark Structured Streaming & Direct Kafka-to-Bronze consumer
│   │
│   ├── batch/
│   │   ├── scd2_room_dimension.py# SCD Type 2 dimension generator & historical evolution engine
│   │   ├── bronze_to_silver.py   # Data cleansing, multi-rule anomaly detection, and SCD2 join
│   │   └── silver_to_gold.py     # Hourly KPI rollups, anomaly marts, and green efficiency scores
│   │
│   ├── dashboard/
│   │   └── app.py                # Streamlit real-time operations dashboard
│   │
│   └── utils/
│       ├── logger.py             # Standardized logging utility
│       └── spark_utils.py        # Optimized SparkSession builder with Kafka connectors
│
├── tests/
│   └── test_pipeline.py          # Unit tests for simulation, SCD2, cleansing, and KPIs
│
├── run_pipeline.py               # Unified CLI orchestrator for demo and individual phases
├── requirements.txt              # Project dependencies
└── README.md                     # Documentation and learning guide
```

---

## 🚀 4. Step-by-Step Execution Guide

### Step 1: Start Kafka & Zookeeper Services
Kafka and Zookeeper run in Docker:
```bash
docker compose -f docker/docker-compose.yml up -d
```
Verify running containers:
- **Kafka Broker:** `localhost:9092`
- **Kafka Web UI:** `http://localhost:8085` (Explore topics, messages, and consumer lag visually)

### Step 2: Seed the SCD Type 2 Room Dimension Table
Creates the initial room dimension baseline (v1) and simulates historical room repurposing (v2):
```bash
python run_pipeline.py --seed-dim
```

### Step 3: Produce Simulated IoT Sensor Telemetry
Publishes continuous sensor readings (temperature, humidity, occupancy, power kW, CO2) to Kafka topic `iot-sensor-readings`:
```bash
python run_pipeline.py --produce --ticks 30
```

### Step 4: Ingest Kafka Stream to Bronze Layer
Consumes from Kafka and writes raw append-only Parquet files into `data/bronze/`:
```bash
python run_pipeline.py --bronze
```

### Step 5: Cleanse, Detect Anomalies, and Enrich in Silver Layer
Cleanses corrupt readings, flags sustained overheating (>30°C for >5min), humidity spikes (>80%), and ghost energy waste, then joins with SCD Type 2 metadata:
```bash
python run_pipeline.py --silver
```

### Step 6: Compute Business KPIs in Gold Layer
Calculates hourly rollups, anomaly frequency summaries, and green building efficiency metrics into `data/gold/`:
```bash
python run_pipeline.py --gold
```

### Step 7: Launch the Real-Time Streamlit Operations Dashboard
```bash
python run_pipeline.py --dashboard
```
Open **`http://localhost:8501`** in your browser to view:
- 📊 Executive KPI Cards (Fleet temp, power draw, active critical alerts)
- 🗺️ Interactive Floor Plan Temperature Heatmap
- ⚡ Energy Waste vs Occupancy Scatter Plot (detects phantom power usage)
- 🚨 Real-Time Anomaly & Incident Log
- 🏛️ Lakehouse Lineage & SCD Type 2 Inspector

---

## ⚡ Automated 1-Click End-to-End Demo
To execute all stages in sequence:
```bash
python run_pipeline.py --demo --ticks 25
```

---

## 🧠 5. Key Data Engineering Concepts Explained

### A. Why Kafka Instead of a Direct Database Insert?
If 1,000 IoT devices write directly to a relational database every 2 seconds, database connection pools get exhausted, disk IO locks up, and peak traffic can crash the DB. 
Kafka acts as a **distributed buffer and shock absorber**:
- **Decoupling:** Sensors push to Kafka without knowing or caring how fast Spark processes them.
- **Partitioning:** Partitioning by `building_id` distributes the load across brokers and guarantees that events for any single building arrive in strict chronological order.
- **Backpressure Handling:** Spark consumers pull at their own rate without overwhelming downstream storage.

### B. Medallion Architecture (Bronze $\to$ Silver $\to$ Gold)
- **Bronze (Raw):** Ingests Kafka payloads as-is, preserving raw JSON, Kafka offset, partition, and arrival timestamp. Never discard raw data!
- **Silver (Cleaned & Enriched):** Removes sensor glitches (e.g. $-99.9^\circ\text{C}$ or null values), enforces schema, flags domain-specific anomalies, and enriches records with dimensional business context.
- **Gold (Curated Aggregations):** Aggregates data into analytical rollups optimized for dashboards and BI tools (e.g., hourly averages, efficiency indexes).

### C. Slowly Changing Dimensions (SCD Type 2)
In smart buildings, rooms get repurposed (e.g., a conference room upgraded to an AI server lab with dedicated cooling).
- **SCD Type 1:** Overwrites old metadata. *Problem:* Historical readings from 6 months ago would falsely join to the new server room profile, corrupting historical power/thermal audits.
- **SCD Type 2:** Keeps both records! Version 1 is marked with an expiration date (`effective_end_date = '2026-06-01'`) and `is_current = False`. Version 2 receives `effective_start_date = '2026-06-01'` and `is_current = True`. A point-in-time join ensures telemetry always reflects the room's true historical reality.

---

## 💬 6. Resume & Interview Talking Points

**Project Description to Put on Your Resume:**
> **IoT Smart Building Telemetry Pipeline | Kafka, PySpark, Lakehouse, SCD Type 2, Streamlit**
> - Architected an end-to-end streaming and batch Medallion pipeline ingesting 100k+ simulated IoT sensor events across 60+ commercial building zones using Apache Kafka and PySpark Structured Streaming.
> - Engineered an anomaly detection engine identifying sustained HVAC overheating (>30°C for >5min), humidity spikes, and ghost energy consumption during vacant hours.
> - Implemented an SCD Type 2 dimension model to preserve historical room and HVAC equipment configurations across time-series joins.
> - Built curated Gold analytical marts calculating green building efficiency scorecards and hourly facility KPI rollups.
> - Developed an interactive Streamlit operations dashboard with real-time floor heatmaps, live alert feeds, and energy waste scatter analytics.

**Common Interview Questions & How to Answer:**
1. *Q: How do you handle duplicate messages from Kafka?*
   - *A:* In the Bronze layer, we append all incoming records with their Kafka offset and partition for auditing. In the Silver layer, we execute deduplication over `(building_id, room_id, event_timestamp)` or `event_id`, ensuring downstream analytics are strictly idempotent.
2. *Q: How does Spark Structured Streaming ensure fault tolerance?*
   - *A:* Structured Streaming uses write-ahead checkpointing (`checkpointLocation`) and state stores. When a worker fails or restarts, it reads the last committed offset from the checkpoint log and resumes processing with at-least-once / effectively exactly-once guarantees.
