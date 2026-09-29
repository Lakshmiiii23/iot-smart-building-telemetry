"""
Apache Airflow DAG: IoT Smart Building Telemetry Batch & Aggregation Orchestrator
Schedules and orchestrates the Medallion pipeline tasks:
1. Validate & update SCD Type 2 Room Dimensions
2. Ingest streaming Bronze micro-batches
3. Run Silver cleansing, anomaly detection, and dimension enrichment
4. Compute Gold building KPIs and Green Sustainability scorecards
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default DAG arguments
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# Define DAG (Runs hourly for batch rollups)
dag = DAG(
    'iot_smart_building_telemetry_pipeline',
    default_args=default_args,
    description='Orchestrates Bronze -> Silver -> Gold lakehouse transformations and anomaly detection',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['iot', 'smart_building', 'medallion', 'streaming', 'spark']
)

# Task 1: Check SCD Type 2 Room Dimension
t1_scd2_dimension = BashOperator(
    task_id='sync_scd2_room_dimension',
    bash_command='python /opt/airflow/src/batch/scd2_room_dimension.py',
    dag=dag,
)

# Task 2: Ingest Kafka messages to Bronze Layer
t2_bronze_ingestion = BashOperator(
    task_id='ingest_kafka_to_bronze',
    bash_command='python /opt/airflow/src/streaming/kafka_to_bronze.py --engine direct --batches 5',
    dag=dag,
)

# Task 3: Silver Layer Processing & Anomaly Detection
t3_silver_processing = BashOperator(
    task_id='process_bronze_to_silver',
    bash_command='python /opt/airflow/src/batch/bronze_to_silver.py',
    dag=dag,
)

# Task 4: Gold Layer Business Aggregations & Efficiency Scoring
t4_gold_aggregations = BashOperator(
    task_id='compute_silver_to_gold_kpis',
    bash_command='python /opt/airflow/src/batch/silver_to_gold.py',
    dag=dag,
)

# Pipeline Task Dependencies
t1_scd2_dimension >> t2_bronze_ingestion >> t3_silver_processing >> t4_gold_aggregations
