"""
IoT Smart Building Real-Time Operations & Anomaly Detection Dashboard
Built with Streamlit, Plotly, and DuckDB.
Features:
- Real-time KPI Metric Cards
- Interactive Floor Plan Heatmap (Temperature & Occupancy)
- Active Anomaly Alert Table & Severity Badges
- Energy Efficiency vs Occupancy Correlation
- Lakehouse Medallion Data Lineage & SCD Type 2 Dimension Explorer
"""

import sys
import os
import time
from pathlib import Path
from datetime import datetime, timezone
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from config.settings import (
    BRONZE_DIR,
    SILVER_DIR,
    GOLD_BUILDING_KPIS_DIR,
    GOLD_ANOMALY_SUMMARY_DIR,
    GOLD_ENERGY_EFFICIENCY_DIR,
    DIM_ROOMS_DIR,
    TEMP_ANOMALY_THRESHOLD,
    HUMIDITY_ANOMALY_THRESHOLD,
    ENERGY_WASTE_THRESHOLD_KW
)

# Page configuration
st.set_page_config(
    page_title="IoT Smart Building Telemetry",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling
st.markdown("""
<style>
    .metric-card {
        background-color: #1E232A;
        border-radius: 8px;
        padding: 15px;
        border: 1px solid #2D3748;
    }
    .badge-critical {
        background-color: #E53E3E;
        color: white;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    .badge-warning {
        background-color: #DD6B20;
        color: white;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    .badge-normal {
        background-color: #38A169;
        color: white;
        padding: 4px 8px;
        border-radius: 4px;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=5)
def load_data():
    """Loads datasets across Silver, Gold, and Dimension layers."""
    silver_df = pd.DataFrame()
    gold_kpis = pd.DataFrame()
    gold_anomalies = pd.DataFrame()
    gold_efficiency = pd.DataFrame()
    dim_rooms = pd.DataFrame()

    silver_file = SILVER_DIR / "silver_sensor_readings.parquet"
    if silver_file.exists():
        silver_df = pd.read_parquet(silver_file)
        if not silver_df.empty:
            silver_df["event_timestamp"] = pd.to_datetime(silver_df["event_timestamp"])

    kpis_file = GOLD_BUILDING_KPIS_DIR / "building_hourly_kpis.parquet"
    if kpis_file.exists():
        gold_kpis = pd.read_parquet(kpis_file)

    anom_file = GOLD_ANOMALY_SUMMARY_DIR / "anomaly_summary.parquet"
    if anom_file.exists():
        gold_anomalies = pd.read_parquet(anom_file)

    eff_file = GOLD_ENERGY_EFFICIENCY_DIR / "energy_efficiency.parquet"
    if eff_file.exists():
        gold_efficiency = pd.read_parquet(eff_file)

    dim_file = DIM_ROOMS_DIR / "dim_rooms.parquet"
    if dim_file.exists():
        dim_rooms = pd.read_parquet(dim_file)

    return silver_df, gold_kpis, gold_anomalies, gold_efficiency, dim_rooms


def main():
    st.title("📡 Smart Building IoT Telemetry & Anomaly Detection")
    st.caption("Real-Time Streaming Lakehouse Pipeline (Kafka → PySpark → Delta/Parquet → Gold KPIs)")

    silver_df, gold_kpis, gold_anomalies, gold_efficiency, dim_rooms = load_data()

    # Sidebar Controls
    with st.sidebar:
        st.header("🎛️ Pipeline Controls")
        auto_refresh = st.checkbox("Auto-Refresh (every 5s)", value=False)
        if auto_refresh:
            time.sleep(5)
            st.rerun()

        st.subheader("Filter by Building")
        if not silver_df.empty and "building_id" in silver_df.columns:
            buildings = ["All"] + sorted(list(silver_df["building_id"].unique()))
        else:
            buildings = ["All", "BLDG-A", "BLDG-B", "BLDG-C"]
        selected_bldg = st.selectbox("Select Facility", buildings)

        st.markdown("---")
        st.markdown("### ⚙️ Anomaly Thresholds")
        st.write(f"- **Overheating:** > {TEMP_ANOMALY_THRESHOLD}°C (Sustained >5m)")
        st.write(f"- **Humidity:** > {HUMIDITY_ANOMALY_THRESHOLD}%")
        st.write(f"- **Energy Waste:** > {ENERGY_WASTE_THRESHOLD_KW} kW (at 0 occ)")
        st.markdown("---")
        st.info("💡 **Medallion Architecture:** Raw telemetry is ingested append-only into Bronze, validated & enriched in Silver, and rolled up into Gold analytical marts.")

    # Filter silver data
    df = silver_df.copy()
    if selected_bldg != "All" and not df.empty:
        df = df[df["building_id"] == selected_bldg]

    if df.empty:
        st.warning("⚠️ No telemetry data found in Silver layer yet.")
        st.markdown("""
        **To start the pipeline and generate data:**
        1. Run the Sensor Producer: `python src/producer/sensor_simulator.py`
        2. Run Ingestion to Bronze: `python src/streaming/kafka_to_bronze.py`
        3. Run Silver ETL: `python src/batch/bronze_to_silver.py`
        4. Run Gold ETL: `python src/batch/silver_to_gold.py`
        """)
        return

    # 1. Top-Level Metric Cards
    latest_readings = df.sort_values("event_timestamp").groupby("room_id").last().reset_index()
    active_critical = latest_readings[latest_readings["anomaly_severity"] == "CRITICAL"]
    active_warning = latest_readings[latest_readings["anomaly_severity"] == "WARNING"]

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("Total Monitored Rooms", f"{latest_readings['room_id'].nunique()}")
    with c2:
        avg_temp = latest_readings["temperature_c"].mean()
        st.metric("Fleet Avg Temp", f"{avg_temp:.1f} °C", delta=f"{avg_temp - 22.0:.1f} °C vs Setpoint")
    with c3:
        total_power = latest_readings["energy_kw"].sum()
        st.metric("Total Power Draw", f"{total_power:.1f} kW")
    with c4:
        total_occ = latest_readings["occupancy"].sum()
        st.metric("Total Occupants", f"{total_occ} people")
    with c5:
        crit_count = len(active_critical)
        st.metric("🚨 Critical Alerts", f"{crit_count}", delta=f"{len(active_warning)} Warnings", delta_color="inverse")

    st.markdown("---")

    # 2. Main Dashboard Tabs
    tab_overview, tab_heatmap, tab_anomalies, tab_efficiency, tab_lineage = st.tabs([
        "📊 Facility Overview",
        "🗺️ Floor Heatmap",
        "🚨 Anomaly Center",
        "🌱 Energy Efficiency",
        "🏛️ Lakehouse Lineage & SCD2"
    ])

    with tab_overview:
        col_left, col_right = st.columns([3, 2])
        
        with col_left:
            st.subheader("⚡ Power Demand vs Occupancy (Energy Waste Detector)")
            fig_scatter = px.scatter(
                latest_readings,
                x="occupancy",
                y="energy_kw",
                color="anomaly_severity",
                size="temperature_c",
                hover_data=["room_id", "room_type", "temperature_c", "anomaly_reason"],
                color_discrete_map={"NORMAL": "#3182CE", "WARNING": "#DD6B20", "CRITICAL": "#E53E3E"},
                title="Current Room Power Consumption vs Occupancy"
            )
            # Add waste threshold line
            fig_scatter.add_hline(y=ENERGY_WASTE_THRESHOLD_KW, line_dash="dash", line_color="orange",
                                  annotation_text=f"Ghost Waste Threshold ({ENERGY_WASTE_THRESHOLD_KW} kW)")
            st.plotly_chart(fig_scatter, use_container_width=True)

        with col_right:
            st.subheader("🏢 Room Type Breakdown")
            room_type_counts = latest_readings["room_type"].value_counts().reset_index()
            room_type_counts.columns = ["Room Type", "Count"]
            fig_pie = px.pie(room_type_counts, names="Room Type", values="Count", hole=0.4)
            st.plotly_chart(fig_pie, use_container_width=True)

    with tab_heatmap:
        st.subheader("🗺️ Live Floor Plan Temperature Heatmap")
        st.write("Visual inspection of current temperatures across floors and room positions. Rooms > 30°C highlighted.")
        
        # Prepare heatmap grid
        pivot_temp = latest_readings.pivot_table(
            index="floor",
            columns="room_id",
            values="temperature_c",
            aggfunc="mean"
        )
        
        fig_heat = px.imshow(
            pivot_temp,
            labels=dict(x="Room ID", y="Floor Level", color="Temp (°C)"),
            color_continuous_scale="RdYlBu_r",
            origin="lower",
            aspect="auto",
            title="Room Temperatures Across Floors"
        )
        st.plotly_chart(fig_heat, use_container_width=True)

    with tab_anomalies:
        st.subheader("🚨 Real-Time Anomaly & Incident Feed")
        
        anomaly_rows = df[df["is_anomaly"] == True].sort_values("event_timestamp", ascending=False)
        if not anomaly_rows.empty:
            st.write(f"Displaying **{len(anomaly_rows)}** historical anomalous telemetry events.")
            display_cols = [
                "event_timestamp", "building_id", "floor", "room_id", "room_type",
                "temperature_c", "humidity_pct", "occupancy", "energy_kw",
                "anomaly_reason", "anomaly_severity"
            ]
            st.dataframe(
                anomaly_rows[display_cols].head(50),
                use_container_width=True,
                column_config={
                    "event_timestamp": st.column_config.DatetimeColumn("Timestamp", format="YYYY-MM-DD HH:mm:ss"),
                    "temperature_c": st.column_config.NumberColumn("Temp (°C)", format="%.1f"),
                    "humidity_pct": st.column_config.NumberColumn("Humidity (%)", format="%.1f"),
                    "energy_kw": st.column_config.NumberColumn("Power (kW)", format="%.2f"),
                    "anomaly_severity": st.column_config.TextColumn("Severity"),
                    "anomaly_reason": st.column_config.TextColumn("Root Cause")
                }
            )
        else:
            st.success("✅ No active anomalies detected. All building systems operating within optimal thresholds.")

    with tab_efficiency:
        st.subheader("🌱 Green Building & Sustainability Scorecard")
        if not gold_efficiency.empty:
            eff_disp = gold_efficiency.copy()
            if selected_bldg != "All":
                eff_disp = eff_disp[eff_disp["building_id"] == selected_bldg]
            
            c_score, c_waste, c_comfort = st.columns(3)
            with c_score:
                avg_score = eff_disp["green_score"].mean() if not eff_disp.empty else 85.0
                st.metric("Fleet Sustainability Score", f"{avg_score:.1f} / 100")
            with c_waste:
                total_wasted = eff_disp["wasted_energy_kwh"].sum() if not eff_disp.empty else 0.0
                st.metric("Total Wasted Energy (Off-Hours)", f"{total_wasted:.2f} kWh")
            with c_comfort:
                avg_comfort = eff_disp["comfort_compliance_pct"].mean() if not eff_disp.empty else 90.0
                st.metric("Comfort Setpoint Compliance", f"{avg_comfort:.1f} %")

            st.dataframe(eff_disp, use_container_width=True)
        else:
            st.info("Run Gold Layer ETL to generate aggregated sustainability metrics.")

    with tab_lineage:
        st.subheader("🏛️ Medallion Architecture Lineage & SCD Type 2 Dimension")
        st.write("Demonstration of lakehouse storage layers and slowly changing dimension versioning.")
        
        col_m1, col_m2, col_m3 = st.columns(3)
        with col_m1:
            bronze_count = len(list(BRONZE_DIR.glob("**/*.parquet"))) if BRONZE_DIR.exists() else 0
            st.markdown(f"**🥉 Bronze Layer (Raw)**\n- Path: `{BRONZE_DIR}`\n- Parquet Files: **{bronze_count}**\n- Retention: Immutable Append-Only")
        with col_m2:
            silver_count = len(silver_df) if not silver_df.empty else 0
            st.markdown(f"**🥈 Silver Layer (Cleaned)**\n- Path: `{SILVER_DIR}`\n- Cleaned Records: **{silver_count}**\n- Anomaly Flags: Evaluated")
        with col_m3:
            gold_count = len(gold_kpis) if not gold_kpis.empty else 0
            st.markdown(f"**🥇 Gold Layer (KPIs)**\n- Path: `{GOLD_BUILDING_KPIS_DIR}`\n- Hourly KPI Records: **{gold_count}**\n- Ready for Executive Dashboards")

        st.markdown("---")
        st.subheader("📋 SCD Type 2 Dimension Table (`dim_rooms`)")
        st.write("Notice how historical room configurations are retained with `effective_start_date`, `effective_end_date`, and `is_current` flags:")
        if not dim_rooms.empty:
            st.dataframe(dim_rooms.sort_values(by=["room_id", "version"]), use_container_width=True)
        else:
            st.info("Run `python src/batch/scd2_room_dimension.py` to inspect the dimension table.")


if __name__ == "__main__":
    main()
