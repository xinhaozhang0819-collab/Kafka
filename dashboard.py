import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Real-Time Ride-sharing Dashboard", layout="wide")
st.title("🚕 Real-Time Ride-sharing Trips Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"


@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)


engine = get_engine(DATABASE_URL)


def load_data(status_filter: str | None = None, limit: int = 500) -> pd.DataFrame:
    base_query = "SELECT * FROM trips"
    params: dict = {}

    if status_filter and status_filter != "All":
        base_query += " WHERE status = :status"
        params["status"] = status_filter

    base_query += " ORDER BY timestamp DESC LIMIT :limit"
    params["limit"] = limit

    try:
        df = pd.read_sql_query(text(base_query), con=engine.connect(), params=params)
        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()


status_options = ["All", "Searching", "Ongoing", "Completed", "Cancelled"]
selected_status = st.sidebar.selectbox("Filter by Trip Status", status_options)

update_interval = st.sidebar.slider(
    "Update Interval (seconds)", min_value=2, max_value=20, value=5
)

limit_records = st.sidebar.number_input(
    "Number of records to load", min_value=50, max_value=5000, value=500, step=50
)

if st.sidebar.button("Refresh now"):
    st.rerun()

placeholder = st.empty()

while True:
    df_trips = load_data(selected_status, limit=int(limit_records))

    with placeholder.container():
        if df_trips.empty:
            st.warning("No trip records found yet. Waiting for data...")
            st.caption(
                f"Last updated: {datetime.now().isoformat()} • "
                f"Auto-refresh: {update_interval}s"
            )
            time.sleep(update_interval)
            continue

        if "timestamp" in df_trips.columns:
            df_trips["timestamp"] = pd.to_datetime(df_trips["timestamp"])

        total_trips = len(df_trips)
        total_revenue = df_trips["price"].sum()

        avg_distance = df_trips["distance_km"].mean()
        avg_duration = df_trips["duration_min"].mean()

        completed = len(df_trips[df_trips["status"] == "Completed"])
        cancelled = len(df_trips[df_trips["status"] == "Cancelled"])
        completion_rate = (completed / total_trips * 100) if total_trips > 0 else 0.0

        st.subheader(f"Displaying {total_trips} trips (Filter: {selected_status})")

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total Trips", total_trips)
        k2.metric("Total Revenue", f"${total_revenue:,.2f}")
        k3.metric("Avg Distance (km)", f"{avg_distance:,.2f}")
        k4.metric("Avg Duration (min)", f"{avg_duration:,.1f}")
        k5.metric("Completion Rate", f"{completion_rate:,.2f}%")

        st.markdown("### Raw Trips Data (Top 10)")
        st.dataframe(df_trips.head(10), use_container_width=True)


        trips_by_pickup = (
            df_trips.groupby("pickup_city")["trip_id"].count().reset_index()
        )
        trips_by_pickup.rename(columns={"trip_id": "trip_count"}, inplace=True)

        fig_trips_city = px.bar(
            trips_by_pickup,
            x="pickup_city",
            y="trip_count",
            title="Trips Count by Pickup City",
        )


        revenue_by_pickup = (
            df_trips.groupby("pickup_city")["price"].sum().reset_index()
        )
        fig_revenue_city = px.bar(
            revenue_by_pickup,
            x="pickup_city",
            y="price",
            title="Revenue by Pickup City",
        )


        fig_distance_dist = px.histogram(
            df_trips,
            x="distance_km",
            nbins=20,
            title="Trip Distance Distribution (km)",
        )

        if "timestamp" in df_trips.columns:
            df_trips_time = df_trips.set_index("timestamp").resample("1min")[
                "trip_id"
            ].count()
            df_trips_time = df_trips_time.reset_index().rename(
                columns={"trip_id": "trip_count"}
            )
            fig_trips_time = px.line(
                df_trips_time,
                x="timestamp",
                y="trip_count",
                title="Trips per Minute",
            )
        else:
            fig_trips_time = None

        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(fig_trips_city, use_container_width=True)
        with c2:
            st.plotly_chart(fig_revenue_city, use_container_width=True)

        c3, c4 = st.columns(2)
        with c3:
            st.plotly_chart(fig_distance_dist, use_container_width=True)
        with c4:
            if fig_trips_time is not None:
                st.plotly_chart(fig_trips_time, use_container_width=True)
            else:
                st.info("No timestamp data for time-series chart.")

        st.markdown("---")
        st.caption(
            f"Last updated: {datetime.now().isoformat()} • "
            f"Auto-refresh: {update_interval}s"
        )

    time.sleep(update_interval)
