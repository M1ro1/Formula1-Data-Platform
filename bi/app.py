import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
import os

st.set_page_config(page_title="F1 Analytics DWH", page_icon="🏎️", layout="wide")


@st.cache_resource
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "F1_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "F1_DWH"),
        schema="MARTS"
    )


conn = get_snowflake_connection()


@st.cache_data(ttl=3600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetch_pandas_all()


st.title("🏎️ Formula 1 Data Warehouse Analytics")

tab1, tab2, tab3 = st.tabs([
    "🏆 Driver & Constructor Performance",
    "⏱️ Pit Stop Efficiency",
    "🎯 Grid Position vs Finish Correlation"
])

# Dashboard 1: Driver & Constructor Standings
with tab1:
    st.header("Driver & Constructor Performance by Season")
    season = st.selectbox("Select Season", options=list(range(2026, 1949, -1)))

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Driver Standings")
        df_drivers = run_query(f"""
            SELECT DRIVER_FULL_NAME, RACE_POINTS, RACES_ENTERED, WINS, PODIUMS, AVG_FINISH_POSITION 
            FROM F1_DWH.MARTS.MART_DRIVER_SEASON_STATS
            WHERE SEASON = {season}
            ORDER BY RACE_POINTS DESC
        """)
        st.dataframe(df_drivers, use_container_width=True)
        fig_driver = px.bar(df_drivers.head(10), x="DRIVER_FULL_NAME", y="RACE_POINTS", title="Top 10 Drivers Points")
        st.plotly_chart(fig_driver, use_container_width=True)

    with col2:
        st.subheader("Constructor Standings")
        df_constructors = run_query(f"""
            SELECT constructor_name, race_points, wins
            FROM mart_constructor_standings
            WHERE season = {season}
            ORDER BY race_points DESC
        """)
        st.dataframe(df_constructors, use_container_width=True)
        fig_team = px.pie(df_constructors, values="RACE_POINTS", names="CONSTRUCTOR_NAME",
                          title="Points Share by Constructor")
        st.plotly_chart(fig_team, use_container_width=True)

# Dashboard 2: Pit Stop Performance
with tab2:
    st.header("Pit Stop Duration Analysis")
    df_pit = run_query("""
                       SELECT constructor_name, season, avg_clean_duration_seconds, median_duration_seconds
                       FROM F1_DWH.MARTS.MART_PIT_STOP_PERFOMANCE
                       WHERE season >= 2020
                       ORDER BY season DESC, avg_clean_duration_seconds ASC
                       """)
    fig_pit = px.box(df_pit, x="CONSTRUCTOR_NAME", y="AVG_CLEAN_DURATION_SECONDS", color="CONSTRUCTOR_NAME",
                     title="Average Pit Stop Duration Distribution by Team (2020-Present)")
    st.plotly_chart(fig_pit, use_container_width=True)

# Dashboard 3: Grid vs Finish Position Correlation
with tab3:
    st.header("Qualifying Position vs Race Finish Correlation")
    df_grid = run_query("""
                        SELECT grid_position, finish_position, COUNT(*) as race_count
                        FROM fct_race_results
                        WHERE grid_position > 0
                          AND finish_position > 0
                        GROUP BY grid_position, finish_position
                        """)
    fig_scatter = px.scatter(df_grid, x="GRID_POSITION", y="FINISH_POSITION", size="RACE_COUNT",
                             title="Impact of Starting Position on Final Race Result",
                             labels={"GRID_POSITION": "Grid Position", "FINISH_POSITION": "Finish Position"})
    st.plotly_chart(fig_scatter, use_container_width=True)