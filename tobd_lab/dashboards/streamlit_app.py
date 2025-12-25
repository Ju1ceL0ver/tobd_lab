import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
import plotly.express as px
from flows.etl_flow import (
    extract,
    load_raw,
    transform,
    load_daily_stats as etl_load_daily_stats,
    _ensure_tables,
)


def _db_url():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "quake")
    user = os.getenv("POSTGRES_USER", "quake")
    password = os.getenv("POSTGRES_PASSWORD", "quake")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


@st.cache_data(ttl=60)
def load_dates(_engine):
    with _engine.begin() as conn:
        row = conn.execute(
            text("SELECT MIN(time)::date, MAX(time)::date FROM raw_events")
        )
        min_date, max_date = row.first()
    return min_date, max_date


def load_events_with_progress(engine, start_date, end_date, min_mag):
    count_query = text(
        """
        SELECT COUNT(*)
        FROM raw_events
        WHERE time::date BETWEEN :start_date AND :end_date
          AND mag >= :min_mag
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        """
    )
    data_query = text(
        """
        SELECT id, time, mag, place, latitude, longitude, depth
        FROM raw_events
        WHERE time::date BETWEEN :start_date AND :end_date
          AND mag >= :min_mag
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        """
    )
    params = {"start_date": start_date, "end_date": end_date, "min_mag": min_mag}

    with engine.begin() as conn:
        total = conn.execute(count_query, params).scalar() or 0

    if total == 0:
        return pd.DataFrame(
            columns=["id", "time", "mag", "place", "latitude", "longitude", "depth"]
        )

    progress = st.progress(0, text="Loading events...")
    chunks = []
    loaded = 0
    for chunk in pd.read_sql(data_query, engine, params=params, chunksize=2000):
        chunks.append(chunk)
        loaded += len(chunk)
        progress.progress(min(loaded / total, 1.0))
        time.sleep(0.01)
    progress.empty()

    return pd.concat(chunks, ignore_index=True)


@st.cache_data(ttl=60)
def load_daily_stats_cached(_engine):
    return pd.read_sql("SELECT * FROM dwh_daily_stats ORDER BY day", _engine)


st.set_page_config(page_title="Сейсмоаналитика", layout="wide")

st.title("Сейсмоаналитика: места и сила толчков")

engine = create_engine(_db_url())
_ensure_tables(engine)
min_date, max_date = load_dates(engine)

today = date.today()
min_allowed = date(1900, 1, 1)
default_start = today - timedelta(days=30)
default_end = today
if not min_date or not max_date:
    st.info("Данных в базе пока нет. Нажми кнопку ниже, чтобы загрузить события USGS.")
else:
    st.caption(f"Данные в базе: {min_date} — {max_date}")

col1, col2 = st.columns([4, 2])
with col1:
    date_range = st.date_input(
        "Период",
        value=(default_start, default_end),
        min_value=min_allowed,
        max_value=today,
    )
with col2:
    min_mag = st.slider("Минимальная магнитуда", 0.0, 8.0, 2.5, 0.1)

if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = date_range
    end_date = date_range

if start_date > end_date:
    st.error("Дата начала должна быть раньше даты окончания.")
    st.stop()

st.divider()
st.subheader("Загрузка данных по требованию")
if st.button("Загрузить данные USGS за выбранный период"):
    with st.spinner("Запускаю ETL за выбранный период..."):
        progress = st.progress(0)
        start_dt = datetime.combine(
            start_date, datetime.min.time(), tzinfo=timezone.utc
        )
        end_dt = datetime.combine(end_date, datetime.min.time(), tzinfo=timezone.utc)
        raw_df = extract.fn(start_dt, end_dt, min_mag)
        progress.progress(0.35)
        inserted = load_raw.fn(raw_df)
        progress.progress(0.65)
        stats_df = transform.fn()
        progress.progress(0.85)
        daily_rows = etl_load_daily_stats.fn(stats_df)
        progress.progress(1.0)
        st.cache_data.clear()
        st.success(
            f"Загружено событий: {inserted}. Обновлено дневных строк: {daily_rows}."
        )
    progress.empty()

events = load_events_with_progress(engine, start_date, end_date, min_mag)

st.subheader("Карта событий 2D")

if events.empty:
    st.warning("Нет событий по выбранным фильтрам.")
else:
    map_events = events[["latitude", "longitude", "mag"]].dropna()
    fig2d = px.scatter_geo(
        map_events,
        lat="latitude",
        lon="longitude",
        color="mag",
        size="mag",
        size_max=14,
        projection="natural earth",
        color_continuous_scale="Turbo",
    )
    fig2d.update_geos(
        showcountries=False,
        showcoastlines=True,
        coastlinecolor="rgba(255,255,255,0.35)",
        showland=True,
        landcolor="rgb(18, 28, 20)",
        showocean=True,
        oceancolor="rgb(8, 12, 22)",
        bgcolor="rgb(8, 12, 22)",
        lataxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.06)"),
        lonaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.06)"),
    )
    fig2d.update_layout(
        height=600,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="rgb(8, 12, 22)",
        plot_bgcolor="rgb(8, 12, 22)",
        coloraxis_colorbar=dict(title="Магнитуда"),
    )
    st.plotly_chart(fig2d, use_container_width=True)

st.subheader("Динамика активности")
stats = load_daily_stats_cached(engine)
if stats.empty:
    st.info("Агрегаты пока не построены. Запусти ETL.")
else:
    chart_df = stats.set_index("day")
    left, right = st.columns(2)
    with left:
        st.caption("События по дням")
        st.line_chart(chart_df["event_count"])
    with right:
        st.caption("Средняя магнитуда по дням")
        st.line_chart(chart_df["avg_mag"])

st.subheader("Примеры событий")
st.dataframe(events.head(200), use_container_width=True)

st.subheader("Карта событий 3D")
if not events.empty:
    globe_events = events[["latitude", "longitude", "mag"]].dropna().head(5000)
    fig = px.scatter_geo(
        globe_events,
        lat="latitude",
        lon="longitude",
        color="mag",
        size="mag",
        size_max=12,
        projection="orthographic",
        color_continuous_scale="Reds",
    )
    fig.update_geos(
        showcountries=False,
        showcoastlines=True,
        coastlinecolor="rgba(255,255,255,0.35)",
        showland=True,
        landcolor="rgb(15, 50, 30)",
        showocean=True,
        oceancolor="rgb(5, 10, 25)",
        bgcolor="rgb(5, 8, 15)",
    )
    fig.update_layout(
        height=600,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="rgb(5, 8, 15)",
        plot_bgcolor="rgb(5, 8, 15)",
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Нет событий для глобуса. Измени период или фильтр по магнитуде.")
