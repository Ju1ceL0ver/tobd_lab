import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import pandas as pd
import requests
from prefect import flow, task
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from psycopg2.extras import execute_values

from dask_jobs.transform import build_daily_stats


def _db_url():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "quake")
    user = os.getenv("POSTGRES_USER", "quake")
    password = os.getenv("POSTGRES_PASSWORD", "quake")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def _wait_for_db(engine, retries=10, delay_seconds=3):
    for attempt in range(1, retries + 1):
        try:
            with engine.connect():
                return
        except OperationalError:
            if attempt == retries:
                raise
            time.sleep(delay_seconds)


def _ensure_tables(engine):
    _wait_for_db(engine)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS raw_events (
                    id TEXT PRIMARY KEY,
                    time TIMESTAMPTZ,
                    mag DOUBLE PRECISION,
                    place TEXT,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    depth DOUBLE PRECISION,
                    url TEXT,
                    raw_json JSONB
                );
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS dwh_daily_stats (
                    day DATE PRIMARY KEY,
                    event_count INTEGER,
                    avg_mag DOUBLE PRECISION,
                    max_mag DOUBLE PRECISION
                );
                """
            )
        )


@task(retries=2, retry_delay_seconds=10)
def extract(start_time, end_time, min_magnitude):
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": start_time.date().isoformat(),
        "endtime": end_time.date().isoformat(),
        "minmagnitude": min_magnitude,
        "orderby": "time",
        "limit": 20000,
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    records = []
    for feature in payload.get("features", []):
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates") or [None, None, None]
        event_time = props.get("time")
        if event_time is None:
            continue
        records.append(
            {
                "id": feature.get("id"),
                "time": datetime.fromtimestamp(event_time / 1000, tz=timezone.utc),
                "mag": props.get("mag"),
                "place": props.get("place"),
                "longitude": coords[0],
                "latitude": coords[1],
                "depth": coords[2],
                "url": props.get("url"),
                "raw_json": json.dumps(feature, ensure_ascii=True),
            }
        )

    return pd.DataFrame.from_records(records)


@task
def load_raw(raw_df):
    if raw_df.empty:
        return 0

    engine = create_engine(_db_url())
    _ensure_tables(engine)

    rows = raw_df.to_dict(orient="records")
    columns = [
        "id",
        "time",
        "mag",
        "place",
        "latitude",
        "longitude",
        "depth",
        "url",
        "raw_json",
    ]
    values = [[row.get(col) for col in columns] for row in rows]

    insert_sql = """
        INSERT INTO raw_events (
            id, time, mag, place, latitude, longitude, depth, url, raw_json
        ) VALUES %s
        ON CONFLICT (id) DO NOTHING;
    """

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values)
        conn.commit()
    finally:
        conn.close()

    return len(values)


@task
def transform():
    engine = create_engine(_db_url())
    _ensure_tables(engine)

    raw_df = pd.read_sql("SELECT * FROM raw_events", engine)
    return build_daily_stats(raw_df)


@task
def load_daily_stats(stats_df):
    if stats_df.empty:
        return 0

    engine = create_engine(_db_url())
    _ensure_tables(engine)

    rows = stats_df.to_dict(orient="records")
    columns = ["day", "event_count", "avg_mag", "max_mag"]
    values = [[row.get(col) for col in columns] for row in rows]

    insert_sql = """
        INSERT INTO dwh_daily_stats (day, event_count, avg_mag, max_mag)
        VALUES %s
        ON CONFLICT (day) DO UPDATE
        SET event_count = EXCLUDED.event_count,
            avg_mag = EXCLUDED.avg_mag,
            max_mag = EXCLUDED.max_mag;
    """

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values)
        conn.commit()
    finally:
        conn.close()

    return len(values)


@flow(name="earthquake-etl")
def etl_flow(days_back=30, min_magnitude=1.0):
    end_time = datetime.now(tz=timezone.utc)
    start_time = end_time - timedelta(days=days_back)

    raw_df = extract(start_time, end_time, min_magnitude)
    inserted = load_raw(raw_df)
    stats_df = transform()
    daily_rows = load_daily_stats(stats_df)

    print(f"Inserted raw events: {inserted}")
    print(f"Upserted daily stats: {daily_rows}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run earthquake ETL flow")
    parser.add_argument("--days-back", type=int, default=30)
    parser.add_argument("--min-magnitude", type=float, default=1.0)
    args = parser.parse_args()

    etl_flow(days_back=args.days_back, min_magnitude=args.min_magnitude)
