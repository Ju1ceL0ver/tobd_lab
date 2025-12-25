# Seismo Analytics: Earthquake Heatmap

Course project for "Big Data Processing Technology".

Idea: ingest earthquakes from USGS, store raw and aggregated data, compute stats with Dask, and display an interactive heatmap (satellite tiles when available).

## Architecture

USGS API
→ Prefect Flow (extract/transform/load)
→ PostgreSQL (raw + dwh)
→ Dask (aggregations)
→ Streamlit (heatmap + charts)

## Stack

- Python 3.11
- Prefect 2.x
- Dask
- PostgreSQL
- Streamlit + PyDeck
- Docker Compose

## Structure

```
bigdata_project/
  data/
  flows/
    etl_flow.py
  dask_jobs/
    transform.py
  dashboards/
    streamlit_app.py
  docker-compose.yml
  Dockerfile
  requirements.txt
  README.md
```

## Run

1. Build images and start PostgreSQL:

```
docker compose up -d postgres
```

2. Run ETL (load USGS data + compute aggregates):

```
docker compose run --rm etl
```

3. Start the dashboard:

```
docker compose up app
```

Open `http://localhost:8501`.

## Satellite tiles

The dashboard uses Esri World Imagery tiles, no API key required.

## ETL parameters

Defaults: last 30 days, `min_magnitude=1.0`.
To override:

```
docker compose run --rm etl python flows/etl_flow.py --days-back 90 --min-magnitude 2.5
```
