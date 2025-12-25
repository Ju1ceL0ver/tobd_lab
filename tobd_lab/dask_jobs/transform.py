import pandas as pd
import dask.dataframe as dd


def build_daily_stats(raw_df):
    if raw_df.empty:
        return pd.DataFrame(columns=["day", "event_count", "avg_mag", "max_mag"])

    df = raw_df.copy()
    df["mag"] = pd.to_numeric(df["mag"], errors="coerce")
    df = df[pd.notna(df["mag"])]
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df["day"] = df["time"].dt.date.astype(str)

    ddf = dd.from_pandas(df, npartitions=max(1, min(8, len(df) // 5000)))
    grouped = ddf.groupby("day").agg(
        event_count=("id", "count"),
        avg_mag=("mag", "mean"),
        max_mag=("mag", "max"),
    )

    result = grouped.compute().reset_index().sort_values("day")
    return result
