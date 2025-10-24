import glob, duckdb, polars as pl
from pathlib import Path

db_path = "weather.duckdb"
raw_dir = Path("data/raw")
latest = max(glob.glob(str(raw_dir / "weather_global_history_20251013_232134.csv")))
con = duckdb.connect(db_path)
con.execute("CREATE SCHEMA IF NOT EXISTS raw")
df = pl.read_csv(latest)
con.execute("""
    CREATE TABLE IF NOT EXISTS raw.weatherhistorical (
        city TEXT, date DATE, temp_max DOUBLE, temp_min DOUBLE, precipitation DOUBLE, windspeed_max DOUBLE
    )
""")
con.register("df", df)
con.execute("INSERT INTO raw.weatherhistorical SELECT city, date, temp_max, temp_min, precipitation, windspeed_max FROM df")
print(f"Loaded {latest} into raw.weatherhistorical")