import glob, duckdb, polars as pl
from pathlib import Path

db_path = "data/weather.duckdb"
raw_dir = Path("data/raw")
latest = max(glob.glob(str(raw_dir / "weather_*.csv")))
con = duckdb.connect(db_path)
con.execute("CREATE SCHEMA IF NOT EXISTS raw")
df = pl.read_csv(latest)
con.execute("""
    CREATE TABLE IF NOT EXISTS raw.weather (
        city TEXT, date DATE, temp_max DOUBLE, temp_min DOUBLE, precipitation DOUBLE
    )
""")
con.execute("INSERT INTO raw.weather SELECT * FROM df")
print(f"Loaded {latest} into raw.weather")