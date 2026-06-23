# Databricks notebook source
# MAGIC %md
# MAGIC # Tests
# MAGIC Quick checks on the transforms using small in-memory DataFrames. Run the
# MAGIC notebook top to bottom; it asserts and prints a pass line at the end.

# COMMAND ----------

# MAGIC %run ./transforms

# COMMAND ----------

import datetime as dt

COLS = ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]


def ts(hour):
    return dt.datetime(2022, 3, 1, hour)

# COMMAND ----------

# clean: only the one valid row survives the null / duplicate / out-of-range rows
rows = [
    (ts(0), 1, 10.0, 180, 2.5),
    (ts(0), 1, 10.0, 180, 2.5),    # duplicate
    (ts(1), 1, 10.0, 180, None),   # missing power
    (ts(2), 1, 99.0, 180, 2.0),    # wind speed too high
    (ts(3), 1, 10.0, 400, 2.0),    # impossible direction
]
assert clean(spark.createDataFrame(rows, COLS)).count() == 1

# COMMAND ----------

# daily_summary: min / max / avg over one day
rows = [(ts(0), 1, 10.0, 180, 1.0), (ts(1), 1, 10.0, 180, 3.0), (ts(2), 1, 10.0, 180, 2.0)]
r = daily_summary(spark.createDataFrame(rows, COLS)).first()
assert (r.min_power, r.max_power, r.avg_power) == (1.0, 3.0, 2.0)

# COMMAND ----------

# flag_anomalies: a single spike is flagged, the steady readings are not
rows = [(ts(h), 1, 10.0, 180, 2.0) for h in range(10)] + [(ts(11), 1, 10.0, 180, 50.0)]
flags = {r.power_output: r.is_anomaly for r in flag_anomalies(spark.createDataFrame(rows, COLS)).collect()}
assert flags[50.0] and not flags[2.0]

# COMMAND ----------

print("All tests passed")

