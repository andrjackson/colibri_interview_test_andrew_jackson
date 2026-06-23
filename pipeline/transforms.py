# Databricks notebook source
# MAGIC %md
# MAGIC # Transforms
# MAGIC transformation functions, use them with %run ./transforms.

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# COMMAND ----------

def clean(df: DataFrame) -> DataFrame:
    """remove nulls, duplicates and unlikely readings."""
    return (
        df
        .dropna(subset=["timestamp", "turbine_id", "power_output"])
        .dropDuplicates(["turbine_id", "timestamp"])
        .filter(
            (F.col("power_output") >= 0)
            # wind speed cannot be negative, and is unlikely to be greater than 50 (this is an assumption of course, maybe this wind turbine was in a hurricane)
            & F.col("wind_speed").between(0, 50)
            #  wind direction cannot be negative or greater than all degrees (360)
            & F.col("wind_direction").between(0, 360)
        )
    )


def daily_summary(df: DataFrame) -> DataFrame:
    """turbine min/max/avg power for each calendar day."""
    return (
        df
        .withColumn("date", F.to_date("timestamp"))
        .groupBy("turbine_id", "date")
        .agg(
            F.min("power_output").alias("min_power"),
            F.max("power_output").alias("max_power"),
            F.round(F.avg("power_output"), 3).alias("avg_power"),
        )
    )


def flag_anomalies(df: DataFrame, n_std: float = 2.0) -> DataFrame:
    """flag readings more than n_std standard deviations from the turbine's mean."""
    per_turbine = Window.partitionBy("turbine_id")
    return (
        df
        .withColumn("mean_power", F.avg("power_output").over(per_turbine))
        .withColumn("std_power", F.stddev("power_output").over(per_turbine))
        .withColumn(
            "is_anomaly",
            F.abs(F.col("power_output") - F.col("mean_power")) > n_std * F.col("std_power"),
        )
    )

# COMMAND ----------


