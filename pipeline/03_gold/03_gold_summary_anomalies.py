# Databricks notebook source
# MAGIC %md
# MAGIC # Gold – daily summary & anomalies
# MAGIC min/max/avg power for each day, plus readings that sit more
# MAGIC than n standard deviations from that turbine's own mean.

# COMMAND ----------

# MAGIC %run /Workspace/Users/andrewj9998@gmail.com/colibri_interview_test_andrew_jackson/pipeline/transforms

# COMMAND ----------

n_std = float("2.0")

silver = spark.table(f"{catalog}.{schema}.silver_turbine_readings")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily summary

# COMMAND ----------

summary = daily_summary(silver)
summary.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_daily_summary")
display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anomalies

# COMMAND ----------

anomalies = (
    flag_anomalies(silver, n_std)
    .filter("is_anomaly")
    .select("timestamp", "turbine_id", "power_output", "mean_power", "std_power")
)
anomalies.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_anomalies")
display(anomalies)
