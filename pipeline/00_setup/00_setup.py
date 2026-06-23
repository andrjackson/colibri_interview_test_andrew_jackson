# Databricks notebook source
# MAGIC %md
# MAGIC Setup the environment using the dbx widgets provided

# COMMAND ----------

dbutils.widgets.text("catalog", "colibri_dev")
dbutils.widgets.text("schema", "wind")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}") 
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints")

print(f"{catalog}.{schema}")
print(f"/Volumes/{catalog}/{schema}/landing_zone")
print(f"/Volumes/{catalog}/{schema}/checkpoints")

# COMMAND ----------


