# Databricks notebook source
# MAGIC %md
# MAGIC Setup the environment using the dbx widgets provided

# COMMAND ----------

import shutil
import os

# COMMAND ----------

dbutils.widgets.text("catalog", "colibri_dev")
dbutils.widgets.text("schema", "wind")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}") 
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.landing_zone")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints")

print(f"{catalog}.{schema}")
print(f"/Volumes/{catalog}/{schema}/landing_zone")
print(f"/Volumes/{catalog}/{schema}/checkpoints")

# COMMAND ----------

# send the csvs to the volume - one time manual load to start
src_dir = "/Workspace/Users/andrewj9998@gmail.com/colibri_interview_test_andrew_jackson/src_data"
dst_dir = f"/Volumes/{catalog}/{schema}/landing_zone"

for file_name in os.listdir(src_dir):
    if file_name.endswith(".csv"):
        shutil.copy(os.path.join(src_dir, file_name), dst_dir)

# COMMAND ----------


