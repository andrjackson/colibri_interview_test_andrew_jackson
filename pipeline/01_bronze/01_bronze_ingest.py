# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze – ingest raw turbine readings

# COMMAND ----------

dbutils.widgets.text("catalog", "colibri_dev")
dbutils.widgets.text("schema", "wind")
dbutils.widgets.text("source_volume", "landing_zone")
dbutils.widgets.text("checkpoint_volume", "checkpoints")

#(will use job parameters when running via DABs)
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_volume = dbutils.widgets.get("source_volume")
checkpoint_volume = dbutils.widgets.get("checkpoint_volume")

# COMMAND ----------

# for development purposes, variables can be hardcoded here but should be modified in the DAB ASAP. Or they can be added in the widgets, the DAB will overwrite the widget variables.

checkpoint_path = f"/Volumes/{catalog}/{schema}/{checkpoint_volume}/bronze"
source_path = f"/Volumes/{catalog}/{schema}/{source_volume}"
# catalog = 
# schema = 

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, TimestampType, IntegerType, DoubleType,
)

# COMMAND ----------

#strict structure, not allowing schema drift. We could also modify this to warn when there is schema drift, but this version will fail if data types change
raw_schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("turbine_id", IntegerType()),
    StructField("wind_speed", DoubleType()),
    StructField("wind_direction", IntegerType()),
    StructField("power_output", DoubleType()),
])

# COMMAND ----------

# DBTITLE 1,Cell 4
#auto-loader
stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option("header", True)
    .schema(raw_schema)
    .load(source_path)
    .writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.bronze_turbine_readings")
)

stream.awaitTermination()

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.bronze_turbine_readings"))

# COMMAND ----------


