# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze – ingest raw turbine readings

# COMMAND ----------

/Volumes/main/wind/checkpoints/bronze

# COMMAND ----------

# for development purposes, variables can be hardcoded here but should be modified in the DAB ASAP

checkpoint_path = f"/Volumes/{catalog}/{schema}/{checkpoint_volume}/bronze"
source_path = f"/Volumes/{catalog}/{schema}/{source_volume}
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
df = (
    spark.read
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option("header", True)
    .schema(raw_schema)
    .load(source_path)
)

df.write.mode("append").saveAsTable(f"{catalog}.{schema}.bronze_turbine_readings")

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.bronze_turbine_readings"))
