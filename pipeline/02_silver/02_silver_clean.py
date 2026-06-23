# Databricks notebook source
# MAGIC %md
# MAGIC # Silver – clean the readings
# MAGIC Removes nulls, duplicate readings and
# MAGIC impossible values. Statistic anomalies readings are kept here and
# MAGIC flagged later in gold.

# COMMAND ----------

dbutils.widgets.text("catalog", "colibri_dev")
dbutils.widgets.text("schema", "wind")


#(will use job parameters when running via DABs)
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %run /Workspace/Users/andrewj9998@gmail.com/colibri_interview_test_andrew_jackson/pipeline/transforms

# COMMAND ----------

bronze = spark.table(f"{catalog}.{schema}.bronze_turbine_readings")
silver = clean(bronze)

silver.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_turbine_readings")

# COMMAND ----------

display(silver)
