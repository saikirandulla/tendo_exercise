# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp


username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]

# Define variables used in code below
file_path = 's3://tendo-customer-data/consumer'
table_name = f"tendo.raw.consumer"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

# # Clear out data from previous execution
# spark.sql(f"DROP TABLE IF EXISTS {table_name}")
# dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest CSV data to a Delta table
df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .option("header", "true")
  .option("inferSchema", "true")
  .load(file_path)
  .select("consumerid",
          "sex",
          "ethnicity",
          "race", 
          "age",
          col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .option("mergeSchema", "true")
  .toTable(table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC Create tables 
