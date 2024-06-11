# Databricks notebook source
avocado_mnt = 's3://tendo-customer-data/avocado'
consumer_mnt = 's3://tendo-customer-data/consumer'
purchase_mnt = 's3://tendo-customer-data/purchase'
fertilizer_mnt = 's3://tendo-customer-data/fertilizer'

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]

# Define variables used in code below
file_path = 's3://tendo-customer-data/avocado'
table_name = f"tendo.raw.avocado"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest CSV data to a Delta table
df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .option("header", "true")
  .option("inferSchema", "true")
  .load(file_path)
  .select("consumerid",
          "purchaseid",
          "avocado_bunch_id",
          "plu", 
          col("ripe index when picked").alias("ripe_index_when_picked"),
          "born_date",
          "picked_date",
          "sold_date",
          col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .option("mergeSchema", "true")
  .toTable(table_name))

# COMMAND ----------

display(avocado_df)
