# Databricks notebook source
# MAGIC %md
# MAGIC Use Autoloader to scan S3 location and write to raw/bronze layer as delta 

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, col

# Define the source table for the Delta Live Table pipeline
@dlt.table(
    name="avocado",
    comment="Raw data ingested from S3 bucket",
    table_properties={"quality": "bronze"}
)
def raw_data():
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")  # 
          .option("cloudFiles.schemaLocation", "/mnt/s3/schema_location")
          .option("cloudFiles.inferColumnTypes", "true")
          .option("cloudFiles.useNotifications", "true")  # Enables S3 event notifications
          .load("s3://tendo-customer-data/avocado")
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
          .option("checkpointLocation", "mnt/s3/bronze/avocado_raw")
          .option("mergeSchema", "true")
          .toTable("avocado"))

raw_data()
