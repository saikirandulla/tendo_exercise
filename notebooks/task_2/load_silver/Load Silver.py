# Databricks notebook source
table_name = "tendo.raw.avocado"

if spark.catalog.tableExists(table_name):
    print(f"The table {table_name} exists.")
else:
    print(f"The table {table_name} does not exist.")

# COMMAND ----------

import dlt

@dlt.table(name="avocado_raw")
def read_raw_table():
    # Read the raw table
    raw_df = spark.read.format("delta").load("/tendo/raw/avocado")
    
    # Perform any necessary transformations or filtering on the raw_df
    
    return raw_df

# COMMAND ----------

import os

path = "/tendo/raw/avocado"
os.makedirs(path, exist_ok=True)

# COMMAND ----------

import dlt

from pyspark.sql.functions import current_timestamp


@dlt.table(name="avocado_bronze")
def read_raw_table():
    # Read the raw table
    raw_df = dlt.read("tendo.raw.avocado")
    
    # Find the maximum load timestamp
    max_load_timestamp = raw_df.agg({"processing_time": "max"}).collect()[0][0]
    
    # Filter records with the maximum load timestamp to get the latest batch of records
    filtered_df = raw_df.filter(raw_df.load_timestamp == max_load_timestamp)
    
    # Check if the filtered DataFrame has any records
    if filtered_df.count() > 0:
        return filtered_df
    else:
        raise ValueError("No records found in the filtered DataFrame.")




# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Define the schema for the silver table
silver_schema = StructType([
    StructField("consumer_id", IntegerType(), nullable=true),
    StructField("purchase_id", IntegerType(), nullable=false),
    StructField("avocado_bunch_id", IntegerType(), nullable=true),
    StructField("plu", IntegerType(), nullable=true),
    StructField("ripe_index_when_picked", IntegerType(), nullable=true),
    StructField("born_date", DateType(), nullable=true),
    StructField("picked_date", DateType(), nullable=true),
    StructField("sold_date", DateType(), nullable=true),

])

# Create the silver table using the defined schema
silver_table = spark.createDataFrame([], silver_schema)
In this example, the schema for the silver table 
