# Databricks notebook source
# MAGIC %md 
# MAGIC Read bronze table and get the latest batch of records

# COMMAND ----------

import dlt

from pyspark.sql.functions import current_timestamp


@dlt.table(name="purchase_bronze")
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

# MAGIC %md
# MAGIC - Add Dataquality checks to drop records that violate constraints and write to error table
# MAGIC - Cast to correct datatypes and prepare for silver merge
# MAGIC

# COMMAND ----------

@dlt.table(name="purchase_cleaned")
def read_purchase_bronze_data():
    purchase_bronze_data = dlt.read("purchase_bronze")
    
    # Add data type expectations
    purchase_bronze_data.expect_or_drop("Non-null consumer_id", "col('consumer_id').isNotNull()")
    purchase_bronze_data.expect_or_drop("Non-null purchase_id", "col('purchase_id').isNotNull()")
    purchase_bronze_data.expect_column_type("consumer_id", IntegerType())
    purchase_bronze_data.expect_column_type("purchase_id", IntegerType())
    purchase_bronze_data.expect_column_type("graphed_date", TimestampType())
    purchase_bronze_data.expect_column_type("avocado_bunch_id", IntegerType())
    purchase_bronze_data.expect_column_type("reporting_year", IntegerType())
    purchase_bronze_data.expect_column_type("qa_process", StringType())
    purchase_bronze_data.expect_column_type("billing_provider_sku", IntegerType())
    purchase_bronze_data.expect_column_type("grocery_store_id", IntegerType())
    purchase_bronze_data.expect_column_type("price_index", IntegerType())
    
    # Add sanity expectations
    purchase_bronze_data.expect_column_values_in_range("reporting_year", 2024, 2099)
    purchase_bronze_data.expect_column_values_in_set("qa_process", ["IR", "Basic"])
    purchase_bronze_data.expect_column_values_in_range("price_index", 1, 15)
    
    error_table_name = "purchase_bronze_data_errors"
    dlt.create_table(error_table_name, purchase_bronze_data.schema)
    
    # Insert dropped records into the error table
    dropped_records = purchase_bronze_data.filter(~col("_dlt_is_valid"))
    dlt.write(error_table_name, dropped_records)
    return purchase_bronze_data

# COMMAND ----------

# MAGIC %md Define Schema for Silver table and 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, DateType, col

# Define the schema for the silver table
silver_schema = StructType([
    StructField("consumer_id", IntegerType(), nullable=True),
    StructField("purchase_id", IntegerType(), nullable=False),
    StructField("graphed_date", TimestampType(), nullable=False),
    StructField("avocado_bunch_id", IntegerType(), nullable=True),
    StructField("reporting_year", IntegerType(), nullable=True),
    StructField("qa_process", StringType(), nullable=True),
    StructField("billing_provider_sku", IntegerType(), nullable=True),
    StructField("grocery_store_id", IntegerType(), nullable=True),
    StructField("price_index", IntegerType(), nullable=True),
    StructField("load_time", TimestampType(), nullable=False),
])

cleaned_bronze = dlt.read("purchase_cleaned") \
                  .select(
                        col("consumer_id").cast(IntegerType()).alias("consumer_id"),
                        col("purchase_id").cast(IntegerType()).alias("purchase_id"),
                        col("graphed_date").cast(TimestampType()).alias("graphed_date"),
                        col("avocado_bunch_id").cast(IntegerType()).alias("avocado_bunch_id"),
                        col("reporting_year").cast(IntegerType()).alias("reporting_year"),
                        col("ripe_index_when_picked").cast(IntegerType()).alias("ripe_index_when_picked"),                         
                        col("qa_process").cast(StringType()).alias("qa_process"),
                        col("billing_provider_sku").cast(IntegerType()).alias("billing_provider_sku"),
                        col("grocery_store_id").cast(IntegerType()).alias("grocery_store_id"),
                        col("price_index").cast(IntegerType()).alias("price_index"))
                  .withColumn("load_time", current_timestamp())

# Create the silver table using the defined schema
silver_latest_valid_records = spark.createDataFrame([], silver_schema)

silver_latest_valid_records = silver_latest_valid_records.union(cleaned_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC Merge with Prod Silver table

# COMMAND ----------

from pyspark.sql import DataFrame

prod_silver_table = spark.read.table("tendo.silver.purchase")

# Perform the upsert operation
def upsert_to_silver_table(existing_silver_table: DataFrame, latest_batch: DataFrame) -> DataFrame:
    # Merge the cleaned_bronze DataFrame into the existing_silver_table based on consumer_id and purchase_id
    merged_table = existing_silver_table.alias("s").merge(
        latest_batch.alias("c"),
        (col("s.consumer_id") == col("c.consumer_id")) & (col("s.purchase_id") == col("c.purchase_id")),
        "whenMatchedUpdate"
    ).whenNotMatchedInsertAll()

    return merged_table

# Upsert the cleaned_bronze into the existing_silver_table
updated_silver_table = upsert_to_silver_table(prod_silver_table, silver_latest_valid_records)

# write to delta table
updated_silver_table.write.mode("overwrite").format("delta").saveAsTable("tendo.silver.purchase")
