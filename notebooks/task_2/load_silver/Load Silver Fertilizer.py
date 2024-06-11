# Databricks notebook source
# MAGIC %md 
# MAGIC Read bronze table and get the latest batch of records

# COMMAND ----------

import dlt

from pyspark.sql.functions import current_timestamp


@dlt.table(name="fertilizer_bronze")
def read_raw_table():
    # Read the raw table
    raw_df = dlt.read("tendo.raw.fertilizer")
    
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

@dlt.table(name="fertilizer_cleaned")
def read_fertilizer_bronze_data():
    fertilizer_bronze_data = dlt.read("fertilizer_bronze")
    
    # Add data quality expectations
    fertilizer_bronze_data.expect_or_drop("Non-null consumer_id", "col('consumer_id').isNotNull()")
    fertilizer_bronze_data.expect_or_drop("Non-null purchase_id", "col('purchase_id').isNotNull()")
    fertilizer_bronze_data.expect_or_drop("Non-null fertilizerid", "col('fertilizerid').isNotNull()")
    fertilizer_bronze_data.expect_or_drop("type should be StringType", "col('type').cast(StringType()))")
    fertilizer_bronze_data.expect_or_drop("mg should be IntegerType", "col('mg').cast(IntegerType())")
    fertilizer_bronze_data.expect_or_drop("frequency should be StringType", "col('frequency').cast(StringType())")
    
    # Add sanity expectations. Added sample values for each of the sanity expectations
    fertilizer_bronze_data.expect_or_drop("type should be between Organic, dry, or heavy metal", "col('type').isin(['heavy metal', 'dry', 'organic']")
    fertilizer_bronze_data.expect_or_drop("mg should be greater than 0", "col('mg') > 0")
    fertilizer_bronze_data.expect_or_drop("frequency should be in ['daily', 'weekly', 'monthly']", "col('frequency').isin(['daily', 'weekly', 'monthly'])")

    error_table_name = "fertilizer_bronze_data_errors"
    dlt.create_table(error_table_name, fertilizer_bronze_data.schema)
    
    # Insert dropped records into the error table
    dropped_records = fertilizer_bronze_data.filter(~col("_dlt_is_valid"))
    dlt.write(error_table_name, dropped_records)
    return fertilizer_bronze_data

# COMMAND ----------

# MAGIC %md Define Schema for Silver table and 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, DateType, col

# Define the schema for the silver table
silver_schema = StructType([
    StructField("purchase_id", IntegerType(), nullable=False),
    StructField("consumer_id", IntegerType(), nullable=False),
    StructField("fertilizer_id", StringType(), nullable=False),
    StructField("type", StringType(), nullable=True),
    StructField("mg", IntegerType(), nullable=True),
    StructField("frequency", StringType(), nullable=True)
])

cleaned_bronze = dlt.read("fertilizer_cleaned") \
                  .select(
                        col("purchase_id").cast(IntegerType()).alias("purchase_id"),
                        col("consumer_id").cast(IntegerType()).alias("consumer_id"),
                        col("fertilizerid").cast(StringType()).alias("fertilizer_id"),
                        col("type").cast(StringType()).alias("type"),
                        col("mg").cast(IntegerType()).alias("mg"),
                        col("frequency").cast(StringType()).alias("frequency")) \
                  .withColumn("load_time", current_timestamp())

# Create the silver table using the defined schema
silver_latest_valid_records = spark.createDataFrame([], silver_schema)

silver_latest_valid_records = silver_latest_valid_records.union(cleaned_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC Merge with Prod Silver table - Upsert

# COMMAND ----------

from pyspark.sql import DataFrame

prod_silver_table = spark.read.table("tendo.silver.fertilizer")

# Perform the upsert operation
def upsert_to_silver_table(existing_silver_table: DataFrame, latest_batch: DataFrame) -> DataFrame:
    # Merge the cleaned_bronze DataFrame into the existing_silver_table based on consumer_id and purchase_id
    merged_table = existing_silver_table.alias("s").merge(
        latest_batch.alias("c"),
        (col("s.consumer_id") == col("c.consumer_id")) & (col("s.purchase_id") == col("c.purchase_id")
                                                       &  (col("fertilizer_id") == col("c.fertilizer_id"))),
        "whenMatchedUpdate"
    ).whenNotMatchedInsertAll()

    return merged_table

# Upsert the cleaned_bronze into the existing_silver_table
updated_silver_table = upsert_to_silver_table(prod_silver_table, silver_latest_valid_records)

# write to delta table
updated_silver_table.write.mode("overwrite").format("delta").saveAsTable("tendo.silver.fertilizer")
