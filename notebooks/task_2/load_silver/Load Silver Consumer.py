# Databricks notebook source
# MAGIC %md 
# MAGIC Read bronze table and get the latest batch of records

# COMMAND ----------

import dlt

from pyspark.sql.functions import current_timestamp


@dlt.table(name="consumer_bronze")
def read_raw_table():
    # Read the raw table
    raw_df = dlt.read("tendo.raw.consumer")
    
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

@dlt.table(name="consumer_cleaned")
def read_consumer_bronze_data():
    consumer_bronze_data = dlt.read("consumer_bronze")
    
    # Add data quality expectations
    consumer_bronze_data.expect_or_drop("Non-null consumer_id", "col('consumer_id').isNotNull()")
    # Sanity expectations for consumer_id
    consumer_bronze_data.expect("Positive consumer_id", col("consumer_id") > 0)

    # Sanity expectations for sex
    consumer_bronze_data.expect("Valid sex values", col("sex").isin(["M", "F"]))

    # Sanity expectations for ethnicity
    consumer_bronze_data.expect("Non-empty ethnicity", col("ethnicity").isNotNull())

    # Sanity expectations for race
    consumer_bronze_data.expect("Non-empty race", col("race").isNotNull())

    # Sanity expectations for age
    consumer_bronze_data.expect("Positive age", col("age") > 0)

    # Sanity expectations for age range
    consumer_bronze_data.expect("Valid age range", (col("age") >= 18) & (col("age") <= 100))

    error_table_name = "consumer_bronze_data_errors"
    dlt.create_table(error_table_name, consumer_bronze_data.schema)
    
    # Insert dropped records into the error table
    dropped_records = consumer_bronze_data.filter(~col("_dlt_is_valid"))
    dlt.write(error_table_name, dropped_records)
    return consumer_bronze_data

# COMMAND ----------

# MAGIC %md Define Schema for Silver table and 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, DateType, col

# Define the schema for the silver table
silver_schema = StructType([
    StructField("consumer_id", IntegerType(), nullable=True),
    StructField("sex", StringType(), nullable=True),
    StructField("ethnicity", StringType(), nullable=True),
    StructField("race", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("load_time", TimestampType(), nullable=False),
])

cleaned_bronze = dlt.read("consumer_cleaned") \
                  .select(
                        col("consumer_id").cast(IntegerType()).alias("consumer_id"),
                        col("sex").cast(IntegerType()).alias("sex"),
                        col("ethinicity").cast(StringType()).alias("ethnicity"),"),
                        col("race").cast(IntegerType()).alias("race"),
                        col("age").cast(IntegerType()).alias("age")
                        )
                  
                  .withColumn("load_time", current_timestamp())
                  .withColumn("effective_from", current_date()) \
                  .withColumn("effective_to", lit('9999-12-31'))
                  
# Create the silver table using the defined schema
silver_latest_valid_records = spark.createDataFrame([], silver_schema)

silver_latest_valid_records = silver_latest_valid_records.union(cleaned_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC Merge with Prod Silver table. SCD type 2

# COMMAND ----------

from pyspark.sql.functions import current_date, lit, row_number, col
from pyspark.sql.window import Window

def upsert_to_silver_table(existing_silver_table: DataFrame, latest_batch: DataFrame) -> DataFrame:
    # Add surrogate key column to existing_silver_table
    existing_silver_table = existing_silver_table.withColumn("surrogate_key", row_number().over(Window.orderBy("consumer_id")))

    # Add surrogate key column to latest_batch
    latest_batch = latest_batch.withColumn("surrogate_key", lit(None))

    # Identify records to insert
    records_to_insert = latest_batch.join(existing_silver_table, ["consumer_id", "sex", "ethnicity", "race", "age", "load_time", "effective_from", "effective_to"], "left_anti")

    # Identify records to update
    records_to_update = latest_batch.join(existing_silver_table, ["consumer_id", "sex", "ethnicity", "race", "age", "load_time", "effective_from", "effective_to"], "inner")

    # Update the effective_to column of old records to yesterday's date
    updated_records = existing_silver_table.withColumn("effective_to", current_date() - lit(1))

    # Set the effective_from column of new records to today's date and the effective_to column to '9999-12-31'
    inserted_records = records_to_insert.withColumn("effective_from", current_date()) \
                                        .withColumn("effective_to", lit('9999-12-31'))

    # Union the updated and inserted records
    merged_table = updated_records.union(inserted_records)

    return merged_table

# Upsert the cleaned_bronze into the existing_silver_table
updated_silver_table = upsert_to_silver_table(prod_silver_table, silver_latest_valid_records)

# write to delta table
updated_silver_table.write.mode("overwrite").format("delta").saveAsTable("tendo.silver.consumer")
