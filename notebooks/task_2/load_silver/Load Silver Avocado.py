# Databricks notebook source
# MAGIC %md 
# MAGIC Read bronze table and get the latest batch of records

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

# MAGIC %md
# MAGIC - Add Dataquality checks to drop records that violate constraints and write to error table
# MAGIC - Cast to correct datatypes and prepare for silver merge
# MAGIC

# COMMAND ----------

@dlt.table(name="avocado_cleaned")
def read_avocado_bronze_data():
    avocado_bronze_data = dlt.read("avocado_bronze")
    
    # Add data quality expectations
    avocado_bronze_data.expect_or_drop("Non-null consumer_id", "col('consumer_id').isNotNull()")
    avocado_bronze_data.expect("Non-null purchase_id", "col('purchase_id').isNotNull()")

    # 7-8 months from the date of birth
    avocado_bronze_data.expect_or_drop("7-8 months of age when picked", "col('born_date') > date_sub(current_date(), 240)")
    #picked date should be after born date
    avocado_bronze_data.expect_or_drop("picked_date > born_date", "col('picked_date') > col('born_date')")
    #sold date should be after picked date
    avocado_bronze_data.expect_or_drop("sold_date > picked_date", "col('sold_date') > col('picked_date')")
    #data type matching
    avocado_bronze_data.expect_or_drop("consumer_id should be IntegerType", "col('consumer_id').cast(IntegerType()).isNotNull()")
    avocado_bronze_data.expect_or_drop("purchase_id should be IntegerType", "col('purchase_id').cast(IntegerType()).isNotNull()")
    avocado_bronze_data.expect_or_drop("avocado_bunch_id should be IntegerType", "col('avocado_bunch_id').cast(IntegerType()).isNotNull()")
    avocado_bronze_data.expect_or_drop("plu should be IntegerType", "col('plu').cast(IntegerType()).isNotNull()")
    avocado_bronze_data.expect_or_drop("ripe_index_when_picked should be IntegerType", "col('ripe_index_when_picked').cast(IntegerType()).isNotNull()")
    avocado_bronze_data.expect_or_drop("born_date should be DateType", "col('born_date').cast(DateType()).isNotNull()")
    avocado_bronze_data.expect_or_drop("picked_date should be DateType", "col('picked_date').cast(DateType()).isNotNull()")
    avocado_bronze_data.expect_or_drop("sold_date should be DateType", "col('sold_date').cast(DateType()).isNotNull()")

    error_table_name = "avocado_bronze_data_errors"
    dlt.create_table(error_table_name, avocado_bronze_data.schema)
    
    # Insert dropped records into the error table
    dropped_records = avocado_bronze_data.filter(~col("_dlt_is_valid"))
    dlt.write(error_table_name, dropped_records)
    return avocado_bronze_data

# COMMAND ----------

# MAGIC %md Define Schema for Silver table and 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, DateType, col

# Define the schema for the silver table
silver_schema = StructType([
    StructField("consumer_id", IntegerType(), nullable=True),
    StructField("purchase_id", IntegerType(), nullable=False),
    StructField("avocado_bunch_id", IntegerType(), nullable=True),
    StructField("plu", IntegerType(), nullable=True),
    StructField("ripe_index_when_picked", IntegerType(), nullable=True),
    StructField("born_date", DateType(), nullable=True),
    StructField("picked_date", DateType(), nullable=True),
    StructField("sold_date", DateType(), nullable=True),
    StructField("load_time", TimestampType(), nullable=False),
])

cleaned_bronze = dlt.read("avocado_cleaned") \
                  .select(
                        col("consumer_id").cast(IntegerType()).alias("consumer_id"),
                        col("purchase_id").cast(IntegerType()).alias("purchase_id"),
                        col("avocado_bunch_id").cast(IntegerType()).alias("avocado_bunch_id"),
                        col("plu").cast(IntegerType()).alias("plu"),
                        col("ripe_index_when_picked").cast(IntegerType()).alias("ripe_index_when_picked"),
                        col("born_date").cast(DateType()).alias("born_date"),
                        col("picked_date").cast(DateType()).alias("picked_date"),
                        col("sold_date").cast(DateType()).alias("sold_date")) \
                  .withColumn("load_time", current_timestamp())

# Create the silver table using the defined schema
silver_latest_valid_records = spark.createDataFrame([], silver_schema)

silver_latest_valid_records = silver_latest_valid_records.union(cleaned_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC Merge with Prod Silver table

# COMMAND ----------

from pyspark.sql import DataFrame

prod_silver_table = spark.read.table("tendo.silver.avocado")

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
updated_silver_table.write.mode("overwrite").format("delta").saveAsTable("tendo.silver.avocado")
