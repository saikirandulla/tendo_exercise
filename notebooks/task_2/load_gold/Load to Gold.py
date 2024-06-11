# Databricks notebook source
silver_avocado_df = spark.read.table("tendo.silver.avocado")
silver_consumer_df = spark.read.table("tendo.silver.consumer")
silver_fertilizer_df = spark.read.table("tendo.silver.fertilizer")
silver_purchase_df = spark.read.table("tendo.silver.purchase")

# COMMAND ----------

get records with the latest load_timestamp from each of the above tables

# COMMAND ----------

from pyspark.sql.functions import max

# Get the latest load_timestamp from each table
silver_avocado_latest_timestamp = silver_avocado_df.select(max("load_timestamp")).first()[0]
silver_consumer_latest_timestamp = silver_consumer_df.select(max("load_timestamp")).first()[0]
silver_fertilizer_latest_timestamp = silver_fertilizer_df.select(max("load_timestamp")).first()[0]
silver_purchase_latest_timestamp = silver_purchase_df.select(max("load_timestamp")).first()[0]

# Filter the records with the latest load_timestamp
silver_avocado_latest_records = silver_avocado_df.filter(silver_avocado_df.load_timestamp == silver_avocado_latest_timestamp)
silver_consumer_latest_records = silver_consumer_df.filter(silver_consumer_df.load_timestamp == silver_consumer_latest_timestamp)
silver_fertilizer_latest_records = silver_fertilizer_df.filter(silver_fertilizer_df.load_timestamp == silver_fertilizer_latest_timestamp)
silver_purchase_latest_records = silver_purchase_df.filter(silver_purchase_df.load_timestamp == silver_purchase_latest_timestamp)


# COMMAND ----------

from pyspark.sql.functions import col

# Filter the DataFrames based on the latest load_timestamp
latest_purchase_df = purchase_df.filter(purchase_df.load_timestamp == purchase_df.selectExpr("max(load_timestamp)").collect()[0][0])
latest_avocado_df = avocado_df.filter(avocado_df.load_timestamp == avocado_df.selectExpr("max(load_timestamp)").collect()[0][0])
latest_fertilizer_df = fertilizer_df.filter(fertilizer_df.load_timestamp == fertilizer_df.selectExpr("max(load_timestamp)").collect()[0][0])
latest_consumer_df = consumer_df.filter(consumer_df.load_timestamp == consumer_df.selectExpr("max(load_timestamp)").collect()[0][0])

# Join the latest DataFrames and select the desired columns
output_df_latest = latest_purchase_df.join(latest_avocado_df, on=['consumer_id', 'purchase_id']) 
                              .join(latest_fertilizer_df, on=['consumer_id', 'purchase_id'], how='outer') 
                              .join(latest_consumer_df, on=['consumer_id'], how='outer') 
                              .select('consumer_id', 'sex', 'age', 'days_sold', 'ripe_index_when_picked', 'days_picked', 'type')
                              .dropDuplicates()
                              .withColumn('load_timestamp', current_timestamp()) 
                              .orderBy('consumer_id') \

# COMMAND ----------

from pyspark.sql import DataFrame

prod_gold_table = spark.read.table("tendo.gold.output")

# Perform the insert operation
def insert_to_gold_table(existing_gold_table: DataFrame, latest_batch: DataFrame) -> DataFrame:
    # Union the existing_gold_table with the latest_batch DataFrame
    inserted_table = existing_gold_table.union(latest_batch)

    return inserted_table

# Insert the latest_batch into the existing_gold_table
updated_gold_table = insert_to_gold_table(prod_gold_table, output_df_latest)

# write to delta table
updated_gold_table.write.mode("overwrite").format("delta").saveAsTable("tendo.gold.output")

# COMMAND ----------



