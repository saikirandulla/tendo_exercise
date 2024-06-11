# Databricks notebook source
# MAGIC %md
# MAGIC Read the csv files from unity catalog into dataframes

# COMMAND ----------

consumer_df = spark.read.csv('/Volumes/sample_data/default/files/consumer_2024-06-02.csv', header=True, inferSchema=True)

# COMMAND ----------

consumer_columns = ["consumer_id", "sex", "ethnicity", "race", "age"]

# COMMAND ----------

consumer_df = consumer_df.toDF(*consumer_columns)


# COMMAND ----------

display(consumer_df)

# COMMAND ----------

purchase_df = spark.read.csv('/Volumes/sample_data/default/files/purchase_2024-06-02.csv', header=True, inferSchema=True)

# COMMAND ----------

purchase_columns = ["consumer_id", "purchase_id", "graphed_date", "avocado_bunch_id", "reporting_year", "qa_process", "billing_provider_sku",
                    "grocery_store_id", "price_index"]

# COMMAND ----------

purchase_df = purchase_df.toDF(*purchase_columns)


# COMMAND ----------

display(purchase_df)

# COMMAND ----------

avocado_df = spark.read.csv('/Volumes/sample_data/default/files/avocado_2024-06-02.csv', header=True, inferSchema=True)

# COMMAND ----------

avocado_columns = ["consumer_id", "purchase_id", "avocado_bunch_id", "plu", "ripe_index_when_picked" ,"born_date",
                   "picked_date", "sold_date"]

# COMMAND ----------

avocado_df = avocado_df.toDF(*avocado_columns)

# COMMAND ----------

display(avocado_df)

# COMMAND ----------

from pyspark.sql.functions import datediff
avocado_df = avocado_df.withColumn("days_picked", datediff(avocado_df["picked_date"], avocado_df["born_date"]))
avocado_df = avocado_df.withColumn("days_sold", datediff(avocado_df["sold_date"], avocado_df["picked_date"]))



# COMMAND ----------

display(avocado_df)

# COMMAND ----------



# COMMAND ----------

fertilizer_df = spark.read.csv('/Volumes/sample_data/default/files/fertilizer_2024-06-02.csv', header=True, inferSchema=True)

# COMMAND ----------

fertilizer_columns = ["purchase_id", "consumer_id", "fertilizer_id", "type", "mg" ,"frequency"]

# COMMAND ----------

fertilizer_df = fertilizer_df.toDF(*fertilizer_columns)

# COMMAND ----------

display(fertilizer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC consumer_id
# MAGIC Sex
# MAGIC age
# MAGIC avocado_days_sold
# MAGIC avocado_ripe_index
# MAGIC avacado_days_picked
# MAGIC fertilizer_type
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

purchase_df.select('consumer_id').distinct().orderBy("consumer_id").show()

# COMMAND ----------

avocado_df.select('consumer_id').distinct().orderBy("consumer_id").show()

# COMMAND ----------

fertilizer_df.select('consumer_id').distinct().orderBy("consumer_id").show()

# COMMAND ----------

consumer_df.select('consumer_id').distinct().orderBy("consumer_id").show()

# COMMAND ----------

output_df = purchase_df.join(avocado_df, on = ['consumer_id', 'purchase_id', 'avocado_bunch_id'], how='outer')\
                      .join(fertilizer_df, on = ['consumer_id', 'purchase_id'], how='outer')\
                      .join(consumer_df, on = ['consumer_id'], how='outer')\
                      .select('consumer_id', 'sex','age', 'days_sold', 'ripe_index_when_picked','days_picked', 'type' ).orderBy('consumer_id').distinct()

# COMMAND ----------

display(output_df)

# COMMAND ----------



# COMMAND ----------

path_table = 'tendo.default'
table_name = 'task_1_output'

# COMMAND ----------

output_df.write.mode("overwrite").saveAsTable(f"{path_table}" + "." + f"{table_name}")


# COMMAND ----------

output_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC select * from tendo.default.task_1_output

# COMMAND ----------


