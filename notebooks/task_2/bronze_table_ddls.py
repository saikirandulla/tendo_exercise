# Databricks notebook source

avocado_mnt = 's3://tendo-customer-data/avocado'
consumer_mnt = 's3://tendo-customer-data/consumer'
purchase_mnt = 's3://tendo-customer-data/purchase'
fertilizer_mnt = 's3://tendo-customer-data/fertilizer'

# COMMAND ----------

avocado_df = (spark.readStream \
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.maxFilesPerTrigger", "1")  #demo only, remove in real stream
                .option("header", "true")
                .option("inferSchema", "true")
                .option("cloudFiles.schemaLocation", "/avocado_mnt/schemas") 
                .load(avocado_mnt))

# COMMAND ----------

display(avocado_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Create tables 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE tendo.default.consumer
# MAGIC (
# MAGIC   consumer_id INT NOT NULL ,
# MAGIC   purchase_id INT,
# MAGIC   graphed_date TIMESTAMP,
# MAGIC   avocado_bunch_id INT,
# MAGIC   reporting_year INT,
# MAGIC   QA_process STRING,
# MAGIC   billing_provider_sku INT,
# MAGIC   grocery_store_id INT,
# MAGIC   price_index INT,
# MAGIC   CONSTRAINT consumer_pk PRIMARY KEY(consumer_id)
# MAGIC );
