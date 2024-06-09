-- Databricks notebook source
-- MAGIC %md
-- MAGIC This is a notebook where you can write sql on the generated output from the sample files that the customer has provided.
-- MAGIC
-- MAGIC Below is a sample query to understand the table_name, schema of the table, and the data. 
-- MAGIC  
-- MAGIC The table name is _tendo.default.task_1_output_

-- COMMAND ----------

select * from tendo.default.task_1_output

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Write queries on the table as below. 

-- COMMAND ----------

select count(*) from tendo.default.task_1_output

-- COMMAND ----------


