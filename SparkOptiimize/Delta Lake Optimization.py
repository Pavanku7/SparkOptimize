# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA TEST_DB

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table TEST_DB.demo
# MAGIC (
# MAGIC   id int,
# MAGIC   name varchar(10)
# MAGIC )
# MAGIC
# MAGIC using Delta
# MAGIC location '/FileStore/rawdata/deltatbl'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into test_db.demo
# MAGIC values
# MAGIC (9, "qwe"),
# MAGIC (10, "zxc")

# COMMAND ----------

# MAGIC %md
# MAGIC **If we are reading the files numerous of time from table , it will be huge drawback as the files in delta location will keep on increasing based on the operation performed on the table**

# COMMAND ----------

# MAGIC %sql
# MAGIC Optimize delta.`/FileStore/rawdata/deltatbl` Zorder by (id)

# COMMAND ----------

df = spark.read.format("delta").load("/FileStore/rawdata/deltatbl")
df.printSchema()