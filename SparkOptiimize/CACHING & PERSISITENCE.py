# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.storagelevel import StorageLevel

# COMMAND ----------

df = spark.read.format("csv")\
        .option("inferSchema",True)\
        .option("header", True)\
        .load("/FileStore/rawdata/walmart.csv")\
        .cache()

# COMMAND ----------

df2 = df.filter(col("Department") == 'Snacks')

# COMMAND ----------

df3 = df.filter(col("Department") == 'Dairy & Eggs')

# COMMAND ----------

df3.display()

# COMMAND ----------

     # to remove from storing the df from cache/ persist

     df.unpersist()

# COMMAND ----------

   # to persist data in memory  

df.persist(StorageLevel.MEMORY_ONLY)