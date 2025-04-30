# Databricks notebook source
# MAGIC %md
# MAGIC **AQE**
# MAGIC
# MAGIC 1. Dynamically coalesce the partition
# MAGIC 2. Optimize the join strategy dusring run time
# MAGIC 3. Optimize skewness

# COMMAND ----------

# MAGIC %md
# MAGIC **Turn off AQE**

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled","false")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.format("csv")\
        .option("inferSchema", True)\
        .option("header", True)\
        .load("/FileStore/rawdata/walmart.csv")


# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.display()

# COMMAND ----------

df_new = df.groupBy("Department").count()

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Turn ON AQE**

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled","true")

# COMMAND ----------

# get confirmed whether the AQE is enabled

spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

df = spark.read.format("csv")\
        .option("inferSchema", True)\
        .option("header", True)\
        .load("/FileStore/rawdata/walmart.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df_new = df.groupBy("Department").count()

df_new.display()

# COMMAND ----------

