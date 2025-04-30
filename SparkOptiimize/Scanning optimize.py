# Databricks notebook source
from pyspark.sql.functions import spark_partition_id, col
from pyspark.storagelevel import *

# COMMAND ----------

# MAGIC %md
# MAGIC **TURN OF AQE**

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled","false")

# COMMAND ----------

#check AQE status
spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC **DATA READING**

# COMMAND ----------

df = spark.read.format("csv")\
    .option("inferSchema", True)\
    .option("header", True)\
    .load("/FileStore/rawdata/walmart.csv")


# COMMAND ----------

display(df)
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC **Get no.of Partitions**

# COMMAND ----------

# by default no of partition done by spark based on the data size
df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC **REPARTITION**

# COMMAND ----------

df = df.repartition(10)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC **GET PARTITION INFO**

# COMMAND ----------

df.withColumn("partition_id",spark_partition_id()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Writing**

# COMMAND ----------

df.write.format("parquet")\
    .mode("append")\
    .option("path","/FileStore/rawdata/parquetWrite")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **New Data Reading**

# COMMAND ----------

newdf_read = spark.read.format("parquet")\
    .load("/FileStore/rawdata/parquetWrite")

#filtering based on the col value
newdf_read = newdf_read.filter(col("DEPARTMENT")=='Alcohol')

# COMMAND ----------

newdf_read.display()
newdf_read.count()

# COMMAND ----------

