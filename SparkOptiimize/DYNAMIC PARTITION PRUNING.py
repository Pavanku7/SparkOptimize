# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC **Turn Off AQE, DPP & Autobroadcast**

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled","false")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled","false")
spark.conf.set("spark.conf.autoBroadcastJoinThresold", -1)

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Read data**

# COMMAND ----------

df = spark.read.format("csv")\
            .option("header",True)\
            .option("inferSchema", True)\
            .load("/FileStore/rawdata/walmart.csv")


df = df.limit(100)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing the partiton data**

# COMMAND ----------

df.write.format("parquet")\
        .mode("append")\
        .partitionBy("Department")\
        .option("path","/FileStore/rawdata/dpp_partioned")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Non Partitioned Data**

# COMMAND ----------

df.write.format("parquet")\
        .mode("append")\
        .option("path","/FileStore/rawdata/dpp_nonpartioned")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Dataframes**

# COMMAND ----------

df1 = spark.read.format("parquet")\
        .load("/FileStore/rawdata/dpp_partioned")

# COMMAND ----------

df2 = spark.read.format("parquet")\
        .load("/FileStore/rawdata/dpp_nonpartioned")

# COMMAND ----------

# MAGIC %md
# MAGIC **JOINS**

# COMMAND ----------

df_joins = df1.join(df2.filter(col("Department")=="Beverages"), df1['tid']== df2['tid'], "inner")

# COMMAND ----------

df_joins.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Turn ON AQE, DPP & Autobroadcast**

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled","true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled","true")
spark.conf.set("spark.conf.autoBroadcastJoinThresold", 5 * 1024 * 1024)

from pyspark.sql.functions import *

# COMMAND ----------

df.write.format("parquet")\
        .mode("append")\
        .partitionBy("tid")\
        .option("path","/FileStore/rawdata/dpp_partioneNew")\
        .save()

# COMMAND ----------

df1 = spark.read.format("parquet")\
        .load("/FileStore/rawdata/dpp_partioneNew")

# COMMAND ----------

df2 = spark.read.format("parquet")\
        .load("/FileStore/rawdata/dpp_nonpartioned")

# COMMAND ----------

df_joins = df1.join(df2.filter(col("tid")=="16164408"), df1['tid']== df2['tid'], "inner")

# COMMAND ----------

df_joins.display()

# COMMAND ----------

