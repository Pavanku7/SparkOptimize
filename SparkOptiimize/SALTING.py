# Databricks notebook source
# MAGIC %md
# MAGIC **SALTING** : 
# MAGIC salting is a technique were you are breaking down the bigger chunk of partition into some smaller partition based on some random #s/un-officially is called salting

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Create a Dataframe**

# COMMAND ----------

data = [("A", 100),("A", 200), ("A", 300), ("B",150),("C", 50)]

df = spark.createDataFrame(data, ["id","purchase"])

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("salt_column", floor(rand()*3))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Create concat column on original groupBy col and salt_column to create a new groupBy col**

# COMMAND ----------

 df = df.withColumn("user_id_salt", concat(col("id"),lit("-"),col("salt_column")))


 df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Applying groupBy to new col**

# COMMAND ----------

df = df.groupBy("user_id_salt").agg(sum("purchase"))

df.display()

# COMMAND ----------

