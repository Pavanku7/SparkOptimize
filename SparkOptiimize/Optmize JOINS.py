# Databricks notebook source
# MAGIC %md
# MAGIC **JOIN OPTIMIZE**

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled","false")

# COMMAND ----------

# Big dataframe
df_transactions = spark.createDataFrame([
    (1, "US", 100),
    (2, "IN", 200),
    (3 ,"RU", 150),
    (4, "CN", 80),
    (5, "US", 50),
], ["id","country","amount"])
                                        
# Small dataframe
df_countries = spark.createDataFrame([
    ("US", "United states"),
    ("IN", "India"),
    ("RU", "Russia"),
    ("CN", "China"),
], ["country","country_name"])


# COMMAND ----------

df_transactions.display()
df_transactions.describe()

# COMMAND ----------

df_countries.display()

# COMMAND ----------

df_join = df_transactions.join(df_countries, df_transactions['country']==df_countries['country'],'inner')

# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Optimize Join**

# COMMAND ----------

df_join_optim = df_transactions.join(broadcast(df_countries), df_transactions['country']==df_countries['country'],'inner')

# COMMAND ----------

df_join_optim.display()

# COMMAND ----------

