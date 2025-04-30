# Databricks notebook source
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

df_transactions.createOrReplaceTempView('transactions')
df_countries.createOrReplaceTempView('countries')

# COMMAND ----------


df_sql = spark.sql('''
select * 
from transactions t 
join countries c 
on t.country = c.country''')

# COMMAND ----------

df_sql.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **how to pass hints using '/*' sign in sq code**

# COMMAND ----------


df_sql_opt = spark.sql('''
select  /*+ broadcast(c) */             ---- provide hints to sql code
*
from transactions t 
join countries c 
on t.country = c.country''')

# COMMAND ----------

df_sql_opt.display()