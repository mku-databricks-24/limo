# Databricks notebook source
print("hello")

# COMMAND ----------

# MAGIC %md
# MAGIC Spark core
# MAGIC
# MAGIC RDD Dataframe - Resilient distributed data
# MAGIC
# MAGIC DataFrame: Structures API

# COMMAND ----------

data = [(1, "a", 20), (2, "b", 30)]

# COMMAND ----------

## Table reside in a single location. Dataframe reside can reside in different cores

schema = ["id", "name", "age"]
df = spark.createDataFrame(data, schema) # Transformation -- lazy evaluation

df.show() # Action
display(df) # Action

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks Function
# MAGIC - select
# MAGIC vs
# MAGIC
# MAGIC Function
# MAGIC - col

# COMMAND ----------

df1 = df.select("id", "name")
display(df1)

# COMMAND ----------

from pyspark.sql.functions import *
df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

#help(df.withColumnRenamed)
df.withColumnRenamed("id", "emp_id").display()

# COMMAND ----------

#help(df.withColumnsRenamed)
df.withColumnsRenamed({"id": "emp_id", "name": "emp_name"}).display()

# COMMAND ----------

df.withColumn("current_date", current_date()).display()

# COMMAND ----------


