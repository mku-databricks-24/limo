# Databricks notebook source
# MAGIC %md
# MAGIC #E
# MAGIC data format - csv, table, json, parquet, delta, avro, orc
# MAGIC (ADLS, Databases, DW, S3)
# MAGIC - PySpark DF
# MAGIC - Spark SQL
# MAGIC
# MAGIC #T
# MAGIC
# MAGIC #L
# MAGIC csv, json, parquet, delta
# MAGIC

# COMMAND ----------

df = spark.read.csv("/Volumes/limo_workspace/default/raw/sales.csv", header=True, inferSchema=True)

# COMMAND ----------

df.display()
df.write.saveAsTable("sales")

# COMMAND ----------

df_customers = spark.read.json("/Volumes/limo_workspace/default/raw/customers.json")

# COMMAND ----------

df_customers.display()

# COMMAND ----------

df_customers.write.saveAsTable("customer")

# COMMAND ----------

df_sales = spark.read.csv("/Volumes/limo_workspace/default/raw/sales.csv", header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %run 
# MAGIC
# MAGIC /Workspace/Users/mku.databricks.24@outlook.com/Day1/Includes
# MAGIC  

# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}sales.csv",header=True,inferSchema=True)
 
df1=add_ingestion(df_sales)
df1.write.mode("overwrite").saveAsTable("sales")
 

# COMMAND ----------

df_order_dates=spark.read.csv(f"{input_path}order_dates.csv",header=True,inferSchema=True)
 
df_order_dates=add_ingestion(df_order_dates)
df_order_dates.write.mode("overwrite").saveAsTable("order_dates")
 
