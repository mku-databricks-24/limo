# Databricks notebook source
df_sales = spark.table("sales")

# COMMAND ----------

df_customers = spark.table("customers")

# COMMAND ----------

df_joined = df_sales.join(df_customers, df_sales["customer_id"] == df_customers["customer_id"], "inner")

# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_customer = spark.table("customer")
df_customer.filter("customer_id=2").display()


# COMMAND ----------

from pyspark.sql.functions import *
df_customer.where(col('customer_id')==2).display()

# COMMAND ----------

df_customer.sort("customer_city", ascending=True).display()

# COMMAND ----------

df_customer.sort(col("customer_city").desc()).display()

# COMMAND ----------

df_joined.display()

# COMMAND ----------

(df_joined.groupBy("customer_name").count()).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales inner join
# MAGIC customer on sales.customer_id = customer.customer_id
