-- Databricks notebook source
select * from file_format.`path`

-- COMMAND ----------

select * from json.`/Volumes/limo_workspace/default/raw/customers.json`

-- COMMAND ----------

select *, current_timestamp() as cts from json.`/Volumes/limo_workspace/default/raw/customers.json`

-- COMMAND ----------

-- DBTITLE 1,CTAS
Create table customers as
select *, current_timestamp() as ingestion_date from json.`/Volumes/limo_workspace/default/raw/customers.json`


-- COMMAND ----------

Create table products as
select * from json.`/Volumes/limo_workspace/default/raw/products.json`

