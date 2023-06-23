-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Address Configuration

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #################### input Path #################
-- MAGIC shaw_consumer_wireline_account = '/mnt/shawsaprod/shaw-snflk-dir-01/SHAW/20220407_CONSUMER_WIRELINE_ACCOUNT'
-- MAGIC shaw_consumer_contact = '/mnt/shawsaprod/shaw-snflk-dir-01/SHAW/20220407_CONSUMER_CONTACT'
-- MAGIC shaw_consumer_wireless_account = '/mnt/shawsaprod/shaw-snflk-dir-01/SHAW/20220407_CONSUMER_WIRELESS_ACCOUNT'
-- MAGIC shaw_consumer_wireless_service = '/mnt/shawsaprod/shaw-snflk-dir-01/SHAW/20220407_CONSUMER_WIRELESS_SERVICE'
-- MAGIC
-- MAGIC rogers_contact = '/mnt/rogerssaprod/deloitte-cr/rogers_0414/20220414_rogers_contact'
-- MAGIC rogers_wireless_account = '/mnt/rogerssaprod/deloitte-cr/rogers_0414/20220414_rogers_wireless_account'
-- MAGIC rogers_wireline_account = '/mnt/rogerssaprod/deloitte-cr/rogers_0414/20220414_rogers_wireline_account'
-- MAGIC rogers_wireless_service = '/mnt/rogerssaprod/deloitte-cr/rogers_0414/20220414_rogers_wireless_services'
-- MAGIC
-- MAGIC #Load dataframes
-- MAGIC shaw_consumer_wireless_account_df       = spark.read.format("parquet").load(shaw_consumer_wireless_account)
-- MAGIC shaw_consumer_wireline_account_df       = spark.read.format("parquet").load(shaw_consumer_wireline_account)
-- MAGIC shaw_consumer_contact_df                = spark.read.format("parquet").load(shaw_consumer_contact)
-- MAGIC shaw_consumer_wireless_service_df       = spark.read.format("parquet").load(shaw_consumer_wireless_service)
-- MAGIC
-- MAGIC rogers_contact_df                 = spark.read.format("parquet").load(rogers_contact)
-- MAGIC rogers_wireless_account_df        = spark.read.format("parquet").load(rogers_wireless_account)
-- MAGIC rogers_wireline_account_df        = spark.read.format("parquet").load(rogers_wireline_account)
-- MAGIC rogers_wireless_service_df        = spark.read.format("parquet").load(rogers_wireless_service)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC unified_path = "/mnt/development/Consumption/CCMM" 
-- MAGIC Contact_df = spark.read.format("parquet").load(unified_path + "/Contact")
-- MAGIC WirelessAccount_df = spark.read.format("parquet").load(unified_path + "/WirelessAccount")
-- MAGIC WirelessService_df = spark.read.format("parquet").load(unified_path + "/WirelessService")
-- MAGIC WirelineAccount_df = spark.read.format("parquet").load(unified_path + "/WirelineAccount")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Build Views

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #cleansed data
-- MAGIC shaw_consumer_wireless_account_df.createOrReplaceTempView("shaw_wireless")
-- MAGIC shaw_consumer_wireline_account_df.createOrReplaceTempView("shaw_wireline")
-- MAGIC shaw_consumer_contact_df.createOrReplaceTempView("shaw_contact")
-- MAGIC shaw_consumer_wireless_service_df.createOrReplaceTempView("shaw_wireless_service")
-- MAGIC
-- MAGIC               
-- MAGIC rogers_wireless_account_df.createOrReplaceTempView("rogers_wireless")        
-- MAGIC rogers_wireline_account_df.createOrReplaceTempView("rogers_wireline")
-- MAGIC rogers_contact_df.createOrReplaceTempView("rogers_contact")  
-- MAGIC rogers_wireless_service_df.createOrReplaceTempView("rogers_wireless_service")  
-- MAGIC
-- MAGIC
-- MAGIC #match output
-- MAGIC Contact_df.createOrReplaceTempView("unifiedContact")
-- MAGIC WirelessAccount_df.createOrReplaceTempView("unifiedWirelessAccount")
-- MAGIC WirelessService_df.createOrReplaceTempView("unifiedWirelessService")
-- MAGIC WirelineAccount_df.createOrReplaceTempView("unifiedWirelineAccount")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###TC-G-01 Table Structures

-- COMMAND ----------

select * from unifiedWirelessAccount limit 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###TC-G-02

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Row Count

-- COMMAND ----------

select "unified" as source, "Contact" as table_name, count(*) from unifiedContact
union
select "unified" as source, "WirelessAccount" as table_name, count(*) from unifiedWirelessAccount
union
select "unified" as source, "WirelessService" as table_name, count(*) from unifiedWirelessService
union
select "unified" as source, "WirelineAccount" as table_name, count(*) from unifiedWirelineAccount

-- COMMAND ----------

select "shaw" as source, "Contact" as table_name, count(*) from shaw_contact
union
select "shaw" as source, "WirelessAccount" as table_name, count(*) as count_rows from shaw_wireless
union
select "shaw" as source, "WirelessService" as table_name, count(*) as count_rows from shaw_wireless_service
union
select "shaw" as source, "WirelineAccount" as table_name, count(*) as count_rows from shaw_wireline

union
select "rogers" as source, "Contact" as table_name, count(*) as count_rows from rogers_contact
union
select "rogers" as source, "WirelessAccount" as table_name, count(*) as count_rows from rogers_wireless
union
select "rogers" as source, "WirelessService" as table_name, count(*) as count_rows from rogers_wireless_service
union
select "rogers" as source, "WirelineAccount" as table_name, count(*) as count_rows from rogers_wireline

-- COMMAND ----------

with shawAndRogers as
(
  select "shaw" as source, "Contact" as table_name, count(*) as count_rows from shaw_contact
  union
  select "shaw" as source, "WirelessAccount" as table_name, count(*) as count_rows from shaw_wireless
  union
  select "shaw" as source, "WirelessService" as table_name, count(*) as count_rows from shaw_wireless_service
  union
  select "shaw" as source, "WirelineAccount" as table_name, count(*) as count_rows from shaw_wireline

  union
  select "rogers" as source, "Contact" as table_name, count(*) as count_rows from rogers_contact
  union
  select "rogers" as source, "WirelessAccount" as table_name, count(*) as count_rows from rogers_wireless
  union
  select "rogers" as source, "WirelessService" as table_name, count(*) as count_rows from rogers_wireless_service
  union
  select "rogers" as source, "WirelineAccount" as table_name, count(*) as count_rows from rogers_wireline
)
select table_name, sum (count_rows)
from shawAndRogers
group by table_name
order by table_name

-- COMMAND ----------


