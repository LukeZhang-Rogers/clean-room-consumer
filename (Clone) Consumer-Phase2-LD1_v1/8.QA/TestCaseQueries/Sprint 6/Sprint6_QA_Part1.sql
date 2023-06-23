-- Databricks notebook source
-- MAGIC %md
-- MAGIC #load data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #Load tables
-- MAGIC skinny_model = '/mnt/development/Processed/QA/Iteration1/CustomerMarketing'
-- MAGIC ruleset_path = '/mnt/development/Consumption/Ruleset'
-- MAGIC MatchedEntity = '/mnt/development/Processed/QA/Iteration6/MatchedEntity'
-- MAGIC ServiceabilityMatchedAccounts = '/mnt/development/Processed/QA/Iteration4/ServiceabilityMatchedAccounts'
-- MAGIC shaw_wireline_serviceability = '/mnt/development/Processed/QA/Iteration4/Shaw/WirelineServiceability'
-- MAGIC shaw_consumer_wireless_account = '/mnt/development/Processed/Shaw/WirelessAccount'
-- MAGIC shaw_consumer_wireline_account = '/mnt/development/Processed/Shaw/WirelineAccount' 
-- MAGIC shaw_consumer_wireless_service = '/mnt/development/Processed/Shaw/WirelessService'
-- MAGIC
-- MAGIC
-- MAGIC #Load dataframes
-- MAGIC shaw_consumer_wireless_account_df = spark.read.format("parquet").load(shaw_consumer_wireless_account)
-- MAGIC shaw_consumer_wireline_account_df = spark.read.format("parquet").load(shaw_consumer_wireline_account)
-- MAGIC shaw_consumer_wireless_service_df = spark.read.format("parquet").load(shaw_consumer_wireless_service)
-- MAGIC shaw_wireline_serviceability_df = spark.read.format("parquet").load(shaw_wireline_serviceability)
-- MAGIC ServiceabilityMatchedAccounts_df = spark.read.format("parquet").load(ServiceabilityMatchedAccounts)
-- MAGIC skinny_model_df = spark.read.format("parquet").load(skinny_model)
-- MAGIC ruleset_path_df = spark.read.format("parquet").load(ruleset_path)
-- MAGIC MatchedEntity_df = spark.read.format("parquet").load(MatchedEntity)
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC # rogers_wireless_account = '/mnt/development/Processed/Iteration8/Rogers/WirelessAccount'
-- MAGIC # rogers_wireline_account = "/mnt/development/Processed/Iteration8/Rogers/WirelineAccount"
-- MAGIC # AccountMatchedRuleset = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration4/AccountMatchedRuleset")
-- MAGIC # cl_shaw_consumer_contact_df                = spark.read.format("parquet").load(shaw_consumer_contact)
-- MAGIC # cl_rogers_contact_df                 = spark.read.format("parquet").load(rogers_contact)
-- MAGIC # cl_rogers_wireless_account_df = spark.read.format("parquet").load(rogers_wireless_account)
-- MAGIC # cl_rogers_wireline_account_df = spark.read.format("parquet").load(rogers_wireline_account)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #Build views
-- MAGIC shaw_consumer_wireless_account_df.createOrReplaceTempView("cl_shaw_wireless")
-- MAGIC shaw_consumer_wireline_account_df.createOrReplaceTempView("cl_shaw_wireline")
-- MAGIC shaw_consumer_wireless_service_df.createOrReplaceTempView("cl_shaw_service")      
-- MAGIC shaw_wireline_serviceability_df.createOrReplaceTempView("cl_shaw_wln_serviceability")        
-- MAGIC ServiceabilityMatchedAccounts_df.createOrReplaceTempView("ServiceabilityMatchedAccounts")
-- MAGIC skinny_model_df.createOrReplaceTempView("skinny_model")  
-- MAGIC ruleset_path_df.createOrReplaceTempView("ruleset")  
-- MAGIC MatchedEntity_df.createOrReplaceTempView("MatchedEntity")  

-- COMMAND ----------

select count(distinct ECID), count(distinct SHAW_MASTER_PARTY_ID), count(distinct ECID,SHAW_MASTER_PARTY_ID) from skinny_model

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #TC-F-01 table structure

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC # NOT NULLABLE columns names
-- MAGIC
-- MAGIC notNullable_columns = ['SHAW_WIRELINE_STATUS','SHAW_WIRELESS_STATUS','SHAW_INTERNET_FLAG','SHAW_CABLE_TV_FLAG','SHAW_LEGACY_TV_FLAG','SHAW_SATELLITE_TV_FLAG','SHAW_HP_FLAG','DET_MATCH_FLAG','DET_CUSTOMER_MATCH_FLAG','DET_ADDRESS_MATCH_FLAG','FUZZY_MATCH_FLAG','FUZZY_CUSTOMER_MATCH_FLAG','FUZZY_ADDRESS_MATCH_FLAG','PROB_MATCH_FLAG','PROB_CUSTOMER_MATCH_FLAG','PROB_ADDRESS_MATCH_FLAG','INT_SERVICEABILITY_FLAG','C_TV_SERVICEABILITY_FLAG','S_TV_SERVICEABILITY_FLAG','HP_SERVICEABILITY_FLAG']
-- MAGIC
-- MAGIC
-- MAGIC for i in notNullable_columns:
-- MAGIC     if skinny_model_df.filter(col(i).isNull()).count() != 0:
-- MAGIC         print(i)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC skinny_model_df.filter(col('SHAW_WIRELESS_STATUS').isNull()).count()

-- COMMAND ----------

select * from skinny_model
-- where SHAW_WIRELESS_STATUS is null

-- COMMAND ----------

describe skinny_model

-- COMMAND ----------

select * from skinny_model
where ECID is null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #TC-F-02 Customer ID

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Matched Entity

-- COMMAND ----------

select * from MatchedEntity
where DET_MATCH_IND is null or FUZZY_MATCH_IND is null or PROB_MATCH_IND is null
-- DET_MATCH_IND = 'Y' 
-- and FUZZY_MATCH_IND = 'Y'
-- and PROB_MATCH_IND = 'Y'

-- COMMAND ----------

select DET_MATCH_IND, count(distinct ROGERS_ECID),count(distinct SHAW_MASTER_PARTY_ID),count(distinct ROGERS_ECID,SHAW_MASTER_PARTY_ID) 
from MatchedEntity
group by DET_MATCH_IND

-- COMMAND ----------

select FUZZY_MATCH_IND, count(distinct ROGERS_ECID),count(distinct SHAW_MASTER_PARTY_ID),count(distinct ROGERS_ECID,SHAW_MASTER_PARTY_ID) 
from MatchedEntity
group by FUZZY_MATCH_IND

-- COMMAND ----------

select PROB_MATCH_IND, count(distinct ROGERS_ECID),count(distinct SHAW_MASTER_PARTY_ID),count(distinct ROGERS_ECID,SHAW_MASTER_PARTY_ID) 
from MatchedEntity
group by PROB_MATCH_IND

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##skinny model

-- COMMAND ----------

select DET_MATCH_FLAG, count(distinct ECID),count(distinct SHAW_MASTER_PARTY_ID),count(distinct ECID,SHAW_MASTER_PARTY_ID) 
from skinny_model
group by DET_MATCH_FLAG

-- COMMAND ----------

select FUZZY_MATCH_FLAG, count(distinct ECID),count(distinct SHAW_MASTER_PARTY_ID),count(distinct ECID,SHAW_MASTER_PARTY_ID) 
from skinny_model
group by FUZZY_MATCH_FLAG

-- COMMAND ----------

select PROB_MATCH_FLAG, count(distinct ECID),count(distinct SHAW_MASTER_PARTY_ID),count(distinct ECID,SHAW_MASTER_PARTY_ID) 
from skinny_model
group by PROB_MATCH_FLAG

-- COMMAND ----------

select DET_MATCH_FLAG,FUZZY_MATCH_FLAG,PROB_MATCH_FLAG, count(distinct ECID),count(distinct SHAW_MASTER_PARTY_ID),count(distinct ECID,SHAW_MASTER_PARTY_ID) 
from skinny_model
group by DET_MATCH_FLAG,FUZZY_MATCH_FLAG,PROB_MATCH_FLAG

-- COMMAND ----------

select DET_MATCH_IND,FUZZY_MATCH_IND,PROB_MATCH_IND, count(distinct ROGERS_ECID),count(distinct SHAW_MASTER_PARTY_ID),count(distinct ROGERS_ECID,SHAW_MASTER_PARTY_ID) 
from MatchedEntity
group by DET_MATCH_IND,FUZZY_MATCH_IND,PROB_MATCH_IND

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #TC-F-08 Wireline Account Status

-- COMMAND ----------

select distinct source_system from cl_shaw_wireline

-- COMMAND ----------

-- skinny model

with skinny_shaw_wireline 
as (

  select ECID, sk.SHAW_MASTER_PARTY_ID, wl.source_system, wl.account_status,
--    sk.SHAW_INTERNET_FLAG, sk.SHAW_INTERNET_DEACT_DT, wl.internet_flag, wl.internet_disconnect_flag, wl.internet_disconnect_date
--    sk.SHAW_CABLE_TV_FLAG, sk.SHAW_CABLE_DEACT_DT, wl.video_delivery_type, wl.video_disconnect_flag, wl.video_disconnect_date -- video_delivery_type = 'CABLE', source_system = 'SBO'
--    sk.SHAW_LEGACY_TV_FLAG, sk.SHAW_LEGACY_TV_DEACT_DT, wl.video_delivery_type, wl.video_disconnect_flag, wl.video_disconnect_date -- video_delivery_type = 'CABLE', source_system = 'CBS'
--    sk.SHAW_SATELLITE_TV_FLAG, sk.SHAW_SATELLITE_TV_DEACT_DT, wl.video_delivery_type, wl.video_disconnect_flag, wl.video_disconnect_date -- video_delivery_type = 'SATELLITE'
   sk.SHAW_HP_FLAG, sk.SHAW_HP_DEACT_DT, wl.wireline_phone_flag, wl.wireline_phone_disconnect_flag, wl.wireline_phone_disconnect_date
 
  from skinny_model sk
  left join cl_shaw_wireline wl on wl.rcis_id_cl = sk.SHAW_MASTER_PARTY_ID
  where ECID is not null and SHAW_MASTER_PARTY_ID is not null and source_system in ('CBS', 'SBO')
)

select * from skinny_shaw_wireline
-- where video_delivery_type = 'CABLE' and source_system = 'SBO'
-- where video_delivery_type = 'CABLE' and source_system = 'CBS'
-- where video_delivery_type = 'SATELLITE'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #TC-F-09 Wireline Account Status

-- COMMAND ----------

-- select distinct SHAW_WIRELINE_STATUS from skinny_model
select distinct account_status from cl_shaw_wireline

-- COMMAND ----------

with skinny_joined_shaw_wireline
as (
select sm.SHAW_MASTER_PARTY_ID, sm.SHAW_WIRELINE_STATUS, sm.SHAW_WIRELINE_DEACT_DT, wl.source_system, 
wl.account_status, wl.VIDEO_DISCONNECT_DATE, wl.WIRELINE_PHONE_DISCONNECT_DATE, wl.INTERNET_DISCONNECT_DATE
from skinny_model sm
left join cl_shaw_wireline wl on wl.rcis_id_cl = sm.SHAW_MASTER_PARTY_ID
where source_system in ('CBS', 'SBO')
)
select count(*) as total_count,
count(case when SHAW_WIRELINE_STATUS = 'ACTIVE' then 1 else 0 end) as active_total_count,
count(case when SHAW_WIRELINE_STATUS = 'ACTIVE' and SHAW_WIRELINE_DEACT_DT is NULL then 1 else 0 end) as active_right_count
from skinny_joined_shaw_wireline

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #TC-F-10 Wireless Account Status

-- COMMAND ----------

-- select distinct SHAW_WIRELESS_STATUS from skinny_model
select distinct account_status from cl_shaw_wireless

-- COMMAND ----------

with skinny_joined_shaw_wireless
as (
select sm.SHAW_MASTER_PARTY_ID, sm.SHAW_WIRELESS_STATUS, sm.SHAW_WIRELESS_DEACT_DT, ws.source_system, ws.account_status 
from skinny_model sm
left join cl_shaw_wireless ws on ws.rcis_id_cl = sm.SHAW_MASTER_PARTY_ID
where source_system = 'SV'
)

-- counts
-- select count(*) as total_count,
-- count(case when SHAW_WIRELESS_STATUS = 'Active' or SHAW_WIRELESS_STATUS = 'Suspended' or SHAW_WIRELESS_STATUS = 'No Match' then 1 else 0 end) as except_inactive_total_count,
-- count(case when SHAW_WIRELESS_STATUS = 'Active' or SHAW_WIRELESS_STATUS = 'Suspended' or SHAW_WIRELESS_STATUS = 'No Match' and SHAW_WIRELESS_DEACT_DT is NULL then 1 else 0 end) as except_inactive_total_right_count
-- from skinny_joined_shaw_wireless

select distinct SHAW_WIRELESS_DEACT_DT from skinny_joined_shaw_wireless
where SHAW_WIRELESS_STATUS = 'Cancelled'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #TC-F-12 Matched Entity - DETER

-- COMMAND ----------

select * from ruleset

-- COMMAND ----------

select * from MatchedEntity
 

-- COMMAND ----------

-- Deterministic matched accounts total count 

select count(distinct ECID, sk.SHAW_MASTER_PARTY_ID) from skinny_model sk
left join MatchedEntity m on m.SHAW_MASTER_PARTY_ID = sk.SHAW_MASTER_PARTY_ID and m.ROGERS_ECID = sk.ECID
where DET_MATCH_FLAG = 'Y'

-- COMMAND ----------

-- check ruleset_id, customer match flag, address match flag, match_priority values

with sk_deter 
as (
  select 
  -- count(*), count(distinct ECID, sk.SHAW_MASTER_PARTY_ID)
  sk.ECID, sk.SHAW_MASTER_PARTY_ID, 
  m.BEST_DET_MATCH_RULESET_ID, r.RULESET_ID, sk.DET_MATCH_RULESET_ID,
  r.RULESET_PRIORITY, sk.DET_MATCH_PRIORITY,
  r.IS_CUSTOMER_MATCH, sk.DET_CUSTOMER_MATCH_FLAG,
  r.IS_ADDRESS_MATCH, sk.DET_ADDRESS_MATCH_FLAG
  from skinny_model sk
  left join MatchedEntity m on m.SHAW_MASTER_PARTY_ID = sk.SHAW_MASTER_PARTY_ID and m.ROGERS_ECID = sk.ECID
  left join ruleset r on r.RULESET_ID = sk.DET_MATCH_RULESET_ID
  where DET_MATCH_FLAG = 'Y'
)

-- 1. check ruleset_id
-- select count(distinct ECID, SHAW_MASTER_PARTY_ID) from sk_deter
-- where  DET_MATCH_RULESET_ID == RULESET_ID

-- 2. check customer match and address match flags
select count(case when IS_CUSTOMER_MATCH = 'TRUE' then 1 else 0 end) as sk_det_customer_match_count,
count(case when DET_CUSTOMER_MATCH_FLAG = 'Y' then 1 else 0 end)  as ruleset_is_customer_match_count,
count(case when IS_ADDRESS_MATCH = 'TRUE' then 1 else 0 end) as sk_det_address_match_count,
count(case when DET_ADDRESS_MATCH_FLAG = 'Y' then 1 else 0 end)  as ruleset_is_address_match_count,
count(case when IS_CUSTOMER_MATCH = 'TRUE' and DET_CUSTOMER_MATCH_FLAG = 'Y' then 1 else 0 end) as deter_customer_match_count,
count(case when IS_ADDRESS_MATCH = 'TRUE' and DET_ADDRESS_MATCH_FLAG = 'Y' then 1 else 0 end)  as deter_address_match_count
from sk_deter
 
-- 3. check ruleset priority and values
-- select count(distinct ECID, SHAW_MASTER_PARTY_ID) from sk_deter
-- where RULESET_PRIORITY != DET_MATCH_PRIORITY

-- select distinct DET_MATCH_PRIORITY from sk_deter

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #TC-F-13 Matched Entity - FUZZY

-- COMMAND ----------

-- Deterministic matched accounts total count 

select count(distinct ECID, sk.SHAW_MASTER_PARTY_ID) from skinny_model sk
left join MatchedEntity m on m.SHAW_MASTER_PARTY_ID = sk.SHAW_MASTER_PARTY_ID and m.ROGERS_ECID = sk.ECID
where FUZZY_MATCH_FLAG = 'Y'

-- COMMAND ----------

-- check ruleset_id, customer match flag, address match flag, match_priority values
-- with fuzzy_score as(

with sk_fuzzy
as (
  select 
  -- count(*), count(distinct ECID, sk.SHAW_MASTER_PARTY_ID)
  sk.ECID, sk.SHAW_MASTER_PARTY_ID, 
  m.BEST_FUZZY_MATCH_RULESET_ID, r.RULESET_ID, sk.FUZZY_MATCH_RULESET_ID,
  m.CUSTOMER_FUZZY_SCORE, sk.FUZZY_MATCH_SCORE,
  r.IS_CUSTOMER_MATCH, sk.FUZZY_CUSTOMER_MATCH_FLAG,
  r.IS_ADDRESS_MATCH, sk.FUZZY_ADDRESS_MATCH_FLAG
  from skinny_model sk
  left join MatchedEntity m on m.SHAW_MASTER_PARTY_ID = sk.SHAW_MASTER_PARTY_ID and m.ROGERS_ECID = sk.ECID
  left join ruleset r on r.RULESET_ID = sk.FUZZY_MATCH_RULESET_ID
  where FUZZY_MATCH_FLAG = 'Y'
)

-- select * from sk_fuzzy
-- select count(distinct ECID, SHAW_MASTER_PARTY_ID) from sk_fuzzy

-- 1. check ruleset_id
-- select count(distinct ECID, SHAW_MASTER_PARTY_ID) from sk_fuzzy
-- where FUZZY_MATCH_RULESET_ID == RULESET_ID

-- 2. check customer match and address match flags
select count(case when IS_CUSTOMER_MATCH = 'TRUE' then 1 else 0 end) as sk_det_customer_match_count,
count(case when FUZZY_CUSTOMER_MATCH_FLAG = 'Y' then 1 else 0 end)  as ruleset_is_customer_match_count,
count(case when IS_ADDRESS_MATCH = 'TRUE' then 1 else 0 end) as sk_det_address_match_count,
count(case when FUZZY_ADDRESS_MATCH_FLAG = 'Y' then 1 else 0 end)  as ruleset_is_address_match_count,
count(case when IS_CUSTOMER_MATCH = 'TRUE' and FUZZY_CUSTOMER_MATCH_FLAG = 'Y' then 1 else 0 end) as fuzzy_customer_match_count,
count(case when IS_ADDRESS_MATCH = 'TRUE' and FUZZY_ADDRESS_MATCH_FLAG = 'Y' then 1 else 0 end)  as fuzzy_address_match_count
from sk_fuzzy
 
-- 3. check ruleset priority and values
-- select max(FUZZY_MATCH_SCORE) as sk_score, max(CUSTOMER_FUZZY_SCORE) as me_score from sk_fuzzy
-- group by ECID, SHAW_MASTER_PARTY_ID
-- )
-- select * from fuzzy_score
-- where sk_score != me_score


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #TC-F-14 Matched Entity - PROB

-- COMMAND ----------

-- Deterministic matched accounts total count 

select count(distinct ECID, sk.SHAW_MASTER_PARTY_ID) from skinny_model sk
left join MatchedEntity m on m.SHAW_MASTER_PARTY_ID = sk.SHAW_MASTER_PARTY_ID and m.ROGERS_ECID = sk.ECID
where PROB_MATCH_FLAG = 'Y'

-- COMMAND ----------

-- check ruleset_id, customer match flag, address match flag, match_priority values

-- with prob_score as (

with sk_prob
as (
  select 
--   count(*), count(distinct ECID, sk.SHAW_MASTER_PARTY_ID)
  sk.ECID, sk.SHAW_MASTER_PARTY_ID, 
  m.BEST_PROB_MATCH_RULESET_ID, r.RULESET_ID, sk.PROB_MATCH_RULESET_ID,
  m.CUSTOMER_PROB_CONFIDENCE_LVL, sk.PROB_MATCH_CONFIDENCE_LVL,
  r.IS_CUSTOMER_MATCH, sk.PROB_CUSTOMER_MATCH_FLAG,
  r.IS_ADDRESS_MATCH, sk.PROB_ADDRESS_MATCH_FLAG
  from skinny_model sk
  left join MatchedEntity m on m.SHAW_MASTER_PARTY_ID = sk.SHAW_MASTER_PARTY_ID and m.ROGERS_ECID = sk.ECID
  left join ruleset r on r.RULESET_ID = sk.PROB_MATCH_RULESET_ID
  where PROB_MATCH_FLAG = 'Y'
)

-- select * from sk_prob

-- 1. check ruleset_id
-- select count(distinct ECID, SHAW_MASTER_PARTY_ID) from sk_prob
-- where PROB_MATCH_RULESET_ID == RULESET_ID

-- 2. check customer match and address match flags
select count(case when IS_CUSTOMER_MATCH = 'TRUE' then 1 else 0 end) as sk_prob_customer_match_count,
count(case when PROB_CUSTOMER_MATCH_FLAG = 'Y' then 1 else 0 end)  as ruleset_is_customer_match_count,
count(case when IS_ADDRESS_MATCH = 'TRUE' then 1 else 0 end) as sk_prob_address_match_count,
count(case when PROB_ADDRESS_MATCH_FLAG = 'Y' then 1 else 0 end)  as ruleset_is_address_match_count,
count(case when IS_CUSTOMER_MATCH = 'TRUE' and PROB_ADDRESS_MATCH_FLAG = 'Y' then 1 else 0 end) as prob_customer_match_count,
count(case when IS_ADDRESS_MATCH = 'TRUE' and PROB_CUSTOMER_MATCH_FLAG = 'Y' then 1 else 0 end)  as prob_address_match_count
from sk_prob
 
-- 3. check ruleset priority and values
-- select ECID, SHAW_MASTER_PARTY_ID,PROB_MATCH_CONFIDENCE_LVL from sk_prob
-- select max(PROB_MATCH_CONFIDENCE_LVL) as sk_score, max(CUSTOMER_PROB_CONFIDENCE_LVL) as me_score from sk_prob
-- group by ECID, SHAW_MASTER_PARTY_ID
-- )
-- select * from prob_score
-- where sk_score != me_score 


-- COMMAND ----------


