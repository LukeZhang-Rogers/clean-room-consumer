-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Address Configuration

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #Load dataframes
-- MAGIC AccountMatchedRuleset = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration5/AccountMatchedRuleset")
-- MAGIC
-- MAGIC MatchedEntity = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration6/MatchedEntity")
-- MAGIC rulesets = spark.read.format("parquet").load("/mnt/development/Consumption/Ruleset")
-- MAGIC Shaw_wln_serviceability = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration4/Shaw/WirelineServiceability")
-- MAGIC ServiceabilityMatchedAccounts = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration4/ServiceabilityMatchedAccounts")
-- MAGIC
-- MAGIC shaw_consumer_wireless_account = spark.read.format("parquet").load('/mnt/development/Processed/Shaw/WirelessAccount')
-- MAGIC shaw_consumer_wireline_account = spark.read.format("parquet").load('/mnt/development/Processed/Shaw/WirelineAccount')
-- MAGIC shaw_consumer_wireless_service = spark.read.format("parquet").load('/mnt/development/Processed/Shaw/WirelessService')
-- MAGIC SkinnyModel = spark.read.format("parquet").load('/mnt/development/Processed/QA/Iteration1/CustomerMarketing')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Build Views

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #cleansed data
-- MAGIC #cl_shaw_consumer_wireless_account_df.createOrReplaceTempView("cl_shaw_wireless")
-- MAGIC #cl_shaw_consumer_wireline_account_df.createOrReplaceTempView("cl_shaw_wireline")
-- MAGIC #cl_shaw_consumer_contact_df.createOrReplaceTempView("cl_shaw_contact")
-- MAGIC               
-- MAGIC #cl_rogers_wireless_account_df.createOrReplaceTempView("cl_rogers_wireless")        
-- MAGIC #cl_rogers_wireline_account_df.createOrReplaceTempView("cl_rogers_wireline")
-- MAGIC #cl_rogers_contact_df.createOrReplaceTempView("cl_rogers_contact")  
-- MAGIC
-- MAGIC
-- MAGIC #match output
-- MAGIC AccountMatchedRuleset.createOrReplaceTempView("matchedAccountView")
-- MAGIC MatchedEntity.createOrReplaceTempView("matchedEntityView")
-- MAGIC rulesets.createOrReplaceTempView("rulesetsView")
-- MAGIC ServiceabilityMatchedAccounts.createOrReplaceTempView("serviceabilityMatchedAccountView")
-- MAGIC
-- MAGIC Shaw_wln_serviceability.createOrReplaceTempView("ShawWlnServiceabilityView")
-- MAGIC shaw_consumer_wireless_account.createOrReplaceTempView("shawConsumerWirelessAccount")
-- MAGIC shaw_consumer_wireline_account.createOrReplaceTempView("shawConsumerWirelineAccount")
-- MAGIC shaw_consumer_wireless_service.createOrReplaceTempView("shawConsumerWirelessService")
-- MAGIC SkinnyModel.createOrReplaceTempView("customerMarketingView")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sprint 6

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-F-03

-- COMMAND ----------

Select distinct c.ECID, sma.RCIS_ID,
        sma.FA_ID, c.SHAW_MASTER_PARTY_ID,            
        sma.SERVICEABILITY_SOURCE,c.INT_SERVICEABILITY_FLAG, swln.int_serviceability_flag,
        swln.address_id ,sma.SERVICEABILITY_ID_MAILING_ADDRESS,  sma.SERVICEABILITY_ID_SERVICE_ADDRESS 
        from customerMarketingView c
  INNER JOIN serviceabilityMatchedAccountView sma ON c.ECID = sma.RCIS_ID
  INNER JOIN ShawWlnServiceabilityView swln ON swln.address_id = sma.SERVICEABILITY_ID_MAILING_ADDRESS
  INNER JOIN ShawWlnServiceabilityView swln2 ON swln2.address_id = sma.SERVICEABILITY_ID_SERVICE_ADDRESS
  where sma.SERVICEABILITY_SOURCE = 'Shaw'

-- COMMAND ----------

select * from ShawWlnServiceabilityView

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-F-04

-- COMMAND ----------

Select distinct c.ECID, sma.RCIS_ID,
        sma.FA_ID, c.SHAW_MASTER_PARTY_ID,            
        sma.SERVICEABILITY_SOURCE,c.C_TV_SERVICEABILITY_FLAG,swln.tv_service_type,
        swln.address_id ,sma.SERVICEABILITY_ID_MAILING_ADDRESS,  sma.SERVICEABILITY_ID_SERVICE_ADDRESS 
        from customerMarketingView c
  INNER JOIN serviceabilityMatchedAccountView sma ON c.ECID = sma.RCIS_ID
  INNER JOIN ShawWlnServiceabilityView swln ON sma.SERVICEABILITY_ID_MAILING_ADDRESS = swln.address_id  
  INNER JOIN ShawWlnServiceabilityView swln2 ON swln2.address_id = sma.SERVICEABILITY_ID_SERVICE_ADDRESS
  where sma.SERVICEABILITY_SOURCE = 'Shaw'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-F-05

-- COMMAND ----------

Select c.ECID,sma.RCIS_ID,
  sma.FA_ID, c.SHAW_MASTER_PARTY_ID,            
        sma.SERVICEABILITY_SOURCE,
        c.C_TV_SERVICEABILITY_TYPE,swln.tv_service_type,
        swln.address_id ,sma.SERVICEABILITY_ID_MAILING_ADDRESS,  sma.SERVICEABILITY_ID_SERVICE_ADDRESS 
               from customerMarketingView c
  INNER JOIN serviceabilityMatchedAccountView sma ON c.ECID = sma.RCIS_ID
  INNER JOIN ShawWlnServiceabilityView swln ON swln.address_id = sma.SERVICEABILITY_ID_MAILING_ADDRESS
  OR swln.address_id = sma.SERVICEABILITY_ID_SERVICE_ADDRESS
  where sma.SERVICEABILITY_SOURCE = 'Shaw' group by c.ECID,sma.RCIS_ID,
  sma.FA_ID, c.SHAW_MASTER_PARTY_ID,            
        sma.SERVICEABILITY_SOURCE,
        c.C_TV_SERVICEABILITY_TYPE,swln.tv_service_type,
        swln.address_id ,sma.SERVICEABILITY_ID_MAILING_ADDRESS,  sma.SERVICEABILITY_ID_SERVICE_ADDRESS order by swln.tv_service_type

-- COMMAND ----------

Select sma.RCIS_ID,
        sma.FA_ID,sma.SERVICEABILITY_SOURCE,swln.tv_service_type,
        sma.SERVICEABILITY_ID_MAILING_ADDRESS,  sma.SERVICEABILITY_ID_SERVICE_ADDRESS 
        from serviceabilityMatchedAccountView sma 
  JOIN ShawWlnServiceabilityView swln ON swln.address_id = sma.SERVICEABILITY_ID_MAILING_ADDRESS
  OR swln.address_id = sma.SERVICEABILITY_ID_SERVICE_ADDRESS
  where sma.RCIS_ID = 453811688 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-F-06

-- COMMAND ----------

Select distinct c.ECID, sma.RCIS_ID,
        sma.FA_ID, c.SHAW_MASTER_PARTY_ID,            
        sma.SERVICEABILITY_SOURCE,c.S_TV_SERVICEABILITY_FLAG,
        swln.address_id ,sma.SERVICEABILITY_ID_MAILING_ADDRESS,  sma.SERVICEABILITY_ID_SERVICE_ADDRESS 
        from customerMarketingView c
  INNER JOIN serviceabilityMatchedAccountView sma ON c.ECID = sma.RCIS_ID
  INNER JOIN ShawWlnServiceabilityView swln ON sma.SERVICEABILITY_ID_MAILING_ADDRESS = swln.address_id  
  INNER JOIN ShawWlnServiceabilityView swln2 ON swln2.address_id = sma.SERVICEABILITY_ID_SERVICE_ADDRESS
  where sma.SERVICEABILITY_SOURCE = 'Shaw' and c.S_TV_SERVICEABILITY_FLAG = 'N'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-F-07

-- COMMAND ----------

Select distinct c.ECID, sma.RCIS_ID,
        sma.FA_ID, c.SHAW_MASTER_PARTY_ID,            
        sma.SERVICEABILITY_SOURCE,c.HP_SERVICEABILITY_FLAG,swln.hp_serviceability_flag,
        swln.address_id ,sma.SERVICEABILITY_ID_MAILING_ADDRESS,  sma.SERVICEABILITY_ID_SERVICE_ADDRESS 
        from customerMarketingView c
  INNER JOIN serviceabilityMatchedAccountView sma ON c.ECID = sma.RCIS_ID
  INNER JOIN ShawWlnServiceabilityView swln ON sma.SERVICEABILITY_ID_MAILING_ADDRESS = swln.address_id  
  INNER JOIN ShawWlnServiceabilityView swln2 ON swln2.address_id = sma.SERVICEABILITY_ID_SERVICE_ADDRESS
  where sma.SERVICEABILITY_SOURCE = 'Shaw'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-F-11

-- COMMAND ----------

Select c.ECID, sma.FA_ID,sma.RCIS_ID,swln.address_id,
        c.MAX_DOWN_SPEED as MAX_DOWN_SPEED,c.MAX_UP_SPEED as MAX_UP_SPEED,
        swln.max_download_SPEED as MAX_DOWNLOAD_SPEED,swln.max_upload_speed as MAX_UPLOAD_SPEED,
        sma.SERVICEABILITY_ID_MAILING_ADDRESS,  sma.SERVICEABILITY_ID_SERVICE_ADDRESS
        from customerMarketingView c
  INNER JOIN serviceabilityMatchedAccountView sma ON c.ECID = sma.RCIS_ID
  INNER JOIN ShawWlnServiceabilityView swln ON sma.SERVICEABILITY_ID_MAILING_ADDRESS = swln.address_id  
  INNER JOIN ShawWlnServiceabilityView swln2 ON swln2.address_id = sma.SERVICEABILITY_ID_SERVICE_ADDRESS
  where sma.SERVICEABILITY_SOURCE = 'Shaw'
   group by c.ECID , sma.FA_ID,sma.RCIS_ID, swln.address_id,c.MAX_DOWN_SPEED, c.MAX_UP_SPEED, swln.max_download_SPEED, swln.max_upload_speed, sma.SERVICEABILITY_ID_MAILING_ADDRESS,  sma.SERVICEABILITY_ID_SERVICE_ADDRESS
  

-- COMMAND ----------

Select sma.RCIS_ID,swln.address_id,
        swln.max_download_SPEED as MAX_DOWNLOAD_SPEED,swln.max_upload_speed as MAX_UPLOAD_SPEED
        from serviceabilityMatchedAccountView sma 
  INNER JOIN ShawWlnServiceabilityView swln ON sma.SERVICEABILITY_ID_MAILING_ADDRESS = swln.address_id  
  OR swln.address_id = sma.SERVICEABILITY_ID_SERVICE_ADDRESS
  where sma.SERVICEABILITY_SOURCE = 'Shaw'
   and  sma.RCIS_ID = 49324946
