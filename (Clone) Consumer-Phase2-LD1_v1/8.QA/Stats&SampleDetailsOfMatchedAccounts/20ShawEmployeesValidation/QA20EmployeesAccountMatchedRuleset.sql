-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Load Data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #Raw Data
-- MAGIC
-- MAGIC #paths
-- MAGIC shaw_consumer_wireless_account = "/mnt/development/processed/interim/shaw_wireless_accounts"
-- MAGIC shaw_consumer_wireless_service = "/mnt/development/processed/interim/shaw_wireless_services"
-- MAGIC shaw_consumer_wireline_account = "/mnt/development/processed/interim/shaw_wireline_accounts"
-- MAGIC shaw_consumer_contact          = "/mnt/development/processed/interim/shaw_contacts"
-- MAGIC rogers_contact                 = '/mnt/rogerssaprod/deloitte-cr/rogers_0218/rogers_contact'
-- MAGIC rogers_wireless_account        = '/mnt/rogerssaprod/deloitte-cr/rogers_0218/rogers_wireless_account'
-- MAGIC rogers_wireline_account        = '/mnt/rogerssaprod/deloitte-cr/rogers_0218/rogers_wireline_account'
-- MAGIC
-- MAGIC #Load Dataframes
-- MAGIC
-- MAGIC shaw_consumer_wireless_account_df       = spark.read.format("parquet").load(shaw_consumer_wireless_account)
-- MAGIC shaw_consumer_wireline_account_df       = spark.read.format("parquet").load(shaw_consumer_wireline_account)
-- MAGIC shaw_consumer_contact_df                = spark.read.format("parquet").load(shaw_consumer_contact)
-- MAGIC shaw_consumer_wireless_service_df       = spark.read.format("parquet").load(shaw_consumer_wireless_service)
-- MAGIC
-- MAGIC rogers_contact_df                 = spark.read.format("parquet").load(rogers_contact)
-- MAGIC rogers_wireless_account_df        = spark.read.format("parquet").load(rogers_wireless_account)
-- MAGIC rogers_wireline_account_df        = spark.read.format("parquet").load(rogers_wireline_account)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #Cleansed Data
-- MAGIC
-- MAGIC shaw_path = "/mnt/development/Processed/Iteration8/Shaw" 
-- MAGIC rogers_path = "/mnt/development/Processed/Iteration8/Rogers" 
-- MAGIC
-- MAGIC
-- MAGIC # shaw_consumer_wireless_account = shaw_path + '/20220216_CONSUMER_WIRELESS_ACCOUNT'
-- MAGIC # shaw_consumer_wireless_service = shaw_path + '/20220216_CONSUMER_WIRELESS_SERVICE'
-- MAGIC # shaw_consumer_wireline_account = shaw_path + '/20220216_CONSUMER_WIRELINE_ACCOUNT'
-- MAGIC # shaw_consumer_contact          = shaw_path + '/20220216_CONSUMER_CONTACT'
-- MAGIC
-- MAGIC # rogers_contact                 = rogers_path + '/rogers_contact'
-- MAGIC # rogers_wireless_account        = rogers_path + '/rogers_wireless_account'
-- MAGIC # rogers_wireline_account        = rogers_path + '/rogers_wireline_account'
-- MAGIC
-- MAGIC shaw_consumer_wireless_account = shaw_path + '/WirelessAccount' 
-- MAGIC shaw_consumer_wireline_account = shaw_path + '/WirelineAccount' 
-- MAGIC shaw_consumer_contact = shaw_path + '/Contact' 
-- MAGIC
-- MAGIC rogers_contact = rogers_path + '/Contact' 
-- MAGIC rogers_wireless_account = rogers_path + '/WirelessAccount' 
-- MAGIC rogers_wireline_account = rogers_path + '/WirelineAccount'
-- MAGIC
-- MAGIC #Load dataframes
-- MAGIC cl_shaw_consumer_wireless_account_df       = spark.read.format("parquet").load(shaw_consumer_wireless_account)
-- MAGIC cl_shaw_consumer_wireline_account_df       = spark.read.format("parquet").load(shaw_consumer_wireline_account)
-- MAGIC cl_shaw_consumer_contact_df                = spark.read.format("parquet").load(shaw_consumer_contact)
-- MAGIC #cl_shaw_consumer_wireless_service_df       = spark.read.format("parquet").load(shaw_consumer_wireless_service)
-- MAGIC
-- MAGIC cl_rogers_contact_df                 = spark.read.format("parquet").load(rogers_contact)
-- MAGIC cl_rogers_wireless_account_df        = spark.read.format("parquet").load(rogers_wireless_account)
-- MAGIC cl_rogers_wireline_account_df        = spark.read.format("parquet").load(rogers_wireline_account)
-- MAGIC
-- MAGIC #Read output parquet files
-- MAGIC matched_df = spark.read.format("parquet").load("/mnt/development/silver/iteration8/matched")
-- MAGIC unmatched_df = spark.read.format("parquet").load("/mnt/development/silver/iteration8/unmatched")
-- MAGIC
-- MAGIC
-- MAGIC #Shaw Employees 8 Records
-- MAGIC shaw_matched_accounts = "/mnt/shawsaprod/shaw-snflk-dir-01/SHAW/20220330_MATCHED_ADDRESSES"
-- MAGIC shaw_matched_accounts_df = spark.read.format("parquet").load(shaw_matched_accounts)
-- MAGIC
-- MAGIC #Output File from DEV team
-- MAGIC account_matched_ruleset = "/mnt/development/Processed/QA/Iteration1/AccountMatchedRuleset"
-- MAGIC account_matched_ruleset_df= spark.read.format("parquet").load(account_matched_ruleset)
-- MAGIC
-- MAGIC #Full Rogers table separate from Full Shaw table
-- MAGIC MatchedEntity = "/mnt/development/Processed/QA/Iteration1/MatchedEntity"
-- MAGIC MatchedEntity_df= spark.read.format("parquet").load(MatchedEntity)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Build Views

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #convert to views
-- MAGIC
-- MAGIC #raw data
-- MAGIC shaw_consumer_wireless_account_df.createOrReplaceTempView("r_shaw_wireless")
-- MAGIC shaw_consumer_wireline_account_df.createOrReplaceTempView("r_shaw_wireline")
-- MAGIC shaw_consumer_contact_df.createOrReplaceTempView("r_shaw_contact")
-- MAGIC               
-- MAGIC rogers_wireless_account_df.createOrReplaceTempView("r_rogers_wireless")       
-- MAGIC rogers_wireline_account_df.createOrReplaceTempView("r_rogers_wireline")
-- MAGIC rogers_contact_df.createOrReplaceTempView("r_rogers_contact")  
-- MAGIC
-- MAGIC #cleansed data
-- MAGIC cl_shaw_consumer_wireless_account_df.createOrReplaceTempView("cl_shaw_wireless")
-- MAGIC cl_shaw_consumer_wireline_account_df.createOrReplaceTempView("cl_shaw_wireline")
-- MAGIC cl_shaw_consumer_contact_df.createOrReplaceTempView("cl_shaw_contact")
-- MAGIC               
-- MAGIC cl_rogers_wireless_account_df.createOrReplaceTempView("cl_rogers_wireless")        
-- MAGIC cl_rogers_wireline_account_df.createOrReplaceTempView("cl_rogers_wireline")
-- MAGIC cl_rogers_contact_df.createOrReplaceTempView("cl_rogers_contact")  
-- MAGIC
-- MAGIC #match output
-- MAGIC matched_df.createOrReplaceTempView("matchedView")
-- MAGIC unmatched_df.createOrReplaceTempView("unmatchedView")
-- MAGIC
-- MAGIC #Shaw Employees 8 Records
-- MAGIC shaw_matched_accounts_df.createOrReplaceTempView("shawEmployees")
-- MAGIC
-- MAGIC #Output File from DEV team
-- MAGIC account_matched_ruleset_df.createOrReplaceTempView("AcctMatchedRuleset")
-- MAGIC
-- MAGIC #Full Rogers or Full Shaw
-- MAGIC MatchedEntity_df.createOrReplaceTempView("MatchedEntity")
-- MAGIC
-- MAGIC #Consider Global views instead of temp views if you plan to split this notebook into separate notebooks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##QA for Eric/Shaw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###The 8 Shaw Employee records

-- COMMAND ----------

select * from shawEmployees order by 3, 2, 4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###UNION cl_shaw_contact & cl_rogers_contact

-- COMMAND ----------

--select first_name, last_name, mailing_address_full_cl, length(mailing_address_full_cl) from cl_shaw_contact
select "SHAW" as source, rcis_id, fa_id, first_name, last_name, address, city, province, postalcode from cl_shaw_contact
  
    where lower(city) = 'toronto'
    --where lower(city) like '%abbotsford%'
    --where lower(city) in ('thornhill','calgary','abbotsford cres','victoria','nanaimo','winnipeg')
    and lower(province) = 'on'
    --and lower(province) in ('ab','bc','mb','on')
    and lower(postalcode_cleansed) = 'l4j8a2'
    --and upper(postalcode_cleansed) = 'L4J8A2'
    --and lower(postalcode_cleansed) in ('t2z4p9','t3h4l4','t3h4m7')
    --and upper(postalcode_cleansed) in ('L4J8A2','T2Z4P9','V2S3L2','T3H4L4','V9B1Y4','V9S2C4','R3T1W1','T3H4M7')
    --and lower(first_name) = 'eric'
    and upper(address) like '%12%ROSEDALE%'

UNION
--select first_name, last_name, mailing_address_full_cl, length(mailing_address_full_cl) from cl_rogers_contact
select "ROGERS" as source, rcis_id, fa_id, first_name, last_name, address, city, province, postalcode from cl_rogers_contact
  
    where lower(city) = 'thornhill'
    --where lower(city) like '%abbotsford%'
    --where lower(city) in ('thornhill','calgary','abbotsford cres','victoria','nanaimo','winnipeg')
    and lower(province) = 'on'
    --and lower(province) in ('ab','bc','mb','on')
    and lower(postalcode_cleansed) = 'l4j8a2'
    --and upper(postalcode_cleansed) = 'R3T1W1'
    --and lower(postalcode_cleansed) in ('t2z4p9','t3h4l4','t3h4m7')
    --and upper(postalcode_cleansed) in ('L4J8A2','T2Z4P9','V2S3L2','T3H4L4','V9B1Y4','V9S2C4','R3T1W1','T3H4M7')
    --and lower(address) like '%30%elgin%est%hill%'
    and lower(address) like '%rosedale%'
    and lower(last_name) = 'brener'
  
    

-- COMMAND ----------

select first_name, last_name, mailing_address_full_cl, dq_e_mail_cl from cl_rogers_contact
where lower(last_name) = 'brener'

-- COMMAND ----------

select * from cl_shaw_contact where dq_e_mail_cl = 'ericbrenergmailcom'

-- COMMAND ----------

select "SHAW" as source, rcis_id, fa_id, first_name, last_name, address, city, province, postalcode from cl_shaw_contact where rcis_id in (2752914, 2166237,794959,1076380,1416875,184016,180833)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Find if shaw FA_ID (shaw_account_id) or rogers FA_ID (rogers_account_id) exist in Account_Matched_Ruleset table

-- COMMAND ----------

--desc AcctMatchedRuleset

--select count(*) from AcctMatchedRuleset


select "SHAW" as source, rogers_ecid, rogers_account_id, rogers_sam_key, rogers_account_type, rogers_account_status, shaw_master_party_id, shaw_account_id, shaw_account_type, shaw_account_status, ruleset_id, ruleset_type from AcctMatchedRuleset
 where shaw_account_id = '09901059903'  
  or shaw_account_id = '03139646265'
  or shaw_account_id = 'DBC50000729306'
  or shaw_account_id = 'DBC10002898657'
  or shaw_account_id = '09900258540'
  or shaw_account_id = '01407338967'
  or shaw_account_id = '09900522227'
  or shaw_account_id = '01279953092'
  or shaw_account_id = '03826578202'


UNION

--Search for all ROGERS ECID's
select "ROGERS" as source, rogers_ecid, rogers_account_id, rogers_sam_key, rogers_account_type, rogers_account_status, shaw_master_party_id, shaw_account_id, shaw_account_type, shaw_account_status, ruleset_id, ruleset_type from AcctMatchedRuleset 
 where rogers_account_id = '906750658'  
  or rogers_account_id = '672716214'
  or rogers_account_id = '775463318'
  or rogers_account_id = '704688738'
  or rogers_account_id = '856429618'
  or rogers_account_id = '725200489'
  or rogers_account_id = '596219790'
  or rogers_account_id = '753628536'
  or rogers_account_id = '239261776207'
  or rogers_account_id = '916202674'
  or rogers_account_id = '907476600'
  or rogers_account_id = '235936218'
  or rogers_account_id = '908250632'
  or rogers_account_id = '740295035'
  or rogers_account_id = '236340519'


 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###AcctMatchedRuleset - Search on RCIS_ID and Shaw FA_ID or RCISID and Rogers FA_ID

-- COMMAND ----------


select "SHAW" as source, rogers_ecid, rogers_account_id, rogers_sam_key, rogers_account_type, rogers_account_status, shaw_master_party_id, shaw_account_id, shaw_account_type, shaw_account_status, ruleset_id, ruleset_type from AcctMatchedRuleset
 where shaw_account_id = '09901059903'  and shaw_master_party_id = '2752914'
  or shaw_account_id = '03139646265'  and shaw_master_party_id = '2166237'
  or shaw_account_id = 'DBC50000729306'  and shaw_master_party_id = '2166237'
  or shaw_account_id = 'DBC10002898657'  and shaw_master_party_id = '794959'
  or shaw_account_id = '09900258540'  and shaw_master_party_id = '794959'
  or shaw_account_id = '01407338967'  and shaw_master_party_id = '1076380'
  or shaw_account_id = '09900522227'  and shaw_master_party_id = '1416875'
  or shaw_account_id = '01279953092'  and shaw_master_party_id = '184016'
  or shaw_account_id = '03826578202'  and shaw_master_party_id = '180833'



UNION

select "ROGERS" as source, rogers_ecid, rogers_account_id, rogers_sam_key, rogers_account_type, rogers_account_status, shaw_master_party_id, shaw_account_id, shaw_account_type, shaw_account_status, ruleset_id, ruleset_type from AcctMatchedRuleset 
 where rogers_account_id = '906750658'  and rogers_ecid = '48876173'
  or rogers_account_id = '672716214'  and rogers_ecid = '431880473'
  or rogers_account_id = '775463318'  and rogers_ecid = '34924139'
  or rogers_account_id = '704688738'  and rogers_ecid = '401217103'
  or rogers_account_id = '856429618'  and rogers_ecid = '66217279'
  or rogers_account_id = '725200489'  and rogers_ecid = '436430609'
  or rogers_account_id = '596219790'  and rogers_ecid = '419401414'
  or rogers_account_id = '753628536'  and rogers_ecid = '440295261'
  or rogers_account_id = '916202674'  and rogers_ecid = '453433779'
  or rogers_account_id = '907476600'  and rogers_ecid = '72718686'
  or rogers_account_id = '235936218'  and rogers_ecid = '72718686'
  or rogers_account_id = '908250632'  and rogers_ecid = '72718686'
  or rogers_account_id = '740295035'  and rogers_ecid = '72718686'
  or rogers_account_id = '236340519'  and rogers_ecid = '72718686'
  or rogers_account_id = '239261776207'  and rogers_ecid = '72718686'
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### MatchedEntity search

-- COMMAND ----------

--desc MatchedEntity

--select MATCHED_ENTITY_GUID, ROGERS_ECID, SHAW_MASTER_PARTY_ID from MatchedEntity
--limit 20


--Search for all ROGERS ECID's

select MATCHED_ENTITY_GUID, ROGERS_ECID, SHAW_MASTER_PARTY_ID, BEST_DET_MATCH_RULESET_ID from MatchedEntity 
 where --rogers_ecid = '34924139'  
    --or rogers_ecid = '48876173'
    --or rogers_ecid = '66217279'
     rogers_ecid = '72718686'
    --or rogers_ecid = '401217103'
    --or rogers_ecid = '419401414'
    --or rogers_ecid = '431880473'
    --or rogers_ecid = '436430609'
    --or rogers_ecid = '440295261'
    or rogers_ecid = '453433779'

--UNION

--Search for all SHAW Master Party ID's
--select MATCHED_ENTITY_GUID, ROGERS_ECID, SHAW_MASTER_PARTY_ID from MatchedEntity 
  --where shaw_master_party_id = '180833'  
    --or shaw_master_party_id = '184016'
    --or shaw_master_party_id = '794959'
    --or shaw_master_party_id = '1076380'
    --or shaw_master_party_id = '1416875'
    --or shaw_master_party_id = '2166237'
    --or shaw_master_party_id = '2752914'
 --order by rogers_ecid, shaw_master_party_id


-- COMMAND ----------

--select * from cl_rogers_wireless where rcis_id = '453433779'
select * from cl_rogers_wireline where rcis_id = '453433779'

-- COMMAND ----------


