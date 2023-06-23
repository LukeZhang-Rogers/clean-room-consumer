-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Address Configuration

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC shaw_path = "/mnt/development/Processed/Iteration8/Shaw" 
-- MAGIC rogers_path = "/mnt/development/Processed/Iteration8/Rogers" 
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
-- MAGIC cl_shaw_consumer_wireless_service_df       = spark.read.format("parquet").load(shaw_consumer_wireless_service)
-- MAGIC
-- MAGIC cl_rogers_contact_df                 = spark.read.format("parquet").load(rogers_contact)
-- MAGIC cl_rogers_wireless_account_df        = spark.read.format("parquet").load(rogers_wireless_account)
-- MAGIC cl_rogers_wireline_account_df        = spark.read.format("parquet").load(rogers_wireline_account)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #Load dataframes
-- MAGIC AccountMatchedRuleset = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration4/AccountMatchedRuleset")
-- MAGIC
-- MAGIC MatchedEntity = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration4/MatchedEntity")
-- MAGIC rulesets = spark.read.format("parquet").load("/mnt/development/referencedata/rulesets")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Build Views

-- COMMAND ----------

-- MAGIC %python
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
-- MAGIC
-- MAGIC #match output
-- MAGIC AccountMatchedRuleset.createOrReplaceTempView("matchedAccountView")
-- MAGIC MatchedEntity.createOrReplaceTempView("matchedEntityView")
-- MAGIC rulesets.createOrReplaceTempView("rulesetsView")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sprint 4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TC-C-03

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Rogers

-- COMMAND ----------

select Rogers_ECID, BEST_det_match_ruleset_id, count (Shaw_master_party_id) from matchedEntityView
where DET_MATCH_IND="YES"
group by Rogers_ECID, BEST_det_match_ruleset_id

-- COMMAND ----------

select * from matchedEntityView
where Rogers_ECID=451071414

-- COMMAND ----------

select * from matchedAccountView where rogers_ECID =451071414 
--and Shaw_master_party_id= 6665833 
-- and shaw_account_id=20016957469

-- COMMAND ----------

select* from cl_rogers_contact where rcis_id = 451071414

-- COMMAND ----------

select S.RCIS_ID,S.fa_id,S.address_full,mailing_address_no_zipcode_dq_cl from matchedEntityView 
inner join cl_shaw_contact S on Shaw_master_party_id =S.rcis_id
where Rogers_ECID=451071414

-- COMMAND ----------

select S.RCIS_ID,S.fa_id,S.address_full,mailing_address_no_zipcode_dq_cl from matchedAccountView 
inner join cl_shaw_contact S on Shaw_master_party_id =S.rcis_id and shaw_account_id=S.fa_id
where Rogers_ECID=451071414 AND ruleset_id = 31

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Shaw

-- COMMAND ----------

select Shaw_master_party_id, BEST_det_match_ruleset_id, count (Rogers_ECID) from matchedEntityView
where DET_MATCH_IND="YES"
group by Shaw_master_party_id, BEST_det_match_ruleset_id

-- COMMAND ----------

select * from matchedEntityView
where Shaw_master_party_id= 24214126

-- COMMAND ----------

select * from cl_shaw_contact where rcis_id=445505

-- COMMAND ----------

select R.RCIS_ID,R.fa_id,R.address_cleansed,mailing_address_no_zipcode_dq_cl from matchedEntityView 
inner join cl_rogers_contact R on Rogers_ECID =R.rcis_id
where Shaw_master_party_id=24214126

-- COMMAND ----------

select R.RCIS_ID,R.fa_id,R.address_cleansed,mailing_address_no_zipcode_dq_cl from matchedAccountView 
inner join cl_rogers_contact R on Rogers_ECID =R.rcis_id and rogers_account_id=R.fa_id
where Shaw_master_party_id=445505 AND ruleset_id = 31

-- COMMAND ----------

Select Distinct ruleset_id from  matchedAccountView

-- COMMAND ----------

SELECT Distinct matchedAccountView.ROGERS_ECID, 
matchedAccountView.ruleset_id AS RULESET_ID, 
cl_rogers_contact.rcis_id AS ROGERS_RCIS_ID,
cl_rogers_contact.fa_id AS ROGERS_FA_ID,
cl_rogers_contact.mailing_address_full_cl as ROGERSADDRESS,
matchedAccountView.SHAW_MASTER_PARTY_ID,
cl_shaw_contact.rcis_id_cl AS SHAW_RCIS_ID,
cl_shaw_contact.fa_id AS SHAW_FA_ID,
cl_shaw_contact.mailing_address_full_cl AS SHAWADDRESS
FROM matchedAccountView
INNER JOIN cl_rogers_contact ON matchedAccountView.ROGERS_ECID=cl_rogers_contact.rcis_id AND matchedAccountView.Rogers_account_id = cl_rogers_contact.fa_id
INNER JOIN cl_shaw_contact ON matchedAccountView.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl AND matchedAccountView.Shaw_account_id = cl_shaw_contact.fa_id
where ruleset_id = 30 AND ruleset_type = 'deter' AND cl_rogers_contact.mailing_address_full_cl!=cl_SHAW_contact.mailing_address_full_cl;

-- COMMAND ----------

SELECT COUNT(*) FROM (SELECT Distinct matchedAccountView.ROGERS_ECID, matchedAccountView.ruleset_id, cl_rogers_contact.rcis_id, cl_rogers_contact.mailing_address_full_cl as ROGERSADDRESS,matchedAccountView.SHAW_MASTER_PARTY_ID,cl_shaw_contact.rcis_id_cl,cl_shaw_contact.mailing_address_full_cl AS SHAWADDRESS
FROM matchedAccountView
INNER JOIN cl_rogers_contact ON matchedAccountView.ROGERS_ECID=cl_rogers_contact.rcis_id
INNER JOIN cl_shaw_contact ON matchedAccountView.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl where ruleset_id = 17 AND cl_rogers_contact.mailing_address_full_cl!=cl_SHAW_contact.mailing_address_full_cl) AS A;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-C-01

-- COMMAND ----------

desc matchedAccountView

-- COMMAND ----------

DESC matchedEntityView

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-C-04

-- COMMAND ----------

SELECT COUNT (DISTINCT rcis_id) FROM cl_shaw_contact; --shaw

-- COMMAND ----------

SELECT COUNT(DISTINCT rcis_id) from cl_rogers_contact --rogers

-- COMMAND ----------

SELECT SUM(CASE WHEN rcis_id is null THEN 1 ELSE 0 END) AS NumberOfNullValues, COUNT(rcis_id) AS NumberOfNonNullValues FROM cl_rogers_contact


-- COMMAND ----------

SELECT SUM(CASE WHEN ROGERS_ECID is null THEN 1 ELSE 0 END) AS NumberOfNullValues, COUNT(ROGERS_ECID) AS NumberOfNonNullValues FROM matchedEntityView

-- COMMAND ----------

SELECT distinct rcis_id_cl FROM cl_rogers_contact WHERE NOT EXISTS (SELECT * FROM matchedEntityView WHERE matchedEntityView.ROGERS_ECID = cl_rogers_contact.rcis_id_cl) limit 5;

-- COMMAND ----------

SELECT COUNT(distinct rcis_id_cl) FROM cl_rogers_contact WHERE NOT EXISTS (SELECT * FROM matchedEntityView WHERE matchedEntityView.ROGERS_ECID = cl_rogers_contact.rcis_id_cl);

-- COMMAND ----------

SELECT COUNT(distinct rcis_id) FROM cl_shaw_contact WHERE NOT EXISTS (SELECT * FROM matchedEntityView WHERE matchedEntityView.SHAW_MASTER_PARTY_ID = cl_shaw_contact.rcis_id);

-- COMMAND ----------

Select * from cl_rogers_contact where rcis_id_cl = 32433614

-- COMMAND ----------

select distinct(rcis_id) from cl_rogers_contact where rcis_id NOT IN (SELECT distinct(ROGERS_ECID) from matchedEntityView) limit 5

-- COMMAND ----------

select count(*) from matchedEntityView where SHAW_MASTER_PARTY_ID IS NULL

-- COMMAND ----------

SELECT COUNT(distinct rcis_id_cl) FROM cl_shaw_contact WHERE NOT EXISTS (SELECT * FROM matchedEntityView WHERE matchedEntityView.SHAW_MASTER_PARTY_ID = cl_shaw_contact.rcis_id_cl);

-- COMMAND ----------

select count(distinct(rcis_id)) from cl_shaw_contact where rcis_id NOT IN (SELECT distinct(SHAW_MASTER_PARTY_ID) from matchedEntityView) --SHAW

-- COMMAND ----------

select distinct(rcis_id) from cl_shaw_contact where rcis_id NOT IN (SELECT distinct(SHAW_MASTER_PARTY_ID) from matchedEntityView) limit 5

-- COMMAND ----------

select * from matchedEntityView where ROGERS_ECID = '30560420' --Rogers

-- COMMAND ----------

select * from cl_shaw_contact where rcis_id = '1095002'

-- COMMAND ----------

select * from matchedEntityView where 

-- COMMAND ----------

select count(DISTINCT rcis_id) from cl_shaw_contact

-- COMMAND ----------

SELECT COUNT ( DISTINCT ROGERS_ECID ) as rogers_matched FROM matchedEntityView;



-- COMMAND ----------

SELECT COUNT ( DISTINCT SHAW_MASTER_PARTY_ID ) as shaw_matched FROM matchedEntityView;

-- COMMAND ----------

DESC cl_rogers_wireless

-- COMMAND ----------

SELECT distinct cl_rogers_contact.* FROM cl_rogers_contact INNER JOIN cl_rogers_wireline ON cl_rogers_contact.rcis_id = cl_rogers_wireline.rcis_id
WHERE NOT EXISTS (SELECT * FROM matchedEntityView WHERE matchedEntityView.ROGERS_ECID = cl_rogers_contact.rcis_id_cl) limit 5;

-- COMMAND ----------

SELECT * FROM cl_rogers_wireline where rcis_id = ' '

-- COMMAND ----------

SELECT * FROM MatchedEntityView where rogers_ecid = ' '

-- COMMAND ----------

SELECT distinct cl_rogers_contact.* FROM cl_rogers_contact INNER JOIN cl_rogers_wireless ON cl_rogers_contact.rcis_id = cl_rogers_wireless.rcis_id
WHERE NOT EXISTS (SELECT * FROM matchedEntityView WHERE matchedEntityView.ROGERS_ECID = cl_rogers_contact.rcis_id_cl);

-- COMMAND ----------

SELECT rcis_id_cl FROM cl_rogers_wireline where rcis_id = ' '

-- COMMAND ----------

SELECT distinct cl_shaw_contact.* FROM cl_shaw_contact INNER JOIN cl_shaw_wireless ON cl_shaw_contact.rcis_id = cl_shaw_wireless.rcis_id
WHERE NOT EXISTS (SELECT * FROM matchedEntityView WHERE matchedEntityView.shaw_master_party_id = cl_shaw_contact.rcis_id_cl) limit 5;

-- COMMAND ----------

SELECT distinct cl_shaw_contact.* FROM cl_shaw_contact INNER JOIN cl_shaw_wireline ON cl_shaw_contact.rcis_id = cl_shaw_wireline.rcis_id
WHERE NOT EXISTS (SELECT * FROM matchedEntityView WHERE matchedEntityView.shaw_master_party_id = cl_shaw_contact.rcis_id_cl) limit 5;

-- COMMAND ----------

SELECT * FROM cl_shaw_wireless where trim(rcis_id) = ""
