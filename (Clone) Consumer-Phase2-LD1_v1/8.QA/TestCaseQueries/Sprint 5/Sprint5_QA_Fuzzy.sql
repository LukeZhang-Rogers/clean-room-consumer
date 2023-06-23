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
-- MAGIC #cl_shaw_consumer_wireless_service_df       = spark.read.format("parquet").load(shaw_consumer_wireless_service)
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
-- MAGIC rulesets = spark.read.format("parquet").load("/mnt/development/referencedata/Ruleset")

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
-- MAGIC #ServiceabilityMatchedAccounts.createOrReplaceTempView("serviceabilityMatchedAccountView")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sprint 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TC-D-01 ##Structure checking

-- COMMAND ----------

DESC  rulesetsView --matchedAccountView --matchedEntityView

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TC-D-02 ##Variance in firstname

-- COMMAND ----------

Select cl_rogers_contact.first_name,cl_rogers_contact.last_name,
cl_shaw_contact.first_name,cl_shaw_contact.last_name,
cl_rogers_contact.city,cl_rogers_contact.state,
cl_shaw_contact.city,cl_shaw_contact.state,
r.ruleset_id,r.ruleset_attr, r.ruleset_type
from matchedAccountView r
INNER JOIN cl_rogers_contact ON r.ROGERS_ECID=cl_rogers_contact.rcis_id AND r.Rogers_account_id = cl_rogers_contact.fa_id
INNER JOIN cl_shaw_contact ON r.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl AND r.shaw_account_id = cl_shaw_contact.fa_id
where ruleset_type = 'FUZZY' AND r.ruleset_id != 33 AND UPPER(cl_rogers_contact.first_name)!=UPPER(cl_shaw_contact.first_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TC-D-03 ##Swapping firstname and lastname

-- COMMAND ----------

Select cl_rogers_contact.first_name,cl_rogers_contact.last_name,
cl_shaw_contact.first_name,cl_shaw_contact.last_name,
r.rogers_ecid, r.shaw_master_party_id,r.ruleset_id,r.ruleset_attr, r.ruleset_type,cl_rogers_contact.fa_id AS ROGERS_FA_ID,
cl_shaw_contact.fa_id AS SHAW_FA_ID from matchedAccountView r
INNER JOIN cl_rogers_contact ON r.ROGERS_ECID=cl_rogers_contact.rcis_id AND r.Rogers_account_id = cl_rogers_contact.fa_id
INNER JOIN cl_shaw_contact ON r.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl AND r.shaw_account_id = cl_shaw_contact.fa_id
where ruleset_type = 'FUZZY' AND UPPER(cl_rogers_contact.first_name)=UPPER(cl_shaw_contact.last_name)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##TC-D-04 ### email domain

-- COMMAND ----------

Select cl_rogers_contact.e_mail,cl_shaw_contact.e_mail,
r.ruleset_id,r.ruleset_attr, r.ruleset_type,
r.rogers_ecid, r.shaw_master_party_id,cl_rogers_contact.fa_id AS ROGERS_FA_ID,
cl_shaw_contact.fa_id AS SHAW_FA_ID from matchedAccountView r
INNER JOIN cl_rogers_contact ON r.ROGERS_ECID=cl_rogers_contact.rcis_id AND r.Rogers_account_id = cl_rogers_contact.fa_id
INNER JOIN cl_shaw_contact ON r.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl AND r.shaw_account_id = cl_shaw_contact.fa_id
where ruleset_type = 'FUZZY' AND UPPER(RIGHT(cl_rogers_contact.e_mail,charindex('@',REVERSE(cl_rogers_contact.e_mail))-1))!=UPPER(RIGHT(cl_shaw_contact.e_mail,charindex('@',REVERSE(cl_shaw_contact.e_mail))-1))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-D-05 ###Ambiguous city 

-- COMMAND ----------

Select 
        cl_rogers_contact.city,cl_shaw_contact.city,
        cl_rogers_contact.state,cl_shaw_contact.state,
        r.ruleset_id,r.ruleset_attr, r.ruleset_type,
        r.rogers_ecid, r.shaw_master_party_id,cl_rogers_contact.fa_id AS ROGERS_FA_ID,
        cl_shaw_contact.fa_id AS SHAW_FA_ID 
   from matchedAccountView r
  INNER JOIN cl_rogers_contact ON r.ROGERS_ECID=cl_rogers_contact.rcis_id AND r.Rogers_account_id = cl_rogers_contact.fa_id
  INNER JOIN cl_shaw_contact ON r.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl AND r.shaw_account_id = cl_shaw_contact.fa_id
      where ruleset_type = 'FUZZY' AND cl_rogers_contact.city_cl!=cl_shaw_contact.city_cl

-- COMMAND ----------

select state_cl from cl_rogers_contact

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Deterministic vs Fuzzy vs Probabilistics ruleset_attr >= 0.85

-- COMMAND ----------

Select  count(distinct r.rogers_ecid), r.ruleset_type
from matchedAccountView r
where r.ruleset_attr >= 0.85 group by r.ruleset_type

-- COMMAND ----------

Select  count(distinct r.rogers_account_id), r.ruleset_type
from matchedAccountView r
where ruleset_attr >= 0.85 group by r.ruleset_type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-D-07 ###Assess cases where result matched on Deterministic, but failed to match on probabilistic or fuzzy

-- COMMAND ----------

select r.ruleset_type,r.RULESET_ID,cl_rogers_contact.first_name, cl_rogers_contact.last_name,cl_shaw_contact.first_name, cl_shaw_contact.last_name,
cl_rogers_contact.city,cl_shaw_contact.city, cl_rogers_contact.state,cl_shaw_contact.state, r.rogers_account_status,r.shaw_account_status
--DET_MATCH_IND, FUZZY_MATCH_IND, PROB_MATCH_IND, best_det_match_ruleset_id, best_fuzzy_match_ruleset_id, best_prob_match_ruleset_id, count(*)
from matchedEntityView
  INNER JOIN matchedAccountView r ON matchedEntityView.ROGERS_ECID = r.ROGERS_ECID AND matchedEntityView.best_det_match_ruleset_id = r.RULESET_ID
  INNER JOIN cl_rogers_contact ON r.ROGERS_ECID=cl_rogers_contact.rcis_id AND r.Rogers_account_id = cl_rogers_contact.fa_id
  INNER JOIN cl_shaw_contact ON r.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl AND r.shaw_account_id = cl_shaw_contact.fa_id
      where (DET_MATCH_IND="Y" and FUZZY_MATCH_IND is null and PROB_MATCH_IND is null) 
      AND (SUBSTRING(cl_rogers_contact.first_name,1,1) = SUBSTRING(cl_shaw_contact.first_name,1,1)) AND
      cl_rogers_contact.last_name = cl_shaw_contact.last_name 
      AND cl_rogers_contact.city = cl_shaw_contact.city
      AND cl_rogers_contact.state = cl_shaw_contact.state
      --AND cl_rogers_contact.first_name != cl_shaw_contact.first_name
--group by DET_MATCH_IND, FUZZY_MATCH_IND, PROB_MATCH_IND, best_det_match_ruleset_id, best_fuzzy_match_ruleset_id, best_prob_match_ruleset_id, r.ruleset_type

-- COMMAND ----------

select r.ruleset_type,r.RULESET_ID,
cl_rogers_contact.ZIPCODE,cl_shaw_contact.ZIPCODE,
      cl_rogers_contact.city,cl_shaw_contact.city,
      cl_rogers_contact.state,cl_shaw_contact.state
--DET_MATCH_IND, FUZZY_MATCH_IND, PROB_MATCH_IND, best_det_match_ruleset_id, best_fuzzy_match_ruleset_id, best_prob_match_ruleset_id, count(*)
from matchedEntityView
  INNER JOIN matchedAccountView r ON matchedEntityView.ROGERS_ECID = r.ROGERS_ECID AND matchedEntityView.best_det_match_ruleset_id = r.RULESET_ID
  INNER JOIN cl_rogers_contact ON r.ROGERS_ECID=cl_rogers_contact.rcis_id AND r.Rogers_account_id = cl_rogers_contact.fa_id
  INNER JOIN cl_shaw_contact ON r.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl AND r.shaw_account_id = cl_shaw_contact.fa_id
      where (DET_MATCH_IND="Y" and FUZZY_MATCH_IND is null and PROB_MATCH_IND is null) 
      AND cl_rogers_contact.ZIPCODE = cl_shaw_contact.ZIPCODE
      AND cl_rogers_contact.city = cl_shaw_contact.city
      AND cl_rogers_contact.state = cl_shaw_contact.state
--group by DET_MATCH_IND, FUZZY_MATCH_IND, PROB_MATCH_IND, best_det_match_ruleset_id, best_fuzzy_match_ruleset_id, best_prob_match_ruleset_id, r.ruleset_type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TC-D-09 ##False positive matches on Fuzzy address level

-- COMMAND ----------

5y6yuselect  best_fuzzy_match_ruleset_id,
cl_rogers_wireline.service_address as rogers_service_address,
cl_rogers_contact.address as rogers_mailing_address,
cl_shaw_contact.mailing_address_full as shaw_mailing_address_full,cl_shaw_wireline.service_address as shaw_service_address,
r.ruleset_type,r.ROGERS_ECID,r.Rogers_account_id,r.SHAW_MASTER_PARTY_ID,r.shaw_account_id,
DET_MATCH_IND, FUZZY_MATCH_IND, PROB_MATCH_IND, best_det_match_ruleset_id,  best_prob_match_ruleset_id
from matchedEntityView
  INNER JOIN matchedAccountView r ON matchedEntityView.ROGERS_ECID = r.ROGERS_ECID AND matchedEntityView.best_fuzzy_match_ruleset_id = r.RULESET_ID
     INNER JOIN cl_rogers_contact ON r.ROGERS_ECID=cl_rogers_contact.rcis_id AND r.Rogers_account_id = cl_rogers_contact.fa_id
          INNER JOIN cl_shaw_contact ON r.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl AND r.shaw_account_id = cl_shaw_contact.fa_id
              LEFT OUTER JOIN cl_rogers_wireline ON r.ROGERS_ECID=cl_rogers_wireline.rcis_id AND r.Rogers_account_id = cl_rogers_wireline.fa_id
                  LEFT OUTER JOIN cl_shaw_wireline ON r.SHAW_MASTER_PARTY_ID=cl_shaw_wireline.rcis_id_cl AND r.shaw_account_id = cl_shaw_wireline.fa_id
      where (DET_MATCH_IND is null and FUZZY_MATCH_IND="Y" and PROB_MATCH_IND is null) AND best_fuzzy_match_ruleset_id = 33 
      AND (cl_rogers_wireline.service_address !=cl_shaw_wireline.service_address AND cl_rogers_contact.address != cl_shaw_contact.address)
     
     group by DET_MATCH_IND, FUZZY_MATCH_IND, PROB_MATCH_IND, best_det_match_ruleset_id, best_fuzzy_match_ruleset_id, best_prob_match_ruleset_id, r.ruleset_type,   rogers_service_address,shaw_service_address,rogers_mailing_address,shaw_mailing_address_full,r.ROGERS_ECID,r.Rogers_account_id,r.SHAW_MASTER_PARTY_ID,r.shaw_account_id

-- COMMAND ----------

Select * from matchedaccountview where rogers_ecid = '441264790' and shaw_master_party_id = '2152797' and ruleset_type = 'FUZZY'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-D-08 fuzzy matches on cases where only names are different

-- COMMAND ----------

Select cl_rogers_contact.first_name,cl_rogers_contact.last_name,
cl_shaw_contact.first_name,cl_shaw_contact.last_name,
cl_rogers_contact.address,
cl_shaw_contact.address,
r.ruleset_id,r.ruleset_attr, r.ruleset_type
from matchedAccountView r
INNER JOIN cl_rogers_contact ON r.ROGERS_ECID=cl_rogers_contact.rcis_id AND r.Rogers_account_id = cl_rogers_contact.fa_id
INNER JOIN cl_shaw_contact ON r.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl AND r.shaw_account_id = cl_shaw_contact.fa_id
where ruleset_type = 'FUZZY' AND UPPER(cl_rogers_contact.first_name)!=UPPER(cl_shaw_contact.first_name)

-- COMMAND ----------

select  DISTINCT(RULESET_ID)
from  matchedAccountView WHERE RULESET_TYPE = 'FUZZY'

-- COMMAND ----------

select  *
from matchedEntityView where ROGERS_ECID is not null
  limit 5

-- COMMAND ----------

select * from matchedAccountView where ROGERS_ACCOUNT_ID = '729491001' AND RULESET_TYPE = 'FUZZY' AND SHAW_ACCOUNT_ID = '09900353772'

-- COMMAND ----------

SELECT * FROM CL_ROGERS_CONTACT WHERE FA_ID = '895283000'

-- COMMAND ----------

SELECT * FROM cl_shaw_wireline WHERE FA_ID = '01421810410'
