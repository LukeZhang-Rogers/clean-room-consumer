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
-- MAGIC AccountMatchedRuleset = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration5/AccountMatchedRuleset")
-- MAGIC MatchedEntity = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration5/MatchedEntity")
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
-- MAGIC rulesets.createOrReplaceTempView("rulesetView")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Sample data exploration

-- COMMAND ----------

select * from rulesetView 
where ruleset_type="DETERMINISTIC"

-- COMMAND ----------

select distinct ruleset_ID, ruleset_type  from matchedAccountView 


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Query Results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Probabilistic Matching - QA

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###TC-E-02 Proof Matching on known variance on names (First/Last)

-- COMMAND ----------

-- desc rulesetView
-- select * from rulesetView where ruleset_type = 'PROBABILISTIC'

-- desc matchedAccountView


--	first_name
select 
    r.ruleset_type
    , r.ruleset_id
    , r.ruleset_attr
	, cl_rogers_contact.first_name
    , cl_rogers_contact.last_name
    , cl_shaw_contact.first_name
    , cl_shaw_contact.last_name
	, cl_rogers_contact.city
	, cl_shaw_contact.city
	, cl_rogers_contact.state
	, cl_shaw_contact.state
	, cl_rogers_contact.e_mail
	, cl_shaw_contact.e_mail    
    , r.rogers_ecid
    , r.shaw_master_party_id
    , cl_rogers_contact.fa_id AS ROGERS_FA_ID
    , cl_shaw_contact.fa_id AS SHAW_FA_ID 
  from matchedAccountView r
    INNER JOIN cl_rogers_contact ON r.ROGERS_ECID=cl_rogers_contact.rcis_id AND r.Rogers_account_id = cl_rogers_contact.fa_id
    INNER JOIN cl_shaw_contact ON r.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl AND r.shaw_account_id = cl_shaw_contact.fa_id
      where ruleset_type = 'PROBABILISTIC' AND UPPER(cl_rogers_contact.first_name)!=UPPER(cl_shaw_contact.first_name)
        --        AND UPPER(cl_rogers_contact.first_name)<2 or UPPER(cl_shaw_contact.first_name)<2
        --AND ruleset_id = 23
        --AND ruleset_id = 24
        --AND ruleset_id = 25
        --AND ruleset_id = 26
        --AND ruleset_id = 27
        --AND ruleset_id = 28
        AND ruleset_id = 29
        AND ruleset_id != 34 
        --AND ruleset_attr != 1
          order by ruleset_id, ruleset_attr desc
  
   


/*  
    --where ruleset_type = 'PROBABILISTIC' AND rogers_ecid = 230471110
    --where ruleset_type = 'PROBABILISTIC' AND shaw_master_party_id = 26065010 or ruleset_type = 'PROBABILISTIC' AND rogers_ecid = 230471110
*/     


-- COMMAND ----------

select r.ruleset_id, count(r.ruleset_attr)
  from matchedAccountView r
    INNER JOIN cl_rogers_contact ON r.ROGERS_ECID=cl_rogers_contact.rcis_id AND r.Rogers_account_id = cl_rogers_contact.fa_id
	INNER JOIN cl_shaw_contact ON r.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl AND r.shaw_account_id = cl_shaw_contact.fa_id
  where ruleset_type = 'PROBABILISTIC' 
  AND r.ruleset_id != 34
  AND UPPER(cl_rogers_contact.first_name)!=UPPER(cl_shaw_contact.first_name)
  AND ruleset_attr = 1 group by r.ruleset_id order by ruleset_id ASC

-- COMMAND ----------

desc cl_rogers_contact

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###TC-E-03 Proof Matching on Firstname and Lastname Swapped

-- COMMAND ----------

/*
--	ROGERS first_name = SHAW last_name
select 
    r.ruleset_type
    , r.ruleset_id
    , r.ruleset_attr
	, cl_rogers_contact.first_name
    , cl_rogers_contact.last_name
    , cl_shaw_contact.first_name
    , cl_shaw_contact.last_name
    , r.rogers_ecid
    , r.shaw_master_party_id
    , cl_rogers_contact.fa_id AS ROGERS_FA_ID
    , cl_shaw_contact.fa_id AS SHAW_FA_ID 
  from matchedAccountView r
	INNER JOIN cl_rogers_contact ON r.ROGERS_ECID=cl_rogers_contact.rcis_id AND r.Rogers_account_id = cl_rogers_contact.fa_id
	INNER JOIN cl_shaw_contact ON r.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl AND r.shaw_account_id = cl_shaw_contact.fa_id
	  where ruleset_type = 'PROBABILISTIC' AND UPPER(cl_rogers_contact.first_name)=UPPER(cl_shaw_contact.last_name)
        order by ruleset_id, ruleset_attr desc
     
 */ 

--	ROGERS last_name = SHAW first_name
select 
    r.ruleset_type
    , r.ruleset_id
    , r.ruleset_attr
	, cl_rogers_contact.first_name
    , cl_rogers_contact.last_name
    , cl_shaw_contact.first_name
    , cl_shaw_contact.last_name
    , r.rogers_ecid
    , r.shaw_master_party_id
    , cl_rogers_contact.fa_id AS ROGERS_FA_ID
    , cl_shaw_contact.fa_id AS SHAW_FA_ID 
  from matchedAccountView r
	INNER JOIN cl_rogers_contact ON r.ROGERS_ECID=cl_rogers_contact.rcis_id AND r.Rogers_account_id = cl_rogers_contact.fa_id
	INNER JOIN cl_shaw_contact ON r.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl AND r.shaw_account_id = cl_shaw_contact.fa_id
	  where ruleset_type = 'PROBABILISTIC' AND UPPER(cl_rogers_contact.last_name)=UPPER(cl_shaw_contact.first_name)
	    order by ruleset_id, ruleset_attr desc

 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###TC-E-04 Comparing eMail domains if they are different

-- COMMAND ----------

select 
    r.ruleset_type
    , r.ruleset_id
    , r.ruleset_attr
	,cl_rogers_contact.e_mail
	,cl_shaw_contact.e_mail
	,r.rogers_ecid
	,r.shaw_master_party_id
	,cl_rogers_contact.fa_id AS ROGERS_FA_ID
	,cl_shaw_contact.fa_id AS SHAW_FA_ID 
  from matchedAccountView r
	INNER JOIN cl_rogers_contact ON r.ROGERS_ECID=cl_rogers_contact.rcis_id AND r.Rogers_account_id = cl_rogers_contact.fa_id
	INNER JOIN cl_shaw_contact ON r.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl AND r.shaw_account_id = cl_shaw_contact.fa_id
	  where ruleset_type = 'PROBABILISTIC' AND UPPER(RIGHT(cl_rogers_contact.e_mail,charindex('@',REVERSE(cl_rogers_contact.e_mail))-1))!=UPPER(RIGHT(cl_shaw_contact.e_mail,charindex('@',REVERSE(cl_shaw_contact.e_mail))-1)) 
        --AND ruleset_id = 23
        --AND ruleset_id = 24
        --AND ruleset_id = 25
        --AND ruleset_id = 26
        --AND ruleset_id = 27
        --AND ruleset_id = 28
        --AND ruleset_id = 29
        --AND ruleset_id = 34 
	    order by ruleset_id, ruleset_attr desc
        
        


-- COMMAND ----------

--EMAIL Domain differs - detailed query based on ruleset id : 


select r.ruleset_id, count(r.ruleset_attr)
  from matchedAccountView r
    INNER JOIN cl_rogers_contact ON r.ROGERS_ECID=cl_rogers_contact.rcis_id AND r.Rogers_account_id = cl_rogers_contact.fa_id
	INNER JOIN cl_shaw_contact ON r.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl AND r.shaw_account_id = cl_shaw_contact.fa_id
  where ruleset_type = 'PROBABILISTIC' 
   AND UPPER(RIGHT(cl_rogers_contact.e_mail,charindex('@',REVERSE(cl_rogers_contact.e_mail))-1))!=UPPER(RIGHT(cl_shaw_contact.e_mail,charindex('@',REVERSE(cl_shaw_contact.e_mail))-1))
   AND ruleset_attr = 1 group by r.ruleset_id order by ruleset_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###TC-E-05 Ambiguous CITY Names

-- COMMAND ----------

select 
    r.ruleset_type
    , r.ruleset_id
    , r.ruleset_attr
	,cl_rogers_contact.city
	,cl_shaw_contact.city
	,cl_rogers_contact.state
	,cl_shaw_contact.state
	,r.rogers_ecid
	,r.shaw_master_party_id
	,cl_rogers_contact.fa_id AS ROGERS_FA_ID
	,cl_shaw_contact.fa_id AS SHAW_FA_ID 
  from matchedAccountView r
	INNER JOIN cl_rogers_contact ON r.ROGERS_ECID=cl_rogers_contact.rcis_id AND r.Rogers_account_id = cl_rogers_contact.fa_id
	INNER JOIN cl_shaw_contact ON r.SHAW_MASTER_PARTY_ID=cl_shaw_contact.rcis_id_cl AND r.shaw_account_id = cl_shaw_contact.fa_id
	  where ruleset_type = 'PROBABILISTIC' AND cl_rogers_contact.city_cl!=cl_shaw_contact.city_cl
		order by ruleset_id, ruleset_attr desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###TC-E-06 Assess rate of match on account and customer level on FUZZY vs PROBABILISTIC vs DETERMINISTIC logic

-- COMMAND ----------

--Customer Level:
select count(distinct r.rogers_ecid), r.ruleset_type
	from matchedAccountView r
		where ruleset_attr > 0.85 
		  group by r.ruleset_type
		  order by ruleset_type

-- COMMAND ----------

Select count(distinct r.rogers_ecid), r.ruleset_type
  from matchedAccountView r
    where ruleset_attr = 1 and r.ruleset_type != 'DETERMINISTIC'
      group by r.ruleset_type
      order by ruleset_type

-- COMMAND ----------

/*
--Detailed query based on ruleset id : 

select r.ruleset_id, count(r.ruleset_attr)
from matchedAccountView r
where ruleset_type = 'PROBABILISTIC' AND ruleset_attr > 0.85 group by r.ruleset_id order by ruleset_id
*/

--Account Level:
select count(distinct r.rogers_account_id), r.ruleset_type
	from matchedAccountView r
	  where ruleset_attr > 0.85 
	    group by r.ruleset_type
		order by ruleset_type

-- COMMAND ----------



-- COMMAND ----------

select distinct(VIDEO_DELIVERY_TYPE) from cl_shaw_wireline

-- COMMAND ----------


