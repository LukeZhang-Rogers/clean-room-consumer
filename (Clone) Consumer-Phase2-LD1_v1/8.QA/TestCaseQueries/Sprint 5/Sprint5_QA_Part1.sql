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
-- MAGIC MatchedEntity = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration6/MatchedEntity")
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
-- MAGIC ##Query Results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###TC-D-06

-- COMMAND ----------

-- New data has inactive accounts excluded
with 
activeAccounts as
(
  select distinct rogers_ecid, shaw_master_party_id, RULESET_TYPE, min (RULESET_ID) as DET_RULESET_ID
  from matchedAccountView
  where rogers_ecid is not null
  and RULESET_TYPE="DETERMINISTIC"
  and (rogers_account_status in ('o', 's', 't') or rogers_account_status in (2,3,4,7))
  and shaw_account_status in ('active', 'soft suspended')
  group by rogers_ecid, shaw_master_party_id, RULESET_TYPE
),
RulesetType as
(
  select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Type  
  from rulesetView
)

select distinct  

R1.Type as DET_TYPE
, R2.Type as fuzzy_TYPE
-- , R3.Type as prob_TYPE
, count (*) as count
from matchedEntityView M
left join activeAccounts A on M.rogers_ecid=A.rogers_ecid and M.shaw_master_party_id=A.shaw_master_party_id 
left join RulesetType R1 on A.DET_RULESET_ID=R1.ruleset_id
left join RulesetType R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
left join RulesetType R3 on M.BEST_prob_MATCH_RULESET_ID=R3.ruleset_id


group by DET_TYPE
, fuzzy_TYPE
 , prob_TYPE
order by DET_TYPE desc, fuzzy_TYPE desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###TC-D-07

-- COMMAND ----------

-- New data has inactive accounts excluded
with 
activeAccounts as
(
  select distinct rogers_ecid, shaw_master_party_id, RULESET_TYPE, min (RULESET_ID) as DET_RULESET_ID
  from matchedAccountView
  where rogers_ecid is not null
  and RULESET_TYPE="DETERMINISTIC"
  and (rogers_account_status in ('o', 's', 't') or rogers_account_status in (2,3,4,7))
  and shaw_account_status in ('active', 'soft suspended')
  group by rogers_ecid, shaw_master_party_id, RULESET_TYPE
),
noFuzzyMatches as
(
  select distinct rogers_ecid, shaw_master_party_id, concat_ws("=", rogers_ecid, shaw_master_party_id) as concat_id
  from matchedEntityView
  where DET_MATCH_IND="Y" and (FUZZY_MATCH_IND = "N" or PROB_MATCH_IND="N")
)

select distinct  
M.rogers_ecid, M.shaw_master_party_id as shaw_id, A.Rogers_account_id, A.shaw_account_id, A.rogers_account_status as rogers_status, A.shaw_account_status as shaw_status, Active.DET_RULESET_ID
, r.first_name, s.first_name, r.last_name, s.last_name
, r.city, s.city, r.state, s.state, r.zipcode, s.zipcode
from matchedEntityView M
INNER JOIN matchedAccountView A ON M.ROGERS_ECID = A.ROGERS_ECID AND M.SHAW_MASTER_PARTY_ID = A.SHAW_MASTER_PARTY_ID AND M.best_det_match_ruleset_id = A.RULESET_ID
INNER JOIN cl_rogers_contact r ON A.ROGERS_ECID=r.rcis_id AND A.Rogers_account_id = r.fa_id
INNER JOIN cl_shaw_contact s ON A.SHAW_MASTER_PARTY_ID=s.rcis_id_cl AND A.shaw_account_id = s.fa_id
INNER JOIN activeAccounts Active on M.rogers_ecid=Active.rogers_ecid and M.shaw_master_party_id=Active.shaw_master_party_id 

where (DET_MATCH_IND="Y" and FUZZY_MATCH_IND = "N" and PROB_MATCH_IND="N")
AND (SUBSTRING(r.first_name,1,1) = SUBSTRING(s.first_name,1,1)) AND
r.last_name = s.last_name AND r.zipcode=s.zipcode AND r.city = s.city AND r.state = s.state
AND concat_ws("=", M.rogers_ecid, M.shaw_master_party_id) in (select distinct concat_id from noFuzzyMatches)


-- COMMAND ----------

-- New data has inactive accounts excluded
with 
activeAccounts as
(
  select distinct rogers_ecid, shaw_master_party_id, RULESET_TYPE, min (RULESET_ID) as DET_RULESET_ID
  from matchedAccountView
  where rogers_ecid is not null
  and RULESET_TYPE="DETERMINISTIC"
  and (rogers_account_status in ('o', 's', 't') or rogers_account_status in (2,3,4,7))
  and shaw_account_status in ('active', 'soft suspended')
  group by rogers_ecid, shaw_master_party_id, RULESET_TYPE
),
noFuzzyMatches as
(
  select distinct rogers_ecid, shaw_master_party_id, concat_ws("=", rogers_ecid, shaw_master_party_id) as concat_id
  from matchedEntityView
  where DET_MATCH_IND="Y" and (FUZZY_MATCH_IND = "N" or PROB_MATCH_IND="N")
)

select distinct  

M.rogers_ecid, M.shaw_master_party_id as shaw_id, A.Rogers_account_id, A.shaw_account_id, A.rogers_account_status as rogers_status, A.shaw_account_status as shaw_status, Active.DET_RULESET_ID
, r.first_name, s.first_name, r.last_name, s.last_name
, r.city, s.city, r.state, s.state, r.zipcode, s.zipcode
from matchedEntityView M
INNER JOIN matchedAccountView A ON M.ROGERS_ECID = A.ROGERS_ECID AND M.SHAW_MASTER_PARTY_ID = A.SHAW_MASTER_PARTY_ID AND M.best_det_match_ruleset_id = A.RULESET_ID
INNER JOIN cl_rogers_contact r ON A.ROGERS_ECID=r.rcis_id AND A.Rogers_account_id = r.fa_id
INNER JOIN cl_shaw_contact s ON A.SHAW_MASTER_PARTY_ID=s.rcis_id_cl AND A.shaw_account_id = s.fa_id

inner join activeAccounts Active on M.rogers_ecid=Active.rogers_ecid and M.shaw_master_party_id=Active.shaw_master_party_id 

where (DET_MATCH_IND="Y" and FUZZY_MATCH_IND = "N" and PROB_MATCH_IND="N")
AND (SUBSTRING(r.first_name,1,1) = SUBSTRING(s.first_name,1,1)) AND
r.last_name = s.last_name
-- AND r.zipcode=s.zipcode
AND r.city = s.city
AND r.state = s.state
-- AND cl_rogers_contact.first_name != cl_shaw_contact.first_name
AND concat_ws("=", M.rogers_ecid, M.shaw_master_party_id) in (select distinct concat_id from noFuzzyMatches)


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
AND cl_rogers_contact.first_name != cl_shaw_contact.first_name
--group by DET_MATCH_IND, FUZZY_MATCH_IND, PROB_MATCH_IND, best_det_match_ruleset_id, best_fuzzy_match_ruleset_id, best_prob_match_ruleset_id, r.ruleset_type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###TC-E-06

-- COMMAND ----------

-- New data has inactive accounts excluded
with 
activeAccounts as
(
  select distinct rogers_ecid, shaw_master_party_id, RULESET_TYPE, min (RULESET_ID) as DET_RULESET_ID
  from matchedAccountView
  where rogers_ecid is not null
  and RULESET_TYPE="DETERMINISTIC"
  and (rogers_account_status in ('o', 's', 't') or rogers_account_status in (2,3,4,7))
  and shaw_account_status in ('active', 'soft suspended')
  group by rogers_ecid, shaw_master_party_id, RULESET_TYPE
),
RulesetType as
(
  select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Type  
  from rulesetView
)

select distinct  

R1.Type as DET_TYPE
-- , R2.Type as fuzzy_TYPE
, R3.Type as prob_TYPE
, count (*) as count
from matchedEntityView M
left join activeAccounts A on M.rogers_ecid=A.rogers_ecid and M.shaw_master_party_id=A.shaw_master_party_id 
left join RulesetType R1 on A.DET_RULESET_ID=R1.ruleset_id
-- left join RulesetType R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
left join RulesetType R3 on M.BEST_prob_MATCH_RULESET_ID=R3.ruleset_id

where (R1.Type is not null or R3.Type is not null)
-- rogers_ecid in (select * from activeRogers)
-- and shaw_master_party_id in (select * from activeShaw)
group by DET_TYPE
-- , fuzzy_TYPE
, prob_TYPE


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##High match rate

-- COMMAND ----------

with matchCount as
(
select  
rogers_ecid
, count (distinct shaw_master_party_id) as matchRate
 from matchedentityView
 where (DET_MATCH_IND="N" or DET_MATCH_IND is null) and shaw_master_party_id is not null and rogers_ecid is not null
 group by rogers_ecid
 )
 select 
   count (distinct rogers_ecid) as Number_of_Rogers_Customers_with_High_Matches
   , matchRate as match_rate_to_Shaw_Customers
from matchCount C
group by matchRate
order by matchRate desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Further Data Exploration

-- COMMAND ----------

select RULESET_TYPE, min(RULESET_ATTR), max (RULESET_ATTR) from matchedAccountView
where RULESET_TYPE in ("FUZZY", "PROBABILISTIC")
group by RULESET_TYPE

-- COMMAND ----------

select min(CUSTOMER_FUZZY_SCORE), min(CUSTOMER_PROB_CONFIDENCE_LVL) from matchedEntityView

-- COMMAND ----------

select distinct DET_MATCH_IND, FUZZY_MATCH_IND, PROB_MATCH_IND
from matchedEntityView
where DET_MATCH_IND="Y" and (FUZZY_MATCH_IND="N" or PROB_MATCH_IND="N")

-- COMMAND ----------

select * from matchedEntityView
where DET_MATCH_IND="Y" and (FUZZY_MATCH_IND="N" or PROB_MATCH_IND="N")

-- COMMAND ----------

select count(shaw_master_party_id) from matchedEntityView
where shaw_master_party_id is not null and DET_MATCH_IND='YES'

-- COMMAND ----------

select count(shaw_master_party_id) from matchedEntityView
where shaw_master_party_id is not null 
and rogers_ecid is not null
and DET_MATCH_IND='YES'

-- COMMAND ----------

select rcis_id_cl 
shaw_direct_flag, WIRELINE_PHONE_FLAG, VIDEO_FLAG
  ,case when shaw_direct_flag='YES' then 1 else 0 end shaw_direct
  ,case when INTERNET_FLAG='YES' or WIRELINE_PHONE_FLAG='YES' or VIDEO_FLAG='YES' then 1 else 0 end shaw_CH
, province
from cl_shaw_wireline

-- COMMAND ----------

with customersWithBothAccounts
(
select rcis_id_cl 
  ,case when shaw_direct_flag='Y' then 1 else 0 end shaw_direct
  ,case when INTERNET_FLAG='Y' or WIRELINE_PHONE_FLAG='Y' or VIDEO_FLAG='Y' then 1 else 0 end shaw_CH
, province
from cl_shaw_wireline

)

select rcis_id_cl, sum (shaw_direct), sum (shaw_CH), count (distinct province) as provinceCount
from customersWithBothAccounts  C
--  where C.shaw_direct>1 and  C.shaw_CH>1
-- where rcis_id_cl=18715165
group by C.rcis_id_cl
order by provinceCount desc


-- COMMAND ----------

select rcis_id_cl,fa_id, shaw_direct_flag, INTERNET_FLAG, WIRELINE_PHONE_FLAG, VIDEO_FLAG, province   from cl_shaw_wireline
where rcis_id_cl in (7250198, 21438140, 2821197,1821495, 6558406, 1574833)
order by rcis_id_cl


-- COMMAND ----------


