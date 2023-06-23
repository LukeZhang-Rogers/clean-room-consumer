-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Address Configuration

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC MatchedEntity = spark.read.format("parquet").load("/mnt/development/Processed/InterimOutput/05_19/entiredataset_matchedentity_withblocking_withthreshold_on_each_rule_activeonly")
-- MAGIC AccountMatchedRuleset = spark.read.format("parquet").load("/mnt/development/Processed/InterimOutput/05_19/entiredataset_accountmatchedruleset_withblocking_withthreshold_on_each_rule_activeonly")
-- MAGIC
-- MAGIC rulesets = spark.read.format("parquet").load("/mnt/development/Consumption/RulesetNew")
-- MAGIC
-- MAGIC
-- MAGIC r_wls_c_joined_df = spark.read.format("parquet").load("/mnt/development/InterimOutput/Rogers/WirelessAndContact")
-- MAGIC r_wln_c_joined_df = spark.read.format("parquet").load("/mnt/development/InterimOutput/Rogers/WirelineAndContact")
-- MAGIC s_wls_c_joined_df = spark.read.format("parquet").load("/mnt/development/InterimOutput/Shaw/WirelessAndContact")
-- MAGIC s_wln_c_joined_df = spark.read.format("parquet").load("/mnt/development/InterimOutput/Shaw/WirelineAndContact")
-- MAGIC
-- MAGIC shaw_consumer_wireline_account = spark.read.format("parquet").load("/mnt/shawsaprod/shaw-snflk-dir-01/SHAW/20220407_CONSUMER_WIRELINE_ACCOUNT")
-- MAGIC shaw_consumer_contact = spark.read.format("parquet").load("/mnt/shawsaprod/shaw-snflk-dir-01/SHAW/20220407_CONSUMER_CONTACT")
-- MAGIC shaw_consumer_wireless_account = spark.read.format("parquet").load("/mnt/shawsaprod/shaw-snflk-dir-01/SHAW/20220407_CONSUMER_WIRELESS_ACCOUNT")
-- MAGIC
-- MAGIC
-- MAGIC rogers_contact = spark.read.format("parquet").load("/mnt/rogerssaprod/deloitte-cr/rogers_0414/20220414_rogers_contact")
-- MAGIC rogers_wireless_account = spark.read.format("parquet").load("/mnt/rogerssaprod/deloitte-cr/rogers_0414/20220414_rogers_wireless_account")
-- MAGIC rogers_wireline_account = spark.read.format("parquet").load("/mnt/rogerssaprod/deloitte-cr/rogers_0414/20220414_rogers_wireline_account")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Build Views

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #match output
-- MAGIC AccountMatchedRuleset.createOrReplaceTempView("matchedAccountView")
-- MAGIC MatchedEntity.createOrReplaceTempView("matchedEntityView")
-- MAGIC rulesets.createOrReplaceTempView("rulesetView")
-- MAGIC
-- MAGIC r_wls_c_joined_df.createOrReplaceTempView("r_wireless")
-- MAGIC r_wln_c_joined_df.createOrReplaceTempView("r_wireline")
-- MAGIC s_wls_c_joined_df.createOrReplaceTempView("s_wireless")
-- MAGIC s_wln_c_joined_df.createOrReplaceTempView("s_wireline")
-- MAGIC
-- MAGIC shaw_consumer_contact.createOrReplaceTempView("raw_shaw_contact")
-- MAGIC shaw_consumer_wireline_account.createOrReplaceTempView("raw_shaw_wireline")
-- MAGIC rogers_contact.createOrReplaceTempView("raw_rogers_contact")
-- MAGIC rogers_wireline_account.createOrReplaceTempView("raw_rogers_wireline")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Matched vs Unmatched Customers Count

-- COMMAND ----------

select distinct ruleset_type from matchedAccountView where (shaw_master_party_id is null or rogers_ecid is null)

-- COMMAND ----------

-- Unmatched Active customers
  select 
   case when RULESET_TYPE = "UNMATCHED"  then "UNMATCHED" else "MATCHED" end Type 
  , count(distinct shaw_master_party_id)
  from matchedAccountView
  where shaw_master_party_id is not null
--   ruleset_type="DETERMINISTIC" and
  and shaw_account_status in ('active', 'soft suspended')
  group by Type



-- COMMAND ----------

-- proof that a matched account can appear in unmatched results as well
with 
matchedRogers as
(
  select distinct rogers_ecid
  from matchedAccountView
  where rogers_ecid is not null
  and ruleset_type!="UNMATCHED" 
)
,unmatchedRogers as
(
  select distinct rogers_ecid
  from matchedAccountView
  where rogers_ecid is not null
  and ruleset_type="UNMATCHED" 
)
select count(*) from matchedRogers where rogers_ecid in (select rogers_ecid from  unmatchedRogers)

-- COMMAND ----------

-- Total Active customers: At the time of executing the query, the inactive accounts are not excluded in the matched results
with 
activeRogers as
(
  select distinct rogers_ecid
  from matchedAccountView
  where rogers_ecid is not null
--   ruleset_type="DETERMINISTIC" and
  and rogers_account_status in ('o', 's', 't') or rogers_account_status in (2,3,4,7)
)
,activeShaw as
(
  select distinct shaw_master_party_id
  from matchedAccountView
  where  rogers_ecid is not null
--   ruleset_type="DETERMINISTIC" and
  and shaw_account_status in ('active', 'soft suspended')
)

select count(distinct rogers_ecid), count(distinct shaw_master_party_id)
from matchedEntityView
where 
(DET_MATCH_IND="N" and DET_MATCH_IND="N" and DET_MATCH_IND="N")
and rogers_ecid in (select * from activeRogers)
and shaw_master_party_id in (select * from activeShaw)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Breadown by Rules

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Deterministic only

-- COMMAND ----------

select best_det_match_ruleset_id, count(*)
from matchedEntityView
where 
DET_MATCH_IND="Y" 
group by best_det_match_ruleset_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###RulesetID breakdown-DET vs Fuzzy vs Prob

-- COMMAND ----------

-- Entity Matched
select distinct DET_MATCH_IND, FUZZY_MATCH_IND, PROB_MATCH_IND, count (*)
from matchedEntityView
group by DET_MATCH_IND, FUZZY_MATCH_IND, PROB_MATCH_IND

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Address vs Customer Level - DET vs Fuzzy vs Prob

-- COMMAND ----------

-- Stats on all accounts
with 
RulesetType as
(
  select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"
  end Type  
  from rulesetView
)

select distinct  R1.Type as DET_TYPE, R2.Type as fuzzy_TYPE, R3.Type as prob_TYPE
, count (*)
from matchedEntityView M
left join RulesetType R1 on M.BEST_DET_MATCH_RULESET_ID=R1.ruleset_id
left join RulesetType R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
left join RulesetType R3 on M.BEST_prob_MATCH_RULESET_ID=R3.ruleset_id
 
group by DET_TYPE, fuzzy_TYPE, prob_TYPE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Household Level vs Customer Level - DET vs Fuzzy vs Prob

-- COMMAND ----------

-- Stats on all accounts - Diffrentiate btwn Customer Level and Household level deterministic matches
with 
RulesetType as
(
  select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "FALSE" then "3-Address lvl" 
        when RULESET_ID<30 then "1-Customer lvl" 
        else "2-Household lvl"
  end Type
  from rulesetView
)

select distinct  R1.Type as DET_TYPE, R2.Type as fuzzy_TYPE, R3.Type as prob_TYPE
, count (*)
from matchedEntityView M
left join RulesetType R1 on M.BEST_DET_MATCH_RULESET_ID=R1.ruleset_id
left join RulesetType R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
left join RulesetType R3 on M.BEST_prob_MATCH_RULESET_ID=R3.ruleset_id
 
group by DET_TYPE, fuzzy_TYPE, prob_TYPE

-- COMMAND ----------

-- Get the count of matches for each deterministic ruleset
select DET_match_ind, BEST_DET_MATCH_RULESET_ID
, case when R1.IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL" end Type
, R1.RULESET_DESCRIPTION
, count (*)
from matchedEntityView M
left join RulesetView R1 on M.BEST_DET_MATCH_RULESET_ID=R1.ruleset_id
where DET_match_ind = "Y"
group by DET_match_ind, BEST_DET_MATCH_RULESET_ID, Type, R1.RULESET_DESCRIPTION

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Account Type, Rulesets as Variables

-- COMMAND ----------

-- Breakdown of matche of ACCOUNTS (and not unique customers) per rules to evaluate the relationship between parent-child rules
-- Broken down by account types 
with matchedResults
as
(
  select 
  
  case when ruleset_id=1 then 1 else 0 end Rule01
  ,case when ruleset_id=2 then 1 else 0 end Rule02
  ,case when ruleset_id=3 then 1 else 0 end Rule03
  ,case when ruleset_id=4 then 1 else 0 end Rule04  
  ,case when ruleset_id=5 then 1 else 0 end Rule05
  ,case when ruleset_id=6 then 1 else 0 end Rule06
  ,case when ruleset_id=7 then 1 else 0 end Rule07
  ,case when ruleset_id=8 then 1 else 0 end Rule08
  ,case when ruleset_id=9 then 1 else 0 end Rule09
  ,case when ruleset_id=10 then 1 else 0 end Rule10
  ,case when ruleset_id=11 then 1 else 0 end Rule11
  ,case when ruleset_id=12 then 1 else 0 end Rule12
  ,case when ruleset_id=13 then 1 else 0 end Rule13
  ,case when ruleset_id=14 then 1 else 0 end Rule14  
  ,case when ruleset_id=15 then 1 else 0 end Rule15
  ,case when ruleset_id=30 then 1 else 0 end Rule30
  ,case when ruleset_id=31 then 1 else 0 end Rule31
  ,case when ruleset_id=32 then 1 else 0 end Rule32
    
 ,  M. rogers_account_type, M. shaw_account_type
  
  from matchedAccountView M
  where ruleset_type="deter" and rogers_ecid is not null
  
  )
  
  select   M. rogers_account_type, M. shaw_account_type,
  sum (M.rule01) as R01, sum (M.rule02) as R02, sum (M.rule03) as R03, sum (M.rule04) as R04, sum (M.rule05) as R05
, sum (M.rule06) as R06, sum (M.rule07) as R07, sum (M.rule08) as R08, sum (M.rule09) as R09, sum (M.rule10) as R10
, sum (M.rule11) as R11, sum (M.rule12) as R12, sum (M.rule13) as R13, sum (M.rule14) as R14, sum (M.rule15) as R15
, sum (M.rule30) as R30, sum (M.rule31) as R31, sum (M.rule32) as R32
from matchedResults M
 group by M.rogers_account_type, M.shaw_account_type
order by R01, R02, R03, R04, R05, R06, R07, R08, R09, R10, R11, R12, R13, R14, R15 desc

-- COMMAND ----------

-- Breakdown of matche of ACCOUNTS (and not unique customers) per rules to evaluate the relationship between parent-child rules

with matchedResults
as
(
  select 
  
  distinct M.rogers_ECID, M.shaw_master_party_id
  ,case when ruleset_id=1 then 1 else 0 end Rule01
  ,case when ruleset_id=2 then 1 else 0 end Rule02
  ,case when ruleset_id=3 then 1 else 0 end Rule03
  ,case when ruleset_id=4 then 1 else 0 end Rule04  
  ,case when ruleset_id=5 then 1 else 0 end Rule05
  ,case when ruleset_id=6 then 1 else 0 end Rule06
  ,case when ruleset_id=7 then 1 else 0 end Rule07
  ,case when ruleset_id=8 then 1 else 0 end Rule08
  ,case when ruleset_id=9 then 1 else 0 end Rule09
  ,case when ruleset_id=10 then 1 else 0 end Rule10
  ,case when ruleset_id=11 then 1 else 0 end Rule11
  ,case when ruleset_id=12 then 1 else 0 end Rule12
  ,case when ruleset_id=13 then 1 else 0 end Rule13
  ,case when ruleset_id=14 then 1 else 0 end Rule14  
  ,case when ruleset_id=15 then 1 else 0 end Rule15
  ,case when ruleset_id=30 then 1 else 0 end Rule30
  ,case when ruleset_id=31 then 1 else 0 end Rule31
  ,case when ruleset_id=32 then 1 else 0 end Rule32
    

  
  from matchedAccountView M
  where ruleset_type="DETERMINISTIC" and rogers_ecid is not null
  
  )
  
  select 
--  M. rogers_account_type, M. shaw_account_type,
  sum (M.rule01) as R01, sum (M.rule02) as R02, sum (M.rule03) as R03, sum (M.rule04) as R04, sum (M.rule05) as R05
, sum (M.rule06) as R06, sum (M.rule07) as R07, sum (M.rule08) as R08, sum (M.rule09) as R09, sum (M.rule10) as R10
, sum (M.rule11) as R11, sum (M.rule12) as R12, sum (M.rule13) as R13, sum (M.rule14) as R14, sum (M.rule15) as R15
, sum (M.rule30) as R30, sum (M.rule31) as R31, sum (M.rule32) as R32
from matchedResults M
-- group by M.rogers_account_type, M.shaw_account_type
order by R01, R02, R03, R04, R05, R06, R07, R08, R09, R10, R11, R12, R13, R14, R15 desc

-- COMMAND ----------


