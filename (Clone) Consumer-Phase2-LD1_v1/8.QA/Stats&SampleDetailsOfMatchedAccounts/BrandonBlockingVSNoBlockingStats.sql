-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Address Configuration

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC rulesets_df = spark.read.format("parquet").load("/mnt/development/referencedata/rulesets")
-- MAGIC
-- MAGIC #Load dataframes and build views
-- MAGIC entitymatched_Brandon_blocking_df = spark.read.format("parquet").load("/mnt/development/Processed/InterimOutput/05_12/brandon_matchedentity_withblocking_withthreshold_on_each_rule")
-- MAGIC accountmatched_Brandon_blocking_df = spark.read.format("parquet").load("/mnt/development/Processed/InterimOutput/05_12/brandon_accountmatchedruleset_withblocking_withthreshold_on_each_rule")
-- MAGIC
-- MAGIC entitymatched_Brandon_no_blocking_df = spark.read.format("parquet").load("/mnt/development/Processed/InterimOutput/05_12/brandon_matchedentity_noblocking_withthreshold_on_each_rule")
-- MAGIC accountmatched_Brandon_no_blocking_df = spark.read.format("parquet").load("/mnt/development/Processed/InterimOutput/05_12/brandon_accountmatchedruleset_noblocking_withthreshold_on_each_rule")
-- MAGIC
-- MAGIC # entitymatched_PEI_blocking_df= spark.read.format("parquet").load("/mnt/development/Processed/InterimOutput/05_10/MatchedEntity_withblocking_exclude_pe_inactive")
-- MAGIC # accountmatched_PEI_blocking_df = spark.read.format("parquet").load("/mnt/development/Processed/InterimOutput/05_10/AccountMatchedRuleset_withblocking_exclude_pe_inactive")
-- MAGIC
-- MAGIC
-- MAGIC # entitymatched_PEI_no_blocking_df= spark.read.format("parquet").load("/mnt/development/Processed/InterimOutput/05_06/MatchedEntity_no_blocking_pe")
-- MAGIC # accountmatched_PEI_no_blocking_df = spark.read.format("parquet").load("/mnt/development/Processed/InterimOutput/05_06/AccountMatchedRuleset_no_blocking_pe")
-- MAGIC
-- MAGIC
-- MAGIC rulesets_df.createOrReplaceTempView("rulesetView")
-- MAGIC entitymatched_Brandon_blocking_df.createOrReplaceTempView("entitymatched_Bran_block")
-- MAGIC accountmatched_Brandon_blocking_df.createOrReplaceTempView("accountmatched_Bran_block")
-- MAGIC
-- MAGIC entitymatched_Brandon_no_blocking_df.createOrReplaceTempView("entitymatched_Bran_no_block")
-- MAGIC accountmatched_Brandon_no_blocking_df.createOrReplaceTempView("accountmatched_Bran_no_block")
-- MAGIC
-- MAGIC # entitymatched_PEI_blocking_df.createOrReplaceTempView("entitymatched_pei_block")
-- MAGIC # accountmatched_PEI_blocking_df.createOrReplaceTempView("accountmatched_pei_block")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Brandon

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Stats With blocking Brandon

-- COMMAND ----------

-- exclude deterministic inactive accounts on customer level
with 

RulesetType as
(
  select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Type  
  from rulesetView
)


select distinct  R1.Type as DET_TYPE, R2.Type as fuzzy_TYPE, R3.Type as prob_TYPE
, count (*) as count
from entitymatched_Bran_block M
left join RulesetType R1 on M.BEST_DET_MATCH_RULESET_ID=R1.ruleset_id
left join RulesetType R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
left join RulesetType R3 on M.BEST_prob_MATCH_RULESET_ID=R3.ruleset_id

group by DET_TYPE, fuzzy_TYPE, prob_TYPE

-- COMMAND ----------

-- exclude deterministic inactive accounts on account level
with 

-- Later recived data for active accounts only. Hence no need to exclude those results in query
-- activeAccounts as
-- (
--   select distinct rogers_ecid, shaw_master_party_id, RULESET_TYPE, min (RULESET_ID) as DET_RULESET_ID
--   from accountmatched_Bran_block
--   where rogers_ecid is not null
--   and RULESET_TYPE="DETERMINISTIC"
--   and (rogers_account_status in ('o', 's', 't') or rogers_account_status in (2,3,4,7))
--   and shaw_account_status in ('active', 'soft suspended')
--   group by rogers_ecid, shaw_master_party_id, RULESET_TYPE
-- )

RulesetType as
(
  select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Type  
  from rulesetView
)

select distinct  R1.Type as DET_TYPE, R2.Type as fuzzy_TYPE, R3.Type as prob_TYPE
, count (*) as count
from entitymatched_Bran_block M
left join activeAccounts A on M.rogers_ecid=A.rogers_ecid and M.shaw_master_party_id=A.shaw_master_party_id 
left join RulesetType R1 on A.DET_RULESET_ID=R1.ruleset_id
left join RulesetType R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
left join RulesetType R3 on M.BEST_prob_MATCH_RULESET_ID=R3.ruleset_id

-- Later recived data for active accounts only. Hence no need to exclude those results in query
-- where 
-- rogers_ecid in (select rogers_ecid from activeRogers)
-- and shaw_master_party_id in (select * from activeShaw)
group by DET_TYPE, fuzzy_TYPE, prob_TYPE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Without blocking Brandon

-- COMMAND ----------

-- New data has inactive accounts excluded
with 
activeRogers as
(
  select distinct rogers_ecid
  from accountmatched_Bran_no_block
  where rogers_ecid is not null
  and (rogers_account_status in ('o', 's', 't') or rogers_account_status in (2,3,4,7))
)
,activeShaw as
(
  select distinct shaw_master_party_id
  from accountmatched_Bran_no_block
  where rogers_ecid is not null
  and shaw_account_status in ('active', 'soft suspended')
),
RulesetType as
(
  select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Type  
  from rulesetView
)


select distinct  R1.Type as DET_TYPE, R2.Type as fuzzy_TYPE, R3.Type as prob_TYPE
, count (*) as count
from entitymatched_Bran_no_block M
left join RulesetType R1 on M.BEST_DET_MATCH_RULESET_ID=R1.ruleset_id
left join RulesetType R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
left join RulesetType R3 on M.BEST_prob_MATCH_RULESET_ID=R3.ruleset_id

where 
rogers_ecid in (select * from activeRogers)
and shaw_master_party_id in (select * from activeShaw)
group by DET_TYPE, fuzzy_TYPE, prob_TYPE

-- COMMAND ----------

select * from rulesetView

-- COMMAND ----------

 select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Type  
  from rulesetView

-- COMMAND ----------

with RulesetType as
(
  select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Type  
  from rulesetView
)
select R2.RULESET_ID as Fuzzy_rule, R3.RULESET_ID as Prob_rule, count(*)
from entitymatched_Bran_no_block M
left join RulesetType R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
left join RulesetType R3 on M.BEST_prob_MATCH_RULESET_ID=R3.ruleset_id
where (DET_MATCH_IND is null or DET_MATCH_IND="N" )
and R2.Type="CUSTOMER LEVEL"
and R3.Type="ADDRESS LEVEL"
group by R2.RULESET_ID, R3.RULESET_ID

-- COMMAND ----------

select shaw_master_party_id, count (rogers_ecid) as count 
from entitymatched_Bran_no_block M
group by shaw_master_party_id
order by count desc

-- COMMAND ----------

select rogers_ecid, count (shaw_master_party_id) as count 
from entitymatched_Bran_no_block M
group by rogers_ecid
order by count desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####excluding inactive deterministic accounts

-- COMMAND ----------

-- exclude deterministic inactive accounts on customer level
with 

activeAccounts as
(
  select distinct rogers_ecid, shaw_master_party_id, RULESET_TYPE, min (RULESET_ID) as DET_RULESET_ID
  from accountmatched_Bran_no_block
  where rogers_ecid is not null
  and RULESET_TYPE="DETERMINISTIC"
  and (rogers_account_status in ('o', 's', 't') or rogers_account_status in (2,3,4,7))
  and shaw_account_status in ('active', 'soft suspended')
  group by rogers_ecid, shaw_master_party_id, RULESET_TYPE
)

,RulesetType as
(
  select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Type  
  from rulesetView
)

select distinct  R1.Type as DET_TYPE, R2.Type as fuzzy_TYPE, R3.Type as prob_TYPE
, count (*) as count
from entitymatched_Bran_no_block M
left join activeAccounts A on M.rogers_ecid=A.rogers_ecid and M.shaw_master_party_id=A.shaw_master_party_id 
left join RulesetType R1 on A.DET_RULESET_ID=R1.ruleset_id
left join RulesetType R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
left join RulesetType R3 on M.BEST_prob_MATCH_RULESET_ID=R3.ruleset_id

-- where 
-- rogers_ecid in (select rogers_ecid from activeRogers)
-- and shaw_master_party_id in (select * from activeShaw)
group by DET_TYPE, fuzzy_TYPE, prob_TYPE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Get sample Ids

-- COMMAND ----------

-- exclude deterministic inactive accounts on account level
with 

RulesetType as
(
  select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Type  
  from rulesetView
)

select  M.rogers_ecid, M.shaw_master_party_id, M.BEST_fuzzy_MATCH_RULESET_ID, M.BEST_Prob_MATCH_RULESET_ID
-- , R2.Type as fuzzy_TYPE, R3.Type as prob_TYPE

from entitymatched_Bran_no_block M

left join RulesetType R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
left join RulesetType R3 on M.BEST_prob_MATCH_RULESET_ID=R3.ruleset_id

where BEST_DET_MATCH_RULESET_ID is null and rogers_ecid is not null and shaw_master_party_id is not null


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Compare blocking to no blocking

-- COMMAND ----------

-- Assess combination of matches on both strategy
with 
activeRogers as
(
  select distinct rogers_ecid
  from accountmatched_Bran_no_block
  where rogers_ecid is not null
  and (rogers_account_status in ('o', 's', 't') or rogers_account_status in (2,3,4,7))
)
,activeShaw as
(
  select distinct shaw_master_party_id
  from accountmatched_Bran_no_block
  where rogers_ecid is not null
  and shaw_account_status in ('active', 'soft suspended')
)
,
RulesetType as
(
  select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Type  
  from rulesetView
)

select distinct  
-- case when M2.DET_MATCH_IND is null and M2.FUZZY_MATCH_IND is null and M2.PROB_MATCH_IND is null then "No Blocking" else "Both"  end Source 

case when R2_1.Type is null and R2_2.Type is null and R2_3.Type is null then "No Blocking" else "Both"  end Source 
, R1.Type as DET_TYPE, R2.Type as fuzzy_TYPE, R3.Type as prob_TYPE
, R2_1.Type as DET_TYPE_BLOCKING, R2_2.Type as fuzzy_TYPE_BLOCKING, R2_3.Type as prob_TYPE_BLOCKING

-- , 
-- M.DET_MATCH_IND as Det, M.FUZZY_MATCH_IND as Fuzzy, M.PROB_MATCH_IND as Prob
-- , M2.DET_MATCH_IND as Det_with_blocking, M2.FUZZY_MATCH_IND as Fuzzy_with_blocking, M2.PROB_MATCH_IND as Prob_with_blocking
, count (*) as count
from entitymatched_Bran_no_block M

left join RulesetType R1 on M.BEST_DET_MATCH_RULESET_ID=R1.ruleset_id
left join RulesetType R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
left join RulesetType R3 on M.BEST_prob_MATCH_RULESET_ID=R3.ruleset_id

left join entitymatched_Bran_block M2
on M.rogers_ecid = M2.rogers_ecid and M.shaw_master_party_id = M2.shaw_master_party_id
left join RulesetType R2_1 on M2.BEST_DET_MATCH_RULESET_ID=R2_1.ruleset_id
left join RulesetType R2_2 on M2.BEST_fuzzy_MATCH_RULESET_ID=R2_2.ruleset_id
left join RulesetType R2_3 on M2.BEST_prob_MATCH_RULESET_ID=R2_3.ruleset_id

where 
M.rogers_ecid in (select * from activeRogers)
and M.shaw_master_party_id in (select * from activeShaw)
group by 
DET_TYPE, fuzzy_TYPE, prob_TYPE, DET_TYPE_BLOCKING, fuzzy_TYPE_BLOCKING, prob_TYPE_BLOCKING
-- Det, Fuzzy, Prob, Det_with_blocking, Fuzzy_with_blocking, Prob_with_blocking

-- COMMAND ----------

-- Ensure no matches exist in blocking results where it's not available in no blocking results
-- There is no such cases beacuse source of all records after join is Both

with 
activeRogers as
(
  select distinct rogers_ecid
  from accountmatched_Bran_no_block
  where rogers_ecid is not null
  and (rogers_account_status in ('o', 's', 't') or rogers_account_status in (2,3,4,7))
)
,activeShaw as
(
  select distinct shaw_master_party_id
  from accountmatched_Bran_no_block
  where rogers_ecid is not null
  and shaw_account_status in ('active', 'soft suspended')
)
,
RulesetType as
(
  select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Type  
  from rulesetView
)
-- , distinctIdsBlock as
-- (
-- distinct (concat_ws("-", rogers_ecid, shaw_master_party_id)), BEST_DET_MATCH_RULESET_ID, BEST_fuzzy_MATCH_RULESET_ID, BEST_prob_MATCH_RULESET_ID
-- from entitymatched_Bran_block
-- where rogers_ecid is not null and shaw_master_party_id is not null
-- )

select distinct  
-- case when M2.DET_MATCH_IND is null and M2.FUZZY_MATCH_IND is null and M2.PROB_MATCH_IND is null then "No Blocking" else "Both"  end Source 

case when R2_1.Type is null and R2_2.Type is null and R2_3.Type is null then "Blocking" else "Both"  end Source 
, R1.Type as DET_TYPE, R2.Type as fuzzy_TYPE, R3.Type as prob_TYPE
, R2_1.Type as DET_TYPE_BLOCKING, R2_2.Type as fuzzy_TYPE_BLOCKING, R2_3.Type as prob_TYPE_BLOCKING

-- , 
-- M.DET_MATCH_IND as Det, M.FUZZY_MATCH_IND as Fuzzy, M.PROB_MATCH_IND as Prob
-- , M2.DET_MATCH_IND as Det_with_blocking, M2.FUZZY_MATCH_IND as Fuzzy_with_blocking, M2.PROB_MATCH_IND as Prob_with_blocking
, count (*) as count
from entitymatched_Bran_block M
left join RulesetType R1 on M.BEST_DET_MATCH_RULESET_ID=R1.ruleset_id
left join RulesetType R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
left join RulesetType R3 on M.BEST_prob_MATCH_RULESET_ID=R3.ruleset_id

left join entitymatched_Bran_no_block M2
on M.rogers_ecid = M2.rogers_ecid and M.shaw_master_party_id = M2.shaw_master_party_id
left join RulesetType R2_1 on M2.BEST_DET_MATCH_RULESET_ID=R2_1.ruleset_id
left join RulesetType R2_2 on M2.BEST_fuzzy_MATCH_RULESET_ID=R2_2.ruleset_id
left join RulesetType R2_3 on M2.BEST_prob_MATCH_RULESET_ID=R2_3.ruleset_id

where 
M.rogers_ecid in (select * from activeRogers)
and M.shaw_master_party_id in (select * from activeShaw)
group by 
DET_TYPE, fuzzy_TYPE, prob_TYPE, DET_TYPE_BLOCKING, fuzzy_TYPE_BLOCKING, prob_TYPE_BLOCKING
-- Det, Fuzzy, Prob, Det_with_blocking, Fuzzy_with_blocking, Prob_with_blocking

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####All pairs in blocking vs no blocking

-- COMMAND ----------

-- Assess combination of matches on both strategy
with 

allPairs as
(
  select distinct rogers_ecid, shaw_master_party_id
  from accountmatched_Bran_no_block
  where rogers_ecid is not null and shaw_master_party_id is not null
  
  union 
  
  select distinct rogers_ecid, shaw_master_party_id
  from accountmatched_Bran_block
  where rogers_ecid is not null and shaw_master_party_id is not null

)
,
RulesetType as
(
  select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Type  
  from rulesetView
)


select distinct  

case when (R1_2.Type is null and R1_3.Type is null) then "Only in Blocking" 
     when (R2_2.Type is null and R2_3.Type is null) then "Only in Whole" 
     else "Both"  
end Source 

, R1_1.Type as DET
, R1_2.Type as WHOLE_fuzzy, R1_3.Type as WHOLE_prob
, R2_2.Type as BLOCK_fuzzy, R2_3.Type as BLOCK_prob

, count (*) as count
from allPairs P
left join entitymatched_Bran_no_block M1 on P.rogers_ecid=M1.rogers_ecid and P.shaw_master_party_id=M1.shaw_master_party_id 
left join RulesetType R1_1 on M1.BEST_DET_MATCH_RULESET_ID=R1_1.ruleset_id
left join RulesetType R1_2 on M1.BEST_fuzzy_MATCH_RULESET_ID=R1_2.ruleset_id
left join RulesetType R1_3 on M1.BEST_prob_MATCH_RULESET_ID=R1_3.ruleset_id

left join entitymatched_Bran_block M2
on P.rogers_ecid = M2.rogers_ecid and P.shaw_master_party_id = M2.shaw_master_party_id
left join RulesetType R2_1 on M2.BEST_DET_MATCH_RULESET_ID=R2_1.ruleset_id
left join RulesetType R2_2 on M2.BEST_fuzzy_MATCH_RULESET_ID=R2_2.ruleset_id
left join RulesetType R2_3 on M2.BEST_prob_MATCH_RULESET_ID=R2_3.ruleset_id

group by 
DET, WHOLE_fuzzy, WHOLE_prob, BLOCK_fuzzy, BLOCK_prob
-- Det, Fuzzy, Prob, Det_with_blocking, Fuzzy_with_blocking, Prob_with_blocking

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Sample Ids

-- COMMAND ----------

-- Assess combination of matches on both strategy
with 

allPairs as
(
  select distinct rogers_ecid, shaw_master_party_id
  from accountmatched_Bran_no_block
  where rogers_ecid is not null and shaw_master_party_id is not null
  
  union 
  
  select distinct rogers_ecid, shaw_master_party_id
  from accountmatched_Bran_block
  where rogers_ecid is not null and shaw_master_party_id is not null
)
, RulesetType as
(
  select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Type  
  from rulesetView
)


select distinct  

case when (R1_2.Type is null and R1_3.Type is null) then "Only in Blocking" 
     when (R2_2.Type is null and R2_3.Type is null) then "Only in Whole" 
     else "Both"  
end Source 
, P.rogers_ecid, P.shaw_master_party_id

, R1_1.Type as DET
, R1_2.Type as WHOLE_fuzzy, R1_3.Type as WHOLE_prob
, R2_2.Type as BLOCK_fuzzy, R2_3.Type as BLOCK_prob

, R1_1.ruleset_id as DET
, R1_2.ruleset_id as WHOLE_fuzzy, R1_3.ruleset_id as WHOLE_prob
, R2_2.ruleset_id as BLOCK_fuzzy, R2_3.ruleset_id as BLOCK_prob

from allPairs P
left join entitymatched_Bran_no_block M1 on P.rogers_ecid=M1.rogers_ecid and P.shaw_master_party_id=M1.shaw_master_party_id 
left join RulesetType R1_1 on M1.BEST_DET_MATCH_RULESET_ID=R1_1.ruleset_id
left join RulesetType R1_2 on M1.BEST_fuzzy_MATCH_RULESET_ID=R1_2.ruleset_id
left join RulesetType R1_3 on M1.BEST_prob_MATCH_RULESET_ID=R1_3.ruleset_id

left join entitymatched_Bran_block M2
on P.rogers_ecid = M2.rogers_ecid and P.shaw_master_party_id = M2.shaw_master_party_id
left join RulesetType R2_1 on M2.BEST_DET_MATCH_RULESET_ID=R2_1.ruleset_id
left join RulesetType R2_2 on M2.BEST_fuzzy_MATCH_RULESET_ID=R2_2.ruleset_id
left join RulesetType R2_3 on M2.BEST_prob_MATCH_RULESET_ID=R2_3.ruleset_id

where 
R1_1.Type is null
-- and Source = "Only in Blocking"
and (R1_2.Type is null and R1_3.Type is null)

-- COMMAND ----------

-- get the ids that did not match on deterministic
with 

RulesetType as
(
  select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Type  
  from rulesetView
)

select  M.rogers_ecid, M.shaw_master_party_id, BEST_fuzzy_MATCH_RULESET_ID, BEST_prob_MATCH_RULESET_ID
-- , R2.Type as fuzzy_TYPE, R3.Type as prob_TYPE

from entitymatched_Bran_block M
left join RulesetType R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
left join RulesetType R3 on M.BEST_prob_MATCH_RULESET_ID=R3.ruleset_id

where BEST_DET_MATCH_RULESET_ID is null and rogers_ecid is not null and shaw_master_party_id is not null
-- rogers_ecid in (select rogers_ecid from activeRogers)
-- and shaw_master_party_id in (select * from activeShaw)
-- group by DET_TYPE, fuzzy_TYPE, prob_TYPE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###High match rates per one customer

-- COMMAND ----------

with matchCount as
(
select  
rogers_ecid
, count (distinct shaw_master_party_id) as matchRate
 from entitymatched_Bran_no_block
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

with matchCount as
(
select  
rogers_ecid
,case when R1.IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Fuzzy_RuleType  
,case when R2.IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Prob_RuleType  
, count (distinct shaw_master_party_id) as matchRate
 from entitymatched_Bran_no_block M
 left join rulesetView R1 on M.BEST_DET_MATCH_RULESET_ID=R1.ruleset_id
 left join rulesetView R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
 where (DET_MATCH_IND="N" or DET_MATCH_IND is null) and shaw_master_party_id is not null and rogers_ecid is not null
 group by rogers_ecid, Fuzzy_RuleType, Prob_RuleType
 )
 select Fuzzy_RuleType, Prob_RuleType
  ,count (distinct rogers_ecid) as Number_of_Rogers_Customers_with_High_Matches
 , matchRate as match_rate_to_Shaw_Customers
from matchCount C
group by matchRate, Fuzzy_RuleType, Prob_RuleType
order by matchRate desc

-- COMMAND ----------

with matchCount as
(
select  
shaw_master_party_id
, count (distinct  rogers_ecid) as matchRate
 from entitymatched_Bran_no_block
  where (DET_MATCH_IND="N" or DET_MATCH_IND is null) and shaw_master_party_id is not null and rogers_ecid is not null
 group by shaw_master_party_id
 )
 select 
    count (distinct shaw_master_party_id) as Number_of_Shaw_Customers_with_High_Matches
   , matchRate as match_rate_to_Rogers_Customers
from matchCount C
group by matchRate
order by matchRate desc

-- COMMAND ----------

with matchCount as
(
select  
Shaw_master_party_id
,case when R1.IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Fuzzy_RuleType  
,case when R2.IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Prob_RuleType  
, count (distinct rogers_ecid) as matchRate
 from entitymatched_Bran_no_block M
 left join rulesetView R1 on M.BEST_DET_MATCH_RULESET_ID=R1.ruleset_id
 left join rulesetView R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
 where (DET_MATCH_IND="N" or DET_MATCH_IND is null) and shaw_master_party_id is not null and rogers_ecid is not null
 group by Shaw_master_party_id, Fuzzy_RuleType, Prob_RuleType
 )
 select Fuzzy_RuleType, Prob_RuleType
  ,count (distinct Shaw_master_party_id) as Number_of_Shaw_Customers_with_High_Matches
 , matchRate as match_rate_to_Rogers_Customers
from matchCount C
group by matchRate, Fuzzy_RuleType, Prob_RuleType
order by matchRate desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##matched on deterministic, no match on fuzzy/probabilistic

-- COMMAND ----------

-- New data has inactive accounts excluded
with 
activeRogers as
(
  select distinct rogers_ecid
  from accountmatched_Bran_no_block
  where rogers_ecid is not null
  and rogers_account_status in ('o', 's', 't') or rogers_account_status in (2,3,4,7)
)
,activeShaw as
(
  select distinct shaw_master_party_id
  from accountmatched_Bran_no_block
  where rogers_ecid is not null
  and shaw_account_status in ('active', 'soft suspended')
),
RulesetType as
(
  select RULESET_ID
  ,case when IS_CUSTOMER_MATCH = "TRUE" then "CUSTOMER LEVEL" else "ADDRESS LEVEL"  end Type  
  from rulesetView
)

select M.rogers_ecid, M.shaw_master_party_id, best_det_match_ruleset_id, fuzzy_match_ind, prob_match_ind
from entitymatched_Bran_no_block M
inner join accountmatched_Bran_no_block A
on M.rogers_ecid=A.rogers_ecid and M.shaw_master_party_id=A.shaw_master_party_id

where 
det_match_ind="Y" and (prob_match_ind is null or fuzzy_match_ind is null)
and (A.rogers_account_status in ('o', 's', 't') or A.rogers_account_status in (2,3,4,7))
and (A.shaw_account_status in ('active', 'soft suspended'))

-- COMMAND ----------


