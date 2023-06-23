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
-- MAGIC ## Sample Matches

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Sample Accounts from Clean files

-- COMMAND ----------

with help as
( 
  select s_account_type
  , s_rcis_id_cl, s_fa_id_cl, s_dq_full_name_cl
  , s_dq_primary_phone_cl, s_dq_alternate_phone_1_cl, s_dq_alternate_phone_2_cl
  , s_mailing_address_full_cl, s_service_address_full_cl
  from r_wireless
  
  union 
  
  select s_account_type
  , s_rcis_id_cl, s_fa_id_cl, s_dq_full_name_cl
  , s_dq_primary_phone_cl, s_dq_alternate_phone_1_cl, s_dq_alternate_phone_2_cl
  , s_mailing_address_full_cl, s_service_address_full_cl
  from r_wireline
  
  )
  select distinct r_account_type from help
  

-- COMMAND ----------

  select *
  from r_wireless

-- COMMAND ----------

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
, RogersAccounts as
(
  select r_account_type
  , r_rcis_id_cl, r_fa_id_cl, r_dq_full_name_cl, r_dq_e_mail_cl
  , r_dq_primary_phone_cl, r_dq_alternate_phone_1_cl, r_dq_alternate_phone_2_cl
  , r_mailing_address_full_cl, r_service_address_full_cl
  from r_wireless
  
  union 
  
  select r_account_type
  , r_rcis_id_cl, r_fa_id_cl, r_dq_full_name_cl, r_dq_e_mail_cl
  , r_dq_primary_phone_cl, r_dq_alternate_phone_1_cl, r_dq_alternate_phone_2_cl
  , r_mailing_address_full_cl, r_service_address_full_cl
  from r_wireline
)
, ShawAccounts as
(
  select s_account_type
  , s_rcis_id_cl, s_fa_id_cl, s_dq_full_name_cl, s_dq_e_mail_cl
  , s_dq_primary_phone_cl, s_dq_alternate_phone_1_cl, s_dq_alternate_phone_2_cl
  , s_mailing_address_full_cl, s_service_address_full_cl
  from s_wireless
  
  union 
  
  select s_account_type
  , s_rcis_id_cl, s_fa_id_cl, s_dq_full_name_cl, s_dq_e_mail_cl
  , s_dq_primary_phone_cl, s_dq_alternate_phone_1_cl, s_dq_alternate_phone_2_cl
  , s_mailing_address_full_cl, s_service_address_full_cl
  from s_wireline
)

select distinct R1.Type as Det_Type, R2.type as Fuzzy_Type, R3.Type as Prob_Type
, R1.ruleset_id as Det_rule, R2.ruleset_id as Fuzzy_rule, R3.ruleset_id as Prob_rule
, M.rogers_ecid, M.shaw_master_party_id, A.Rogers_account_id, A.Shaw_account_id, A.rogers_account_type, A.shaw_account_type
, A.rogers_account_type, A.shaw_account_type
, r_dq_full_name_cl, r_dq_primary_phone_cl, r_dq_e_mail_cl, r_dq_alternate_phone_1_cl, r_dq_alternate_phone_2_cl, r_mailing_address_full_cl, r_service_address_full_cl
, s_dq_full_name_cl, s_dq_primary_phone_cl, s_dq_e_mail_cl, s_dq_alternate_phone_1_cl, s_dq_alternate_phone_2_cl, s_mailing_address_full_cl, s_service_address_full_cl
from matchedEntityView M


INNER JOIN matchedAccountView A ON M.ROGERS_ECID = A.ROGERS_ECID AND M.SHAW_MASTER_PARTY_ID = A.SHAW_MASTER_PARTY_ID
INNER JOIN RogersAccounts R ON A.ROGERS_ECID=R.r_rcis_id_cl AND A.Rogers_account_id = R.r_fa_id_cl
INNER JOIN ShawAccounts S ON A.Shaw_master_party_id=S.s_rcis_id_cl AND A.Shaw_account_id = S.s_fa_id_cl

left join RulesetType R1 on M.BEST_DET_MATCH_RULESET_ID=R1.ruleset_id
left join RulesetType R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
left join RulesetType R3 on M.BEST_prob_MATCH_RULESET_ID=R3.ruleset_id
 
where R1.Type is null and (R2.Type ="1-Customer lvl"  or R3.Type="1-Customer lvl" )
limit 500

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Sample Accounts from raw files

-- COMMAND ----------

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

select distinct 
R1.Type as Det_Type, R2.type as Fuzzy_Type, R3.Type as Prob_Type
-- , M.BEST_DET_MATCH_RULESET_ID
, M.BEST_fuzzy_MATCH_RULESET_ID as Fuzzy_rule, M.CUSTOMER_FUZZY_SCORE as Fuzzy_Score
, M.BEST_prob_MATCH_RULESET_ID as Prob_rule, M.CUSTOMER_PROB_CONFIDENCE_LVL as Prob_Score
, M.rogers_ecid, M.shaw_master_party_id, A.Rogers_account_id, A.Shaw_account_id, A.rogers_account_type, A.shaw_account_type
, A.rogers_account_type, A.shaw_account_type

, concat_ws(" ", R.first_name, R.last_name) as r_full_name, R.e_mail as r_e_mail
, R.primary_phone as r_primary_phone, R.alternate_phone_1 as r_alternate_phone_1, R.alternate_phone_2 as r_alternate_phone_2 
, R.address as r_mailing_address, RW.service_address as r_service_address

, concat_ws(" ", S.first_name, S.last_name) as s_full_name, S.e_mail as s_e_mail
, S.primary_phone as s_primary_phone, S.alternate_phone_1 as s_alternate_phone_1, S.alternate_phone_2 as s_alternate_phone_2 
, concat_ws(" ", S.address, S.city, S.state, S.zipcode) as s_mailing_address
, concat_ws(" ", SW.service_address, SW.service_city, SW.service_province, SW.service_postal_code) as s_service_address

from matchedEntityView M


INNER JOIN matchedAccountView A ON M.ROGERS_ECID = A.ROGERS_ECID AND M.SHAW_MASTER_PARTY_ID = A.SHAW_MASTER_PARTY_ID
INNER JOIN raw_rogers_contact R ON A.ROGERS_ECID=R.x_rcis_id AND A.Rogers_account_id = R.fa_id
INNER JOIN raw_shaw_contact S ON A.Shaw_master_party_id=S.rcis_id AND A.Shaw_account_id = S.fa_id

LEFT JOIN raw_rogers_wireline RW ON A.ROGERS_ECID=RW.x_rcis_id AND A.Rogers_account_id = RW.fa_id AND A.ROGERS_SAM_KEY=RW.SAM_KEY
LEFT JOIN raw_shaw_wireline SW ON A.Shaw_master_party_id=SW.rcis_id AND A.Shaw_account_id = SW.fa_id

left join RulesetType R1 on M.BEST_DET_MATCH_RULESET_ID=R1.ruleset_id
left join RulesetType R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
left join RulesetType R3 on M.BEST_prob_MATCH_RULESET_ID=R3.ruleset_id
 
where R1.Type is null and (R2.Type ="1-Customer lvl"  or R3.Type="1-Customer lvl" )
and (M.BEST_fuzzy_MATCH_RULESET_ID = A.ruleset_id or M.BEST_prob_MATCH_RULESET_ID = A.ruleset_id)
 and M.rogers_ecid=30449157

limit 500

-- COMMAND ----------

select  * from matchedAccountView A
where 
rogers_ecid=30449157 and shaw_master_party_id=583925
-- rogers_ecid=453293405 and shaw_master_party_id=13221138 and rogers_account_id=914785506 and shaw_account_id=9901098046
-- rogers_ecid=423594879 and shaw_master_party_id=1724749 and rogers_account_id=746926815 and shaw_account_id=3805253828

			


-- COMMAND ----------

select * from raw_rogers_contact
where x_rcis_id = 423594879


-- COMMAND ----------

select * from raw_shaw_contact
where rcis_id = 1724749


-- COMMAND ----------



-- COMMAND ----------

select * from rulesetView

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Sample Accounts, raw files, accounts of prob,fuzzy seperately

-- COMMAND ----------

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

select distinct 
R1.Type as Det_Type, R2.type as Fuzzy_Type, R3.Type as Prob_Type
-- , M.BEST_DET_MATCH_RULESET_ID
, M.BEST_fuzzy_MATCH_RULESET_ID as Fuzzy_rule, M.CUSTOMER_FUZZY_SCORE as Fuzzy_Score
, M.BEST_prob_MATCH_RULESET_ID as Prob_rule, M.CUSTOMER_PROB_CONFIDENCE_LVL as Prob_Score

, M.rogers_ecid, M.shaw_master_party_id

, A.Rogers_account_id, A.Shaw_account_id
, A.ruleset_type, A.ruleset_id
-- , A.rogers_account_type, A.shaw_account_type
-- , A.rogers_account_type, A.shaw_account_type

, concat_ws(" ", R.first_name, R.last_name) as r_full_name, R.e_mail as r_e_mail
, R.primary_phone as r_primary_phone, R.alternate_phone_1 as r_alternate_phone_1, R.alternate_phone_2 as r_alternate_phone_2 
, R.address as r_mailing_address, RW.service_address as r_service_address

, concat_ws(" ", S.first_name, S.last_name) as s_full_name, S.e_mail as s_e_mail
, S.primary_phone as s_primary_phone, S.alternate_phone_1 as s_alternate_phone_1, S.alternate_phone_2 as s_alternate_phone_2 
, concat_ws(" ", S.address, S.city, S.state, S.zipcode) as s_mailing_address
, concat_ws(" ", SW.service_address, SW.service_city, SW.service_province, SW.service_postal_code) as s_service_address

from matchedEntityView M


INNER JOIN matchedAccountView A ON M.ROGERS_ECID = A.ROGERS_ECID AND M.SHAW_MASTER_PARTY_ID = A.SHAW_MASTER_PARTY_ID
INNER JOIN raw_rogers_contact R ON A.ROGERS_ECID=R.x_rcis_id AND A.Rogers_account_id = R.fa_id
INNER JOIN raw_shaw_contact S ON A.Shaw_master_party_id=S.rcis_id AND A.Shaw_account_id = S.fa_id

LEFT JOIN raw_rogers_wireline RW ON A.ROGERS_ECID=RW.x_rcis_id AND A.Rogers_account_id = RW.fa_id AND A.ROGERS_SAM_KEY=RW.SAM_KEY
LEFT JOIN raw_shaw_wireline SW ON A.Shaw_master_party_id=SW.rcis_id AND A.Shaw_account_id = SW.fa_id

left join RulesetType R1 on M.BEST_DET_MATCH_RULESET_ID=R1.ruleset_id
left join RulesetType R2 on M.BEST_fuzzy_MATCH_RULESET_ID=R2.ruleset_id
left join RulesetType R3 on M.BEST_prob_MATCH_RULESET_ID=R3.ruleset_id
 
where R1.Type is null and (R2.Type ="1-Customer lvl"  or R3.Type="1-Customer lvl" )
and ((M.BEST_fuzzy_MATCH_RULESET_ID = A.ruleset_id and M.CUSTOMER_FUZZY_SCORE=A.ruleset_attr) or (M.BEST_prob_MATCH_RULESET_ID = A.ruleset_id and M.CUSTOMER_PROB_CONFIDENCE_LVL=A.ruleset_attr))

order by M.rogers_ecid, M.shaw_master_party_id, A.Rogers_account_id, A.Shaw_account_id

limit 300

-- COMMAND ----------

select * from matchedEntityView
where rogers_ecid=230099337 and shaw_master_party_id=2103199

-- COMMAND ----------


