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
-- MAGIC MatchedEntity = "/mnt/development/Processed/QA/Iteration2/MatchedEntity"
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
-- MAGIC ##TC-A-17 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Validate matched Address Level output 

-- COMMAND ----------

with distinctMatch as
(
select distinct 
s_rcis_id_cl
,s_fa_id_cl
,r_rcis_id_cl
,r_fa_id_cl
, concat_ws ("-",s_rcis_id_cl, s_fa_id_cl, r_rcis_id_cl, r_fa_id_cl) as uniqueID
-- ,s_dq_full_name_cl
-- ,r_dq_full_name_cl
-- ,s_mailing_address_no_zipcode_dq_cl
-- ,s_service_address_no_zipcode_dq_cl
-- ,r_mailing_address_no_zipcode_dq_cl
-- ,r_service_address_no_zipcode_dq_cl
 from matchedView where Ruleset_id=17
 )
 
 select M.s_rcis_id_cl, M.s_fa_id_cl, count(M.uniqueID) as countID
from distinctMatch M
group by s_rcis_id_cl, s_fa_id_cl
order by countID desc

-- COMMAND ----------

with matchCount as
(
select  
s_rcis_id_cl
,s_fa_id_cl
, count (concat_ws ("-",r_rcis_id_cl, r_fa_id_cl)) as counts
-- ,s_dq_full_name_cl
-- ,r_dq_full_name_cl
-- ,s_mailing_address_no_zipcode_dq_cl
-- ,s_service_address_no_zipcode_dq_cl
-- ,r_mailing_address_no_zipcode_dq_cl
-- ,r_service_address_no_zipcode_dq_cl
 from matchedView 
 where Ruleset_id=17
 group by s_rcis_id_cl, s_fa_id_cl
 )
 
select counts, S.address, R.address, S.mailing_address_full_cl, R.mailing_address_full_cl
, M.s_rcis_id_cl, M.s_fa_id_cl, M.r_rcis_id_cl, M.r_fa_id_cl
from matchCount C
inner join matchedView M on C.s_rcis_id_cl=M.s_rcis_id_cl and C.s_fa_id_cl = M.s_fa_id_cl
inner join cl_shaw_contact S on M.s_rcis_id_cl=S.rcis_id_cl and M.s_fa_id_cl = S.fa_id_cl
inner join cl_rogers_contact R on M.r_rcis_id_cl=R.rcis_id_cl and M.r_fa_id_cl = R.fa_id_cl
--where counts <3

-- COMMAND ----------

select s_contact_id
, s_rcis_id_cl
, s_fa_id_cl
, s_dq_full_name_cl
-- , s_address_cl
-- , s_mailing_address_no_zipcode_dq_cl

, s_service_address_cl
, s_service_address_no_zipcode_dq_cl

, r_rcis_id_cl
, r_fa_id_cl
, r_dq_full_name_cl
-- , r_address_cl
-- , r_mailing_address_no_zipcode_dq_cl
, r_service_address_cl
, r_service_address_no_zipcode_dq_cl
, Ruleset_ID 
from matchedView 
where 
-- s_rcis_id_cl =447900 and s_fa_id_cl= 05563748586 and ruleset_id=16
s_rcis_id_cl =7299589 and s_fa_id_cl= 20029168833 and ruleset_id=16

-- COMMAND ----------

select * from cl_shaw_concat where 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-A-18 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Validating cleansed street directions

-- COMMAND ----------

select 
  
    x_direction , streetdirection, StreetDirection_Cleansed
    , address, mailing_address_no_zipcode_dq
    , "Rogers" as source
    , rcis_id_cl, fa_id_cl
  from cl_rogers_contact
  where 
  lower(x_direction) != StreetDirection_Cleansed
  
  union
  
  select 
  
    x_direction , streetdirection, StreetDirection_Cleansed
    , address, mailing_address_no_zipcode_dq
    , "Shaw" as source
    , rcis_id_cl, fa_id_cl
  
  from cl_shaw_contact
  where 
  lower(x_direction) != StreetDirection_Cleansed

-- COMMAND ----------


select address, mailing_address_no_zipcode_cl, R.x_direction, R.streetdirection, R.streettype, rcis_id, fa_id
  from cl_rogers_contact R
--   where R.StreetDirection_Cleansed not in ('e', 'w', 'n', 's', 'ne', 'nw', 'se', 'sw')
--  where lower(R.streettype) like  '%boul%' 
--  where lower(R.address) like '%parkway%'
  where lower(R.address) like '%martha\'s%'
--  or lower(R.address) like '%179 east\'%'
--  where lower(R.address) like '%park%'
--  or lower(R.address) like '%parkway%'
--  or lower(R.address) like '%boul%'
--  or lower(R.address) like '%bl%'
--  or lower(R.address) like '%boulevard%'




-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-A-19

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Addresses with GD, PO Box, RR, STN

-- COMMAND ----------

-- Shaw -- Find example records in the raw inout files with addresses that include GD, PO Box, RR, STN

  select 
  
   concat_ws(" ", S.address, S.city, S.state), S.mailing_address_no_zipcode_cl
  ,concat_ws(" ", SW.service_address, SW.service_city, SW.service_province), SW.service_address , SW.service_address_no_zipcode_dq_cl
   , S.contact_id, S.rcis_id_cl, S.fa_id_cl

  from cl_shaw_contact S 
  left join cl_shaw_wireline SW on S.fa_id_cl = SW.fa_id and S.rcis_id_cl = SW.rcis_id

  where 
--    lower(concat(address,service_address)) like '% GD%' or lower(address) like '% general delivery %'
--    lower(concat(address,service_address)) like '% rr %' or lower(concat(address,service_address)) like '% rural route%'
--  or lower(concat(address,service_address)) like '% po box %'
    lower(concat(address,service_address)) like '% stn %' or lower(address) like '% station%'
--  or lower(concat(address,service_address)) like '% GD%' or lower(address) like '% general delivery %'


-- COMMAND ----------

-- Rogers -- Find example records in the raw inout files with addresses that include GD, PO Box, RR, STN

  select 
  
   R.address, R.mailing_address_no_zipcode_cl
   , RW.service_address , RW.service_address_no_zipcode_dq_cl
   , R.contact_id, R.rcis_id_cl, R.fa_id_cl

  from cl_rogers_contact R 
  left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.rcis_id

  where 
  --lower(concat(R.address,RW.service_address)) like '% GD%' or lower(R.address) like '% general delivery %'
  --lower(concat(R.address,RW.service_address)) like '% rr %' or lower(concat(R.address,RW.service_address)) like '% rural route%'
--  or lower(concat(R.address,RW.service_address)) like '% po box %'
  lower(concat(R.address,RW.service_address)) like '% stn %' or lower(R.address) like '% station%'
--  or lower(concat(R.address,RW.service_address)) like '% GD%' or lower(R.address) like '% general delivery %'


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Cleaned Addresses that are NULL

-- COMMAND ----------

-- Shaw -- Find example records in the raw inout files with addresses that include GD, PO Box, RR, STN

  select 
  "shaw" as source
   ,concat_ws(" ", S.address, S.city, S.state) as raw_mailing_address , S.mailing_address_full_cl
  ,concat_ws(" ", SW.service_address, SW.service_city, SW.service_province) as raw_service_address , SW.service_address_full_cl
   , S.contact_id, S.rcis_id_cl, S.fa_id_cl

  from cl_shaw_contact S 
  left join cl_shaw_wireline SW on S.fa_id_cl = SW.fa_id and S.rcis_id_cl = SW.rcis_id
  where 
length(S.mailing_address_full_cl)*2.5<length(trim(concat_ws(" ", S.address, S.city, S.state)))
or 
length(SW.service_address_full_cl)*2.5<length(trim(concat_ws(" ", SW.service_address, SW.service_city, SW.service_province)))

union 

  select 
  "rogers" as source
   , R.address as raw_mailing_address , R.mailing_address_full_cl
  , RW.service_address as raw_service_address , RW.service_address_full_cl
   , R.contact_id, R.rcis_id_cl, R.fa_id_cl

  from cl_shaw_contact R 
  left join cl_shaw_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.rcis_id
  where 
length(R.mailing_address_full_cl)*2.5<length(trim(R.address))
or 
length(RW.service_address_full_cl)*2.5<length(trim(RW.service_address))

-- COMMAND ----------

-- Shaw -- Find example records in the raw inout files with addresses that include GD, PO Box, RR, STN

  select 
  "shaw" as source
   ,concat_ws(" ", S.address, S.city, S.state) as raw_mailing_address , S.mailing_address_full_cl
  ,concat_ws(" ", SW.service_address, SW.service_city, SW.service_province) as raw_service_address , SW.service_address_full_cl
   , S.contact_id, S.rcis_id_cl, S.fa_id_cl

  from cl_shaw_contact S 
  left join cl_shaw_wireline SW on S.fa_id_cl = SW.fa_id and S.rcis_id_cl = SW.rcis_id
  where 
S.mailing_address_full_cl is NULL


union 

  select 
  "rogers" as source
   , R.address as raw_mailing_address , R.mailing_address_full_cl
  , RW.service_address as raw_service_address , RW.service_address_full_cl
   , R.contact_id, R.rcis_id_cl, R.fa_id_cl

  from cl_shaw_contact R 
  left join cl_shaw_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.rcis_id
  where 
R.mailing_address_full_cl is NULL





-- COMMAND ----------

-- Shaw -- Find example records in the raw inout files with addresses that include GD, PO Box, RR, STN

  select 
  
   concat_ws(" ", S.address, S.city, S.state), S.mailing_address_no_zipcode_cl
  ,concat_ws(" ", SW.service_address, SW.service_city, SW.service_province), SW.service_address , SW.service_address_no_zipcode_dq_cl
   , S.contact_id, S.rcis_id_cl, S.fa_id_cl

  from cl_shaw_contact S 
  left join cl_shaw_wireline SW on S.fa_id_cl = SW.fa_id and S.rcis_id_cl = SW.rcis_id

  where 
--    lower(concat(address,service_address)) like '% GD%' or lower(address) like '% general delivery %'
  lower(concat(address,service_address)) like '% rr %' or lower(concat(address,service_address)) like '% rural route%'
--  or lower(concat(address,service_address)) like '% po box %'
--  or lower(concat(address,service_address)) like '% stn %' or lower(address) like '% station%'
--  or lower(concat(address,service_address)) like '% GD%' or lower(address) like '% general delivery %'



-- COMMAND ----------

-- Rogers -- Find example records in the raw inout files with addresses that include GD, PO Box, RR, STN

  select 
  
   R.address, R.mailing_address_no_zipcode_cl
   , RW.service_address , RW.service_address_no_zipcode_dq_cl
   , R.contact_id, R.rcis_id_cl, R.fa_id_cl

  from cl_rogers_contact R 
  left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.rcis_id

  where 
--  lower(concat(R.address,RW.service_address)) like '% GD%' or lower(R.address) like '% general delivery %'
  lower(concat(R.address,RW.service_address)) like '% rr %' or lower(concat(R.address,RW.service_address)) like '% rural route%'
--  or lower(concat(R.address,RW.service_address)) like '% po box %'
--  or lower(concat(R.address,RW.service_address)) like '% stn %' or lower(R.address) like '% station%'
--  or lower(concat(R.address,RW.service_address)) like '% GD%' or lower(R.address) like '% general delivery %'


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-A-17 re-test APRIL 27th

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Validate matched Address Level output - distinctMatch

-- COMMAND ----------

/*

with distinctMatch as
(
select distinct 
s_rcis_id_cl
,s_fa_id_cl
,r_rcis_id_cl
,r_fa_id_cl
, concat_ws ("-",s_rcis_id_cl, s_fa_id_cl, r_rcis_id_cl, r_fa_id_cl) as uniqueID
-- ,s_dq_full_name_cl
-- ,r_dq_full_name_cl
-- ,s_mailing_address_no_zipcode_dq_cl
-- ,s_service_address_no_zipcode_dq_cl
-- ,r_mailing_address_no_zipcode_dq_cl
-- ,r_service_address_no_zipcode_dq_cl
 from matchedView where Ruleset_id=17
 )
 
 select M.s_rcis_id_cl, M.s_fa_id_cl, count(M.uniqueID) as countID
from distinctMatch M
group by s_rcis_id_cl, s_fa_id_cl
order by countID desc




select distinct 
s_rcis_id_cl
,s_fa_id_cl
,r_rcis_id_cl
,r_fa_id_cl
,s_mailing_address_no_zipcode_dq_cl
,s_service_address_no_zipcode_dq_cl
,r_mailing_address_no_zipcode_dq_cl
,r_service_address_no_zipcode_dq_cl
 from matchedView where Ruleset_id=17



SELECT * FROM matchedView
  WHERE s_service_address_no_zipcode_dq_cl = r_service_address_no_zipcode_dq_cl
    and s_service_address_cl != r_service_address_cl


    
SELECT s_contact_id, s_rcis_id_cl, s_fa_id_cl, s_dq_full_name_cl, s_address_cl, s_city_cl, s_state_cl, s_zipcode_cl, s_mailing_address_no_zipcode_dq_cl, s_source_system, s_service_address_cl, s_service_address_no_zipcode_dq_cl, r_contact_id, r_rcis_id_cl, r_fa_id_cl, r_dq_full_name_cl, r_address_cl, r_city_cl, r_state_cl, r_zipcode_cl, r_mailing_address_no_zipcode_dq_cl, r_source_system, r_service_address_cl, r_service_address_no_zipcode_dq_cl, Ruleset_ID
FROM matchedView
  WHERE s_service_address_no_zipcode_dq_cl = r_service_address_no_zipcode_dq_cl
    and concat(concat(concat(concat(concat(concat(s_service_address_cl,' '),s_city_cl),' '),s_state_cl),' '),s_zipcode_cl) != replace(r_service_address_cl,'  ',' ')


--SELECT * from cl_rogers_wireline where service_address_no_zipcode_dq_cl = '1 aberfoyle cres etobicoke on'


desc matchedView

SELECT s_contact_id, s_rcis_id_cl, s_fa_id_cl, s_dq_full_name_cl, s_address_cl, s_city_cl, s_state_cl, s_zipcode_cl, s_mailing_address_no_zipcode_dq_cl, s_source_system, s_service_address_cl, s_service_address_no_zipcode_dq_cl, r_contact_id, r_rcis_id_cl, r_fa_id_cl, r_dq_full_name_cl, r_address_cl, r_city_cl, r_state_cl, r_zipcode_cl, r_mailing_address_no_zipcode_dq_cl, r_source_system, r_service_address_cl, r_service_address_no_zipcode_dq_cl, Ruleset_ID
FROM matchedView
  WHERE s_service_address_no_zipcode_dq_cl = r_service_address_no_zipcode_dq_cl
    --and concat(concat(concat(concat(concat(concat(s_service_address_cl,' '),s_city_cl),' '),s_state_cl),' '),s_zipcode_cl) != replace(r_service_address_cl,'  ',' ')
      and length(r_zipcode_cl)>6

select rcis_id, fa_id, address, city, state, zipcode, city_cleansed, address_cleansed, postalcode, province, province_cleansed, postalcode_cleansed, mailing_address_no_zipcode_dq, mailing_address_no_zipcode, mailing_address_full, rcis_id_cl, address_cl, city_cl, state_cl, zipcode_cl, mailing_address_no_zipcode_dq_cl, mailing_address_no_zipcode_cl, mailing_address_full_cl  from cl_rogers_contact where rcis_id = '451697189' or rcis_id = '452317610'

*/

/*
select 
"Shaw Contact Address" as source
, concat_ws(" ", address, city, province) as address
, mailing_address_no_zipcode_dq_cl as normalized_address
, PostalCode, rcis_id, fa_id, contact_id
from cl_shaw_contact  where 
length(postalcode)>15


union 

select 
"Shaw Service Address" as source
, concat_ws(" ", service_address, service_city, service_province) as address
, service_address_no_zipcode_dq_cl as normalized_address
, PostalCode, rcis_id, fa_id, contact_id
from cl_shaw_wireline  where 
length(postalcode)>15

union


select 
"Rogers Contact Address" as source
, address
, mailing_address_no_zipcode_dq_cl as normalized_address
, PostalCode, rcis_id, fa_id, contact_id
from cl_rogers_contact  where 
length(postalcode)>15


union 

select
"Rogers Service Address" as source
, service_address as address
, service_address_no_zipcode_dq_cl as normalized_address
, PostalCode, rcis_id, fa_id, contact_id
from cl_rogers_wireline where 
length(postalcode)>15


*/


--select * from cl_rogers_contact where rcis_id = 31624671
select * from cl_rogers_wireline where rcis_id = '453320303'


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Validate matched Address Level output - matchCount

-- COMMAND ----------

with matchCount as
(
select  
s_rcis_id_cl
,s_fa_id_cl
, count (concat_ws ("-",r_rcis_id_cl, r_fa_id_cl)) as counts
-- ,s_dq_full_name_cl
-- ,r_dq_full_name_cl
-- ,s_mailing_address_no_zipcode_dq_cl
-- ,s_service_address_no_zipcode_dq_cl
-- ,r_mailing_address_no_zipcode_dq_cl
-- ,r_service_address_no_zipcode_dq_cl
 from matchedView 
 where Ruleset_id=17
 group by s_rcis_id_cl, s_fa_id_cl
 )
 
select counts, S.address, R.address, S.mailing_address_full_cl, R.mailing_address_full_cl
, M.s_rcis_id_cl, M.s_fa_id_cl, M.r_rcis_id_cl, M.r_fa_id_cl
from matchCount C
inner join matchedView M on C.s_rcis_id_cl=M.s_rcis_id_cl and C.s_fa_id_cl = M.s_fa_id_cl
inner join cl_shaw_contact S on M.s_rcis_id_cl=S.rcis_id_cl and M.s_fa_id_cl = S.fa_id_cl
inner join cl_rogers_contact R on M.r_rcis_id_cl=R.rcis_id_cl and M.r_fa_id_cl = R.fa_id_cl
--where counts <3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###matchedView

-- COMMAND ----------

select s_contact_id
, s_rcis_id_cl
, s_fa_id_cl
, s_dq_full_name_cl
-- , s_address_cl
-- , s_mailing_address_no_zipcode_dq_cl

, s_service_address_cl
, s_service_address_no_zipcode_dq_cl

, r_rcis_id_cl
, r_fa_id_cl
, r_dq_full_name_cl
-- , r_address_cl
-- , r_mailing_address_no_zipcode_dq_cl
, r_service_address_cl
, r_service_address_no_zipcode_dq_cl
, Ruleset_ID 
from matchedView 
--where 
-- s_rcis_id_cl =447900 and s_fa_id_cl= 05563748586 and ruleset_id=16
-- s_rcis_id_cl =7299589 and s_fa_id_cl= 20029168833 and ruleset_id=16
