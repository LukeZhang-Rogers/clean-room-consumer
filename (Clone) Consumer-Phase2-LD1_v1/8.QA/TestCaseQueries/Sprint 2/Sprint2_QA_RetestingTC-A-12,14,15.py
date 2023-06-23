# Databricks notebook source
# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC #Raw Data
# MAGIC
# MAGIC #paths
# MAGIC shaw_consumer_wireless_account = "/mnt/development/processed/interim/shaw_wireless_accounts"
# MAGIC shaw_consumer_wireless_service = "/mnt/development/processed/interim/shaw_wireless_services"
# MAGIC shaw_consumer_wireline_account = "/mnt/development/processed/interim/shaw_wireline_accounts"
# MAGIC shaw_consumer_contact          = "/mnt/development/processed/interim/shaw_contacts"
# MAGIC rogers_contact                 = '/mnt/rogerssaprod/deloitte-cr/rogers_0218/rogers_contact'
# MAGIC rogers_wireless_account        = '/mnt/rogerssaprod/deloitte-cr/rogers_0218/rogers_wireless_account'
# MAGIC rogers_wireline_account        = '/mnt/rogerssaprod/deloitte-cr/rogers_0218/rogers_wireline_account'
# MAGIC
# MAGIC #Load Dataframes
# MAGIC
# MAGIC shaw_consumer_wireless_account_df       = spark.read.format("parquet").load(shaw_consumer_wireless_account)
# MAGIC shaw_consumer_wireline_account_df       = spark.read.format("parquet").load(shaw_consumer_wireline_account)
# MAGIC shaw_consumer_contact_df                = spark.read.format("parquet").load(shaw_consumer_contact)
# MAGIC shaw_consumer_wireless_service_df       = spark.read.format("parquet").load(shaw_consumer_wireless_service)
# MAGIC
# MAGIC rogers_contact_df                 = spark.read.format("parquet").load(rogers_contact)
# MAGIC rogers_wireless_account_df        = spark.read.format("parquet").load(rogers_wireless_account)
# MAGIC rogers_wireline_account_df        = spark.read.format("parquet").load(rogers_wireline_account)

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC #Cleansed Data
# MAGIC
# MAGIC shaw_path = "/mnt/development/Processed/Iteration8/Shaw" 
# MAGIC rogers_path = "/mnt/development/Processed/Iteration8/Rogers" 
# MAGIC
# MAGIC
# MAGIC # shaw_consumer_wireless_account = shaw_path + '/20220216_CONSUMER_WIRELESS_ACCOUNT'
# MAGIC # shaw_consumer_wireless_service = shaw_path + '/20220216_CONSUMER_WIRELESS_SERVICE'
# MAGIC # shaw_consumer_wireline_account = shaw_path + '/20220216_CONSUMER_WIRELINE_ACCOUNT'
# MAGIC # shaw_consumer_contact          = shaw_path + '/20220216_CONSUMER_CONTACT'
# MAGIC
# MAGIC # rogers_contact                 = rogers_path + '/rogers_contact'
# MAGIC # rogers_wireless_account        = rogers_path + '/rogers_wireless_account'
# MAGIC # rogers_wireline_account        = rogers_path + '/rogers_wireline_account'
# MAGIC
# MAGIC shaw_consumer_wireless_account = shaw_path + '/WirelessAccount' 
# MAGIC shaw_consumer_wireline_account = shaw_path + '/WirelineAccount' 
# MAGIC shaw_consumer_contact = shaw_path + '/Contact' 
# MAGIC
# MAGIC rogers_contact = rogers_path + '/Contact' 
# MAGIC rogers_wireless_account = rogers_path + '/WirelessAccount' 
# MAGIC rogers_wireline_account = rogers_path + '/WirelineAccount'
# MAGIC
# MAGIC #Load dataframes
# MAGIC cl_shaw_consumer_wireless_account_df       = spark.read.format("parquet").load(shaw_consumer_wireless_account)
# MAGIC cl_shaw_consumer_wireline_account_df       = spark.read.format("parquet").load(shaw_consumer_wireline_account)
# MAGIC cl_shaw_consumer_contact_df                = spark.read.format("parquet").load(shaw_consumer_contact)
# MAGIC #cl_shaw_consumer_wireless_service_df       = spark.read.format("parquet").load(shaw_consumer_wireless_service)
# MAGIC
# MAGIC cl_rogers_contact_df                 = spark.read.format("parquet").load(rogers_contact)
# MAGIC cl_rogers_wireless_account_df        = spark.read.format("parquet").load(rogers_wireless_account)
# MAGIC cl_rogers_wireline_account_df        = spark.read.format("parquet").load(rogers_wireline_account)
# MAGIC
# MAGIC #Read output parquet files
# MAGIC matched_df = spark.read.format("parquet").load("/mnt/development/silver/iteration8/matched")
# MAGIC unmatched_df = spark.read.format("parquet").load("/mnt/development/silver/iteration8/unmatched")
# MAGIC
# MAGIC
# MAGIC #Shaw Employees 8 Records
# MAGIC shaw_matched_accounts = "/mnt/shawsaprod/shaw-snflk-dir-01/SHAW/20220330_MATCHED_ADDRESSES"
# MAGIC shaw_matched_accounts_df = spark.read.format("parquet").load(shaw_matched_accounts)
# MAGIC
# MAGIC #Output File from DEV team
# MAGIC account_matched_ruleset = "/mnt/development/Processed/QA/Iteration1/AccountMatchedRuleset"
# MAGIC account_matched_ruleset_df= spark.read.format("parquet").load(account_matched_ruleset)
# MAGIC
# MAGIC #Full Rogers table separate from Full Shaw table
# MAGIC MatchedEntity = "/mnt/development/Processed/QA/Iteration2/MatchedEntity"
# MAGIC MatchedEntity_df= spark.read.format("parquet").load(MatchedEntity)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build Views

# COMMAND ----------

# MAGIC %python
# MAGIC #convert to views
# MAGIC
# MAGIC #raw data
# MAGIC shaw_consumer_wireless_account_df.createOrReplaceTempView("r_shaw_wireless")
# MAGIC shaw_consumer_wireline_account_df.createOrReplaceTempView("r_shaw_wireline")
# MAGIC shaw_consumer_contact_df.createOrReplaceTempView("r_shaw_contact")
# MAGIC               
# MAGIC rogers_wireless_account_df.createOrReplaceTempView("r_rogers_wireless")       
# MAGIC rogers_wireline_account_df.createOrReplaceTempView("r_rogers_wireline")
# MAGIC rogers_contact_df.createOrReplaceTempView("r_rogers_contact")  
# MAGIC
# MAGIC #cleansed data
# MAGIC cl_shaw_consumer_wireless_account_df.createOrReplaceTempView("cl_shaw_wireless")
# MAGIC cl_shaw_consumer_wireline_account_df.createOrReplaceTempView("cl_shaw_wireline")
# MAGIC cl_shaw_consumer_contact_df.createOrReplaceTempView("cl_shaw_contact")
# MAGIC               
# MAGIC cl_rogers_wireless_account_df.createOrReplaceTempView("cl_rogers_wireless")        
# MAGIC cl_rogers_wireline_account_df.createOrReplaceTempView("cl_rogers_wireline")
# MAGIC cl_rogers_contact_df.createOrReplaceTempView("cl_rogers_contact")  
# MAGIC
# MAGIC #match output
# MAGIC matched_df.createOrReplaceTempView("matchedView")
# MAGIC unmatched_df.createOrReplaceTempView("unmatchedView")
# MAGIC
# MAGIC #Shaw Employees 8 Records
# MAGIC shaw_matched_accounts_df.createOrReplaceTempView("shawEmployees")
# MAGIC
# MAGIC #Output File from DEV team
# MAGIC account_matched_ruleset_df.createOrReplaceTempView("AcctMatchedRuleset")
# MAGIC
# MAGIC #Full Rogers or Full Shaw
# MAGIC MatchedEntity_df.createOrReplaceTempView("MatchedEntity")
# MAGIC
# MAGIC #Consider Global views instead of temp views if you plan to split this notebook into separate notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC ##TC-A-12

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Records that match based on address normalization

# COMMAND ----------

-- Find example records in the matched result set that match on normalized address, and do not otherwise match on original address
-- With the focus on address, only results of Rules #13, #14, #15 are picked to compare results of join btwn rogers & shaw on original address vs on normalized address
with results as
(
  select 
  ruleset_id
--   , R.address as raw_r_address, 
  , concat_ws(" ", S.address, S.city, S.state) as raw_s_address
  , R.address as raw_r_address 
--  , concat_ws(" ", S.address, S.city, S.state) as raw_s_address
--   , r_address_cl, s_address_cl 
--   , r_service_address_cl, s_service_address_cl
  , s_mailing_address_no_zipcode_dq_cl, r_mailing_address_no_zipcode_dq_cl
--   , r_service_address_no_zipcode_dq_cl, s_service_address_no_zipcode_dq_cl 
  
    , case
    when  (r_address_cl =concat_ws(" ", r_address_cl, r_city_cl, r_state_cl) or s_address_cl =concat_ws(" ", s_address_cl, s_city_cl, s_state_cl)) then 'true'
    else 'false'
    end match_mail_address
  
  , r_rcis_id_cl , r_fa_id_cl, r_contact_id
  , s_rcis_id_cl , s_fa_id_cl, s_contact_id
  
  from matchedView M
  inner join cl_rogers_contact R on M.r_rcis_id_cl = R.rcis_id_cl and M.r_fa_id_cl = R.fa_id_cl
  inner join cl_shaw_contact S on M.s_rcis_id_cl = S.rcis_id_cl and M.s_fa_id_cl = S.fa_id_cl
  where ruleset_id =14 
)

select * from results where match_mail_address='false' 
-- select * from results where match_mail_address='true' 
-- select count(*) from results where match_mail_address='false' 
-- select count(*) from results where match_mail_address='true' 
-- and 
-- (raw_r_address like '%unit%' 
-- or raw_r_address like '%Rural%'
-- or raw_r_address like '%house%'
-- or raw_r_address like '%door%'
-- or raw_r_address like '%lower%'
-- or raw_r_address like '%upper%'
-- or raw_r_address like '%#%'
-- or raw_r_address like '%ontario%'
-- or raw_r_address like '%basement%')


# COMMAND ----------

# MAGIC %md
# MAGIC ###instances where the normalized address is considerably shorter than the original form

# COMMAND ----------

-- Shaw -- Find example records in the raw inout files with addresses that include GD, PO Box, RR, STN

  select 
  "shaw" as source
   ,concat_ws(" ", S.address, S.city, S.state) as raw_mailing_address , S.mailing_address_full_cl
  ,concat_ws(" ", SW.service_address, SW.service_city, SW.service_province) as raw_service_address , SW.service_address_full_cl
   , S.contact_id, S.rcis_id_cl, S.fa_id_cl

  from cl_shaw_contact S 
  left join cl_shaw_wireline SW on S.fa_id_cl = SW.fa_id and S.rcis_id_cl = SW.rcis_id
  where 
length(S.mailing_address_full_cl)*2<length(trim(concat_ws(" ", S.address, S.city, S.state)))
or 
length(SW.service_address_full_cl)*2<length(trim(concat_ws(" ", SW.service_address, SW.service_city, SW.service_province)))

union 

  select 
  "rogers" as source
   , R.address as raw_mailing_address , R.mailing_address_full_cl
  , RW.service_address as raw_service_address , RW.service_address_full_cl
   , R.contact_id, R.rcis_id_cl, R.fa_id_cl

  from cl_shaw_contact R 
  left join cl_shaw_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.rcis_id
  where 
length(R.mailing_address_full_cl)*2<length(trim(R.address))
or 
length(RW.service_address_full_cl)*2<length(trim(RW.service_address))

# COMMAND ----------

# MAGIC %md
# MAGIC ##TC-A-14 (April 11)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rogers Alternate street types

# COMMAND ----------

-- Find example records in the raw inout files with lomg forms of street address type


  select
--  "Rogers" as source,
     R.x_street_type as r_street_type, R.streettype,
    R.address, R.mailing_address_full_cl
    , RW.service_address, RW.service_address_full_cl
    , R.rcis_id_cl, R.fa_id_cl

  
  from cl_rogers_contact R 
  left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.rcis_id

  where 
--   lower (R.address) like '%culross-green%'
--   lower (R.address) like '%po box%po box%'
--   lower(x_street_type) in ('street', 'drive', 'cresent', 'Avenue', 'Road') 
  --and rcis_id_cl = 408252105
--   or length(x_street_type)>4 
  --or length(service_street_type)>4
--   or lower(R.address) like '% street%'
--    lower(R.address) like '% avenue%'
--   or lower(R.address) like '% drive%'
--   or lower(R.address) like '% road%'
   lower(R.streettype) != 'cres'
   and  lower(R.address) like '% cresent%'
--  and  lower(R.mailing_address_no_zipcode_dq_cl) like '%creses%'
--  lower(R.mailing_address_no_zipcode_dq_cl) like '%culross-green%' or  lower(RW.service_address_full_cl) like '%culross-green%'
--  lower(R.mailing_address_no_zipcode_dq_cl) like '%culross-green%' 
--   or lower(R.address) like '% avenue%'
--   or lower(R.address) like '% drive%'
--   or lower(R.address) like '% road%'



# COMMAND ----------

-- Find example records in the raw inout files with lomg forms of street address type

  select 
  "Rogers" as source,
--     R.x_street_type as r_street_type, R.streettype,
    R.address, R.mailing_address_no_zipcode_dq_cl
    , RW.service_address, RW.service_address_no_zipcode_dq_cl
    , R.rcis_id_cl, R.fa_id_cl

  
  from cl_rogers_contact R 
  left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.rcis_id

  where   
  lower(R.mailing_address_no_zipcode_dq) like '% road%' or  lower(RW.service_address_no_zipcode_dq) like '% road%'

union
  select 
  "Shaw" as source,
--     R.x_street_type as r_street_type, R.streettype,
    S.address, S.mailing_address_no_zipcode_dq_cl
    , SW.service_address, SW.service_address_no_zipcode_dq_cl
    , S.rcis_id_cl, S.fa_id_cl

  
  from cl_shaw_contact S 
  left join cl_shaw_wireline SW on S.contact_id = SW.contact_id and S.rcis_id_cl = SW.rcis_id

  where   
  lower(S.mailing_address_no_zipcode_dq) like '% road%' or  lower(SW.service_address_no_zipcode_dq) like '% road%'

# COMMAND ----------

-- Find behavior against crescent / cresent as wrong spelling

  select 
  "Rogers" as source,
--     R.x_street_type as r_street_type, R.streettype,
    R.address, R.mailing_address_no_zipcode_dq_cl
    , R.mailing_address_no_zipcode_dq
    , R.rcis_id_cl, R.fa_id_cl

  
  from cl_rogers_contact R 
--   left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.x_rcis_id

  where 

--   lower(R.address) like '% avenue%' or
--    lower(R.address) like '% cr %' or   
--    lower(R.address) like '% av %'
--    lower(R.streettype) = 'cr'
--    lower(R.streettype) = 'av'
--    lower(R.streettype) = 'pk'
lower (R.address) like '% park%' and lower(R.streettype) = 'pk'
--    lower(R.streettype) = 'pky'
--    lower(R.streettype) = 'blvd'
--   lower(R.address) like '% crescent%' or   
--   lower(R.address) like '% cresent%'
--   or lower(R.address) like '% avenue%'
--   or lower(R.address) like '% drive%'
--   or lower(R.address) like '% road%'


# COMMAND ----------

# MAGIC %md
# MAGIC ###Edge cases of street name similar to street type

# COMMAND ----------

-- Find example records in the raw inout files with Avenue as street name

  select 
  
    R.x_street_type as r_street_type, R.x_street_type_cl as r_street_type_cl

    , RW.service_street_type as r_street_type, RW.service_street_type_cl as r_street_type_cl 
    , R.address, R.mailing_address_no_dq_cl
    , RW.service_address as r_service_address
    , R.contact_id_cl, R.rcis_id_cl, R.fa_id_cl

  
  from cl_rogers_contact R 
  left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.x_rcis_id

  where 
  lower(address) like '% avenue road%'
  or lower(address) like '% avenue rd%'
  or lower(service_address) like '% avenue road%'
  or lower(address) like '% avenue rd%'
  


# COMMAND ----------

# MAGIC %md
# MAGIC ##TC-A-15

# COMMAND ----------

# MAGIC %md
# MAGIC ###Validating unit numbers

# COMMAND ----------

  select 
  
   R.address, R.mailing_address_no_zipcode_cl
   , RW.service_address , RW.service_address_no_zipcode_dq_cl
   , R.contact_id, R.rcis_id_cl, R.fa_id_cl

  from cl_rogers_contact R 
  left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.rcis_id

  where 
lower(concat(address,service_address)) like '% unit%' 
   lower(concat(address,service_address)) like '% #%' or lower(concat(address,service_address)) like '% number%'
  or lower(concat(address,service_address)) like '% apartment%' 
  or lower(concat(address,service_address)) like '% apt%' 
  or lower(concat(address,service_address)) like '% suite%' 



# COMMAND ----------

-- Shaw -- Find example records in the raw inout files with addresses that include GD, PO Box, RR, STN

  select 
  
   concat_ws(" ", S.address, S.city, S.state), S.mailing_address_no_zipcode_cl
  ,concat_ws(" ", SW.service_address, SW.service_city, SW.service_province), SW.service_address , SW.service_address_no_zipcode_dq_cl
   , S.contact_id, S.rcis_id_cl, S.fa_id_cl

  from cl_shaw_contact S 
  left join cl_shaw_wireline SW on S.fa_id_cl = SW.fa_id and S.rcis_id_cl = SW.rcis_id

  where 
   lower(concat(address,service_address)) like '% unit%' 
  or lower(concat(address,service_address)) like '% #%' or lower(concat(address,service_address)) like '% number%'
  or lower(concat(address,service_address)) like '% apartment%' 
  or lower(concat(address,service_address)) like '% apt%' 
  or lower(concat(address,service_address)) like '% suite%' 



# COMMAND ----------

--  PROOF that parser is working as expected:

select R.dq_full_name_cl as r_fullname, S.dq_full_name_cl as S_fullname 
, R.address as r_address, concat_ws(" ", S.address, S.city, S.State, S.zipcode) as s_address
, R.mailing_address_no_zipcode_dq_cl as r_clean_address
, S.mailing_address_no_zipcode_dq_cl as s_clean_address
, M.s_rcis_id_cl, M.s_fa_id_cl, M.r_rcis_id_cl, M.r_fa_id_cl, M.*
from matchedView M 
inner join cl_shaw_contact S on M.s_rcis_id_cl=S.rcis_id_cl and M.s_fa_id_cl = S.fa_id_cl
inner join cl_rogers_contact R on M.r_rcis_id_cl=R.rcis_id_cl and M.r_fa_id_cl = R.fa_id_cl
where M.Ruleset_id=17 and 
(
lower(concat_ws(" ", R.address, S.address)) like '% no.%' 
--  lower(concat_ws(" ", R.address, S.address)) like '% apartment%' 
--  or lower(concat_ws(" ", R.address, S.address)) like '% apt%' 
--  or lower(concat_ws(" ", R.address, S.address)) like '% suite%' 
--  or lower(concat_ws(" ", R.address, S.address)) like '% unit%' 
  )
  
