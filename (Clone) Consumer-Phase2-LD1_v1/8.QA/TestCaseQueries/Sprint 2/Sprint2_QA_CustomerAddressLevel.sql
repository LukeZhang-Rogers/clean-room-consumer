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
-- MAGIC shaw_path = "/mnt/development/Processed/Iteration6/Shaw" 
-- MAGIC rogers_path = "/mnt/development/Processed/Iteration6/Rogers" 
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
-- MAGIC matched_df = spark.read.format("parquet").load("/mnt/development/silver/iteration6/matched")
-- MAGIC unmatched_df = spark.read.format("parquet").load("/mnt/development/silver/iteration6/unmatched")

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
-- MAGIC #Consider Global views instead of temp views if you plan to split this notebook into separate notebooks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TestCase TC-A-01 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###French Characters 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### no diacritical marks in Cleaned data

-- COMMAND ----------

-- Check for existance of diacritical characters in Rogers cleaned data

with concatenatedClean as
(
select  concat_ws(" ", S.mailing_address_full_cl, S.city_cl, S.state_cl, SW.service_address_full_cl, SW.service_city_cl, SW.service_province_cl) as concatRecord
  , "Shaw" as source
  , concat_ws(" ", S.contact_id, S.rcis_id, S.fa_id) as concatID
  from cl_shaw_contact S 
  left join cl_shaw_wireline SW on S.fa_id_cl = SW.fa_id and S.rcis_id_cl = SW.rcis_id 
union
select  concat_ws(" ", R.mailing_address_full_cl, R.city_cl, R.state_cl, RW.service_address_full_cl, RW.service_city_cl, RW.service_province_cl) as concatRecord
  , "Rogers" as source
  , concat_ws(" ", R.contact_id, R.rcis_id, R.fa_id) as concatID
  from cl_rogers_contact R 
  left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.rcis_id 
)

select source, count (concatID)
from concatenatedClean
where 
contains (concatRecord, 'é')

or contains (concatRecord, 'à') 
or contains (concatRecord, 'è')
or contains (concatRecord, 'ì')
or contains (concatRecord, 'ò')
or contains (concatRecord, 'ù')

or contains (concatRecord, 'â')
or contains (concatRecord, 'ê')
or contains (concatRecord, 'î')
or contains (concatRecord, 'ô')
or contains (concatRecord, 'û')

or contains (concatRecord, 'ö')

or contains (concatRecord, 'ç')
or contains (concatRecord, 'ø')

or contains (concatRecord, 'ñ')
or contains (concatRecord, 'ã')
or contains (concatRecord, 'õ')

or contains (concatRecord, 'æ')
or contains (concatRecord, 'å')
group by source

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### #of records with diacritical characters in Rogers Raw data

-- COMMAND ----------

-- Check for existance of diacritical characters in Rogers Raw data
select count (*)
--first_name, last_name, address, x_rcis_id , fa_id, contact_id
from r_rogers_contact 

where 
contains (lower(concat (first_name, " ", last_name, " ", address)), 'é')

or contains (lower(concat (first_name, " ", last_name, " ", address)), 'à') 
or contains (lower(concat (first_name, " ", last_name, " ", address)), 'è')
or contains (lower(concat (first_name, " ", last_name, " ", address)), 'ì')
or contains (lower(concat (first_name, " ", last_name, " ", address)), 'ò')
or contains (lower(concat (first_name, " ", last_name, " ", address)), 'ù')

or contains (lower(concat (first_name, " ", last_name, " ", address)), 'â')
or contains (lower(concat (first_name, " ", last_name, " ", address)), 'ê')
or contains (lower(concat (first_name, " ", last_name, " ", address)), 'î')
or contains (lower(concat (first_name, " ", last_name, " ", address)), 'ô')
or contains (lower(concat (first_name, " ", last_name, " ", address)), 'û')

or contains (lower(concat (First_Name, " ", last_name, " ", address)), 'ö')

or contains (lower(concat (first_name, " ", last_name, " ", address)), 'ç')
or contains (lower(concat (first_name, " ", last_name, " ", address)), 'ø')

or contains (lower(concat (first_name, " ", last_name, " ", address)), 'ñ')
or contains (lower(concat (first_name, " ", last_name, " ", address)), 'ã')
or contains (lower(concat (first_name, " ", last_name, " ", address)), 'õ')

or contains (lower(concat (first_name, " ", last_name, " ", address)), 'æ')
or contains (lower(concat (first_name, " ", last_name, " ", address)), 'å')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### # of records with diacritical characters in Shaw Raw data

-- COMMAND ----------

-- Check for existance of diacritical characters in shaw Raw data
select count (*)
--first_name, last_name, address, x_rcis_id , fa_id, contact_id
from r_shaw_contact C inner join r_shaw_wireline W
on C.contact_id = W.contact_id and C.rcis_id=W.rcis_id

where 
contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'é')

or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'à') 
or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'è')
or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'ì')
or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'ò')
or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'ù')

or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'â')
or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'ê')
or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'î')
or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'ô')
or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'û')

or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'ö')

or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'ç')
or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'ø')

or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'ñ')
or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'ã')
or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'õ')

or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'æ')
or contains (lower(concat (first_name, last_name, C.address, W.service_address)), 'å')

-- COMMAND ----------



-- COMMAND ----------

-- find cases where french characters in firstname/lastname on one side is matching to the normal alphabetic characters in the other side
--TC-A-01


query_rogers_containfrench as
(select x_rcis_id , contact_id, fa_id, first_name, last_name
from r_rogers_contact where 
contains (lower(concat (first_name, last_name)), 'è') 
or contains (lower(concat (first_name, " ", last_name)), 'ì')
or contains (lower(concat (first_name, " ", last_name)), 'ò')
or contains (lower(concat (first_name, " ", last_name)), 'ù')
or contains (lower(concat (first_name, " ", last_name)), 'û')
), 

query_shaw_containfrench as
(select rcis_id , contact_id, fa_id, first_name, last_name, address
from r_shaw_contact where 
contains (lower(concat (first_name, last_name)), 'è') 
or contains (lower(concat (first_name, " ", last_name)), 'ì')
or contains (lower(concat (first_name, " ", last_name)), 'ò')
or contains (lower(concat (first_name, " ", last_name)), 'ù')
or contains (lower(concat (first_name, " ", last_name)), 'û')
), 

distinctMatch as 
(select distinct s_contact_id_cl, s_rcis_id_cl, s_fa_id_cl, s_first_name_cl, s_last_name_cl, r_contact_id_cl, r_rcis_id_cl, r_fa_id_cl, r_first_name_cl, r_last_name_cl
from matchedView
where ruleset_id<16
)


select 
RF.first_name as r_first_name, S.first_name as s_first_name, 
RF.last_name as r_last_name, S.last_name as s_last_name, 
M.r_rcis_id_cl, M.r_fa_id_cl, M.r_contact_id_cl, M.s_rcis_id_cl, M.s_fa_id_cl, M.s_contact_id_cl
from query_rogers_containfrench RF 
inner join distinctMatch M on RF.x_rcis_id=M.r_rcis_id_cl and RF.fa_id=M.r_fa_id_cl and RF.contact_id=M.r_contact_id_cl
inner join r_shaw_contact S on M.s_rcis_id_cl=S.rcis_id and M.s_fa_id_cl=S.fa_id and M.s_contact_id_cl=S.contact_id
where lower(concat (RF.first_name, RF.last_name))!=lower(concat (S.first_name, S.last_name))

union
select 
R.first_name as r_first_name, SF.first_name as s_first_name, 
R.last_name as r_last_name, SF.last_name as s_last_name, 
M.r_rcis_id_cl, M.r_fa_id_cl, M.r_contact_id_cl, M.s_rcis_id_cl, M.s_fa_id_cl, M.s_contact_id_cl
from query_shaw_containfrench SF 
inner join distinctMatch M on SF.rcis_id=M.s_rcis_id_cl and SF.fa_id = M.s_fa_id_cl and SF.contact_id = M.s_contact_id_cl
inner join r_rogers_contact R on M.r_rcis_id_cl=R.x_rcis_id and M.r_fa_id_cl=R.fa_id and M.s_contact_id_cl=R.contact_id
where lower(concat (R.first_name, R.last_name))!=lower(concat (SF.first_name, SF.last_name))

-- results only contain examples of ô

-- COMMAND ----------

-- find cases where french characters in firstname/lastname on one side is matching to the normal alphabetic characters in the other side
--TC-A-01
with 

query_rogers_containfrench as
(select x_rcis_id , contact_id, fa_id, first_name, last_name
from r_rogers_contact where 
contains (lower(concat (first_name, last_name)), 'é') 
), 

query_shaw_containfrench as
(select rcis_id , contact_id, fa_id, first_name, last_name, address
from r_shaw_contact where 
contains (lower(concat (first_name, " ", last_name, " ", address)), 'é') 
), 

distinctMatch as 
(select distinct s_contact_id_cl, s_rcis_id_cl, s_fa_id_cl, s_first_name_cl, s_last_name_cl, r_contact_id_cl, r_rcis_id_cl, r_fa_id_cl, r_first_name_cl, r_last_name_cl
from matchedView
where ruleset_id<16
)


select 
RF.first_name as r_first_name, S.first_name as s_first_name, 
RF.last_name as r_last_name, S.last_name as s_last_name, 
M.r_rcis_id_cl, M.r_fa_id_cl, M.r_contact_id_cl, M.s_rcis_id_cl, M.s_fa_id_cl, M.s_contact_id_cl
from query_rogers_containfrench RF 
inner join distinctMatch M on RF.x_rcis_id=M.r_rcis_id_cl and RF.fa_id=M.r_fa_id_cl and RF.contact_id=M.r_contact_id_cl
inner join r_shaw_contact S on M.s_rcis_id_cl=S.rcis_id and M.s_fa_id_cl=S.fa_id and M.s_contact_id_cl=S.contact_id
where lower(concat (RF.first_name, RF.last_name))!=lower(concat (S.first_name, S.last_name))

union
select 
R.first_name as r_first_name, SF.first_name as s_first_name, 
R.last_name as r_last_name, SF.last_name as s_last_name, 
M.r_rcis_id_cl, M.r_fa_id_cl, M.r_contact_id_cl, M.s_rcis_id_cl, M.s_fa_id_cl, M.s_contact_id_cl
from query_shaw_containfrench SF 
inner join distinctMatch M on SF.rcis_id=M.s_rcis_id_cl and SF.fa_id = M.s_fa_id_cl and SF.contact_id = M.s_contact_id_cl
inner join r_rogers_contact R on M.r_rcis_id_cl=R.x_rcis_id and M.r_fa_id_cl=R.fa_id and M.s_contact_id_cl=R.contact_id
where lower(concat (R.first_name, R.last_name))!=lower(concat (SF.first_name, SF.last_name))


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### matching caret/circumflex to alphabetical characters

-- COMMAND ----------

-- find cases where french characters in firstname/lastname on one side is matching to the normal alphabetic characters in the other side
--TC-A-01
with 

query_rogers_containfrench as
(select x_rcis_id , contact_id, fa_id, first_name, last_name
from r_rogers_contact where 
contains (lower(concat (first_name, last_name)), 'â') 
or contains (lower(concat (first_name, " ", last_name)), 'ê')
or contains (lower(concat (first_name, " ", last_name)), 'î')
or contains (lower(concat (first_name, " ", last_name)), 'ô')
or contains (lower(concat (first_name, " ", last_name)), 'û')
), 

query_shaw_containfrench as
(select rcis_id , contact_id, fa_id, first_name, last_name, address
from r_shaw_contact where 
contains (lower(concat (first_name, last_name)), 'â') 
or contains (lower(concat (first_name, " ", last_name)), 'ê')
or contains (lower(concat (first_name, " ", last_name)), 'î')
or contains (lower(concat (first_name, " ", last_name)), 'ô')
or contains (lower(concat (first_name, " ", last_name)), 'û')
), 

distinctMatch as 
(select distinct s_contact_id_cl, s_rcis_id_cl, s_fa_id_cl, s_first_name_cl, s_last_name_cl, r_contact_id_cl, r_rcis_id_cl, r_fa_id_cl, r_first_name_cl, r_last_name_cl
from matchedView
where ruleset_id<16
)


select 
RF.first_name as r_first_name, S.first_name as s_first_name, 
RF.last_name as r_last_name, S.last_name as s_last_name, 
M.r_rcis_id_cl, M.r_fa_id_cl, M.r_contact_id_cl, M.s_rcis_id_cl, M.s_fa_id_cl, M.s_contact_id_cl
from query_rogers_containfrench RF 
inner join distinctMatch M on RF.x_rcis_id=M.r_rcis_id_cl and RF.fa_id=M.r_fa_id_cl and RF.contact_id=M.r_contact_id_cl
inner join r_shaw_contact S on M.s_rcis_id_cl=S.rcis_id and M.s_fa_id_cl=S.fa_id and M.s_contact_id_cl=S.contact_id
where lower(concat (RF.first_name, RF.last_name))!=lower(concat (S.first_name, S.last_name))

union
select 
R.first_name as r_first_name, SF.first_name as s_first_name, 
R.last_name as r_last_name, SF.last_name as s_last_name, 
M.r_rcis_id_cl, M.r_fa_id_cl, M.r_contact_id_cl, M.s_rcis_id_cl, M.s_fa_id_cl, M.s_contact_id_cl
from query_shaw_containfrench SF 
inner join distinctMatch M on SF.rcis_id=M.s_rcis_id_cl and SF.fa_id = M.s_fa_id_cl and SF.contact_id = M.s_contact_id_cl
inner join r_rogers_contact R on M.r_rcis_id_cl=R.x_rcis_id and M.r_fa_id_cl=R.fa_id and M.s_contact_id_cl=R.contact_id
where lower(concat (R.first_name, R.last_name))!=lower(concat (SF.first_name, SF.last_name))

-- results only contain examples of ô

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TestCase TC-A-02

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### TestCase case sensitivity

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### name match regardless of case of fname

-- COMMAND ----------

-- find cases to show first names matching regardless of case sinsitivity
with distinctMatch as 
(select distinct s_contact_id_cl, s_rcis_id_cl, s_fa_id_cl, s_first_name_cl, s_last_name_cl, r_contact_id_cl, r_rcis_id_cl, r_fa_id_cl, r_first_name_cl, r_last_name_cl
from matchedView
--where Ruleset_id<16
)

select R.first_name as r_first_name, S.first_name as s_first_name, M.r_first_name_cl, R.last_name as r_last_name, S.last_name as s_last_name, M.r_rcis_id_cl, M.r_fa_id_cl, M.r_contact_id_cl, M.s_rcis_id_cl, M.s_fa_id_cl, M.s_contact_id_cl
from distinctMatch M
inner join r_rogers_contact R on M.r_rcis_id_cl=R.x_rcis_id and M.r_fa_id_cl=R.fa_id and M.r_contact_id_cl=R.contact_id
inner join r_shaw_contact S on M.s_rcis_id_cl=S.rcis_id and M.s_fa_id_cl=S.fa_id and M.s_contact_id_cl=S.contact_id
where trim (R.first_name) != trim (S.first_name) and lower(trim(R.first_name)) = lower(trim(S.first_name))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Matching despite of special characters

-- COMMAND ----------

with distinctMatch as 
(
  select distinct s_contact_id_cl, s_rcis_id_cl, s_fa_id_cl, s_first_name_cl, s_last_name_cl, r_contact_id_cl, r_rcis_id_cl, r_fa_id_cl, r_first_name_cl, r_last_name_cl
  from matchedView
  where ruleset_id<16
)
select R.first_name as r_first_name, S.first_name as s_first_name, 
R.last_name as r_last_name, S.last_name as s_last_name, 
--M.r_first_name_cl, M.s_first_name_cl, 
M.r_rcis_id_cl, M.r_fa_id_cl, M.r_contact_id_cl, M.s_rcis_id_cl, M.s_fa_id_cl, M.s_contact_id_cl
--count (*)

from distinctMatch M
inner join r_rogers_contact R on M.r_rcis_id_cl=R.x_rcis_id and M.r_fa_id_cl=R.fa_id and M.r_contact_id_cl=R.contact_id
inner join r_shaw_contact S on M.s_rcis_id_cl=S.rcis_id and M.s_fa_id_cl=S.fa_id and M.s_contact_id_cl=S.contact_id
where lower(concat(R.first_name, R.last_name)) != lower(concat(S.first_name, S.last_name))

-- COMMAND ----------

-- Code currently not working!!
-- find cases where first names on both sides are not the same but they will match (due to DQ steps) - excluding differences in case 

with distinctMatch as 
(
  select distinct s_rcis_id_cl, s_fa_id_cl, s_first_name_cl, s_last_name_cl, r_rcis_id_cl, r_fa_id_cl, r_first_name_cl, r_last_name_cl
  from matchedView
  where ruleset_id<16
)
, rogersContainSpecialChar as 
(
  select * from cl_rogers_contact R
  where concat(R.first_name, R.last_name) like '%-'
  or concat(R.first_name, R.last_name) like '%;'
  or concat(R.first_name, R.last_name) like '%,'
  or concat(R.first_name, R.last_name) like '%('
  or concat(R.first_name, R.last_name) like '%)'
)
, shawContainSpecialChar as 
(
  select * from cl_shaw_contact S
  where concat(S.first_name, S.last_name) like '%-'
  or concat(S.first_name, S.last_name) like '%;'
  or concat(S.first_name, S.last_name) like '%,'
  or concat(S.first_name, S.last_name) like '%('
  or concat(S.first_name, S.last_name) like '%)'
)
select R.first_name as r_first_name, S.first_name as s_first_name, 
R.last_name as r_last_name, S.last_name as s_last_name, 
--M.r_first_name_cl, M.s_first_name_cl, 
M.r_rcis_id_cl, M.r_fa_id_cl, M.s_rcis_id_cl, M.s_fa_id_cl
--count (*)

from distinctMatch M
inner join rogersContainSpecialChar R on M.r_rcis_id_cl=R.x_rcis_id and M.r_fa_id_cl=R.fa_id 
inner join shawContainSpecialChar S on M.s_rcis_id_cl=S.rcis_id and M.s_fa_id_cl=S.fa_id


-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TestCase TC-A-03

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Forms of missing values of fname,lname

-- COMMAND ----------

select lower(first_name), count(objid) from cl_shaw_contact
where lower(first_name) in ('null', 'unknown', 'na', 'n/a', 'test', 'missing', 'not applicable', 'blank', 'empty')
group by lower(first_name)
order by count(objid) desc

-- COMMAND ----------

-- Shaw first name
select lower(first_name), lower(first_name_cl), lower(dq_first_name_cl), 
count(objid) from cl_shaw_contact
where lower(first_name) in ('null', 'unknown', 'na', 'n/a', 'test', 'missing', 'not applicable', 'blank', 'empty')
group by lower(first_name), lower(first_name_cl), lower(dq_first_name_cl)
order by count(objid) desc

-- COMMAND ----------

-- Shaw last name
select lower(last_name), lower(last_name_cl), lower(dq_last_name_cl), 
count(objid) from cl_shaw_contact
where lower(last_name) in ('null', 'unknown', 'na', 'n/a', 'test', 'missing', 'not applicable', 'blank', 'empty')
group by lower(last_name), lower(last_name_cl), lower(dq_last_name_cl)
order by count(objid) desc

-- COMMAND ----------

-- Rogers last name
select lower(last_name), lower(last_name_cl), lower(dq_last_name_cl), 
count(objid) from cl_rogers_contact
where lower(last_name) in ('null', 'unknown', 'na', 'n/a', 'test', 'missing', 'not applicable', 'blank', 'empty')
group by lower(last_name), lower(last_name_cl), lower(dq_last_name_cl)
order by count(objid) desc

-- COMMAND ----------

-- Rogers first name
select lower(first_name), lower(first_name_cl), lower(dq_first_name_cl), 
count(objid) from cl_rogers_contact
where lower(first_name) in ('null', 'unknown', 'na', 'n/a', 'test', 'missing', 'not applicable', 'blank', 'empty')
group by lower(first_name), lower(first_name_cl), lower(dq_first_name_cl)
order by count(objid) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Proof that removing NA not recommended

-- COMMAND ----------

select * from cl_rogers_contact
where lower(first_name) in ('na', 'n/a') or lower(last_name) in ('na', 'n/a')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Cleaned values of fname,lname

-- COMMAND ----------

-- find proof of cleaned fname,last name
with 
shawNullname as 
(select * from cl_shaw_contact S 
where S.first_name is null or S.first_name ="null" or trim(S.first_name)="" or lower(S.first_name) like "blank" or lower(S.first_name) like "unknown"
or S.last_name is null or S.last_name ="null" or trim(S.last_name)="" or lower(S.last_name) like "blank" or lower(S.last_name) like "unknown"
)
,rogersNullname as 
(select * from cl_rogers_contact R 
where R.first_name is null or R.first_name ="null" or trim(R.first_name)="" or lower(R.first_name) like "blank" or lower(R.first_name) like "unknown"
or R.last_name is null or R.last_name ="null" or trim(R.last_name)="" or lower(R.last_name) like "blank"  or lower(R.last_name) like "unknown"
)

select 
S.first_name, S.last_name, dq_first_name_cl, dq_last_name_cl, S.dq_full_name_cl, S.rcis_id, S.fa_id,
"Shaw" as nullsource 
from shawNullname S 


union 
select
R.first_name, R.last_name, dq_first_name_cl, dq_last_name_cl, R.dq_full_name_cl, R.rcis_id , R.fa_id,
"Rogers" as nullsource 
from rogersNullname R 





-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TestCase TC-A-04

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### examples of fname,lname captured together

-- COMMAND ----------

-- check for any occurances of null first name or last name
--select * from r_rogers_contact R where R.first_name is null or R.last_name is null
select * from r_shaw_contact S where S.first_name is null or S.last_name is null


-- COMMAND ----------

-- Rogers
-- check for any occurances of null first name or last name
--select * from r_rogers_contact R where R.first_name is null or R.last_name is null
select * from r_rogers_contact S where S.first_name is null or S.last_name is null 


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###False negative matches where first name, last name are captured together (only based on names, phone, email)

-- COMMAND ----------

with 
-- Focus on customer level match results and fetch the first name and last name from contact tables for the matched records
distinctMatch as 
(select distinct s_contact_id, s_rcis_id_cl, s_fa_id_cl
, r_contact_id, r_rcis_id_cl, r_fa_id_cl
from matchedView

-- NOTE!! including the below two ineerjoins will add more records to the result, but the extra records do not look ok
-- inner join cl_rogers_contact R on  r_rcis_id_cl = R.rcis_id_cl and r_fa_id_cl = R.fa_id_cl
-- inner join cl_shaw_contact S on  r_rcis_id_cl = S.rcis_id_cl and r_fa_id_cl = S.fa_id_cl
where Ruleset_id<16
)
--further clean first name and last name to clear any fillers
,cl_shaw as 
(
  select 
    Case   
      when first_name_cl is null then ''
      when first_name_cl = 'na' or first_name_cl = 'n/a' then ''
      when first_name_cl ='null' then ''
      when first_name_cl='' then ''
      when first_name_cl = 'blank' then ''
      when first_name_cl = 'unknown' then ''
      when first_name_cl = 'none' then ''
      when first_name_cl = 'empty' then ''
      Else first_name_cl
    End first_name_cl2
  ,
    Case       
      when last_name_cl is null then ''
      when first_name_cl = 'na' or first_name_cl = 'n/a' then ''
      when last_name_cl ='null' then ''
      when last_name_cl='' then ''
      when last_name_cl = 'blank' then ''
      when last_name_cl = 'unknown' then ''
      when last_name_cl = 'none' then ''
      when last_name_cl = 'empty' then ''
      Else last_name_cl 
    End last_name_cl2
  , 
  *
  from cl_shaw_contact 
  where first_name_cl is null or last_name_cl is null 
  or first_name_cl ='null' or first_name_cl='' or first_name_cl = 'blank' or first_name_cl = 'unknown' or first_name_cl = 'none' or first_name_cl = 'empty' 
  or last_name_cl ='null' or last_name_cl='' or last_name_cl = 'blank' or last_name_cl = 'unknown' or last_name_cl = 'none' or last_name_cl = 'empty' 

),
  
cl_rogers as 
(
select 
    Case   
      when first_name_cl is null then ''
      when first_name_cl ='null' then ''
      when first_name_cl='' then ''
      when first_name_cl = 'comp' then ''
      when first_name_cl = 'blank' then ''
      when first_name_cl = 'unknown' then ''
      when first_name_cl = 'none' then ''
      when first_name_cl = 'empty' then ''
      Else first_name_cl
    End first_name_cl2
  ,
    Case          
      when last_name_cl is null then ''
      when last_name_cl ='null' then ''
      when last_name_cl='' then ''
      when last_name_cl = 'blank' then ''
      when last_name_cl = 'unknown' then ''
      when last_name_cl = 'none' then ''
      when last_name_cl = 'empty' then ''
      Else last_name_cl
    End last_name_cl2
  , 
  *
  from cl_rogers_contact 
  where first_name_cl is null or last_name_cl is null 
  or first_name_cl ='null' or first_name_cl='' or first_name_cl = 'blank' or first_name_cl = 'unknown' or first_name_cl = 'none' or first_name_cl = 'empty' 
  or last_name_cl ='null' or last_name_cl='' or last_name_cl = 'blank' or last_name_cl = 'unknown' or last_name_cl = 'none' or last_name_cl = 'empty' 
)

-- Implement matching logic for concatenated first name and last name the same, and at least a common phone number or an email
select 
  R.first_name_cl as r_first_name_cl, R.last_name_cl as r_last_name_cl, S.first_name_cl as s_first_name_cl, S.last_name_cl as s_last_name_cl
--   concat_ws(" ", R.first_name_cl2 as r_first_name_cl2, R.last_name_cl2 as r_last_name_cl2, S.first_name_cl2 as s_first_name_cl2, S.last_name_cl2 as s_last_name_cl2

  , R.dq_e_mail as r_email, S.dq_e_mail as s_email
   , concat_ws(" ", R.dq_primary_phone_cl, R.dq_alternate_phone_1_cl, R.dq_alternate_phone_2_cl) as r_phones
   , concat_ws(" ", S.dq_primary_phone_cl, S.dq_alternate_phone_1_cl, S.dq_alternate_phone_2_cl) as s_phones
 , R.rcis_id_cl as r_rcic_id_cl, R.fa_id_cl as r_fa_id_cl
 , S.rcis_id_cl as s_rcic_id_cl, S.fa_id_cl as s_fa_id_cl
, M.R_rcis_id_cl as matched_r_rcis_id
, M.s_rcis_id_cl as matched_s_rcis_id
  
  from cl_rogers R   
  inner join cl_shaw S
  on concat_ws(" ", R.first_name_cl2, R.last_name_cl2) = concat_ws(" ", S.first_name_cl2, S.last_name_cl2)
  and 
  ( R.dq_e_mail_cl = S.dq_e_mail
  or R.dq_primary_phone_cl = S.dq_primary_phone_cl or R.dq_primary_phone_cl = S.dq_alternate_phone_1_cl or R.dq_primary_phone_cl = S.dq_alternate_phone_2_cl
  or R.dq_alternate_phone_1_cl = S.dq_primary_phone_cl or R.dq_alternate_phone_1_cl = S.dq_alternate_phone_1_cl or R.dq_alternate_phone_1_cl = S.dq_alternate_phone_2_cl
  or R.dq_alternate_phone_2_cl = S.dq_primary_phone_cl or R.dq_alternate_phone_2_cl = S.dq_alternate_phone_1_cl or R.dq_alternate_phone_2_cl = S.dq_alternate_phone_2_cl
  )
  -- find the above found matched records that do not exist in Matched Result out put
  left join distinctMatch M
  on R.rcis_id_cl = M.r_rcis_id_cl and S.rcis_id_cl = M.s_rcis_id_cl
  and R.fa_id_cl = M.r_fa_id_cl and S.fa_id_cl = M.s_fa_id_cl 
  where M.r_rcis_id_cl is null
  
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TestCase TC-A-05

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Emails containing + sign

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Shaw Emails containing + sign

-- COMMAND ----------

select S.e_mail, 
concat(
Left(S.e_mail, charindex('+', S.e_mail)-1)
, substring (S.e_mail, charindex('@', S.e_mail), LENgth(S.e_mail)-charindex('@', S.e_mail)+1)
) as s_e_mail_cl
from r_shaw_contact S 
where S.e_mail like '%+%' 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Rogers Emails containing + sign

-- COMMAND ----------

select R.e_mail, 
concat(
  Left(R.e_mail, charindex('+', R.e_mail)-1)
  , substring (R.e_mail, charindex('@', R.e_mail), LENgth(R.e_mail)-charindex('@', R.e_mail)+1)
) as r_e_mail_cl
from r_rogers_contact R 
where R.e_mail like '%+%' 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####False Negative matches on records containing '+' in email

-- COMMAND ----------

-- clean shaw emails for the ones containing + by removing characters between + and @ and adding it as a new column
With 
-- list of cleaned emails from Shaw that contain +
ShawEmails as
( select 
  Case 
    when S.e_mail like '%+%@%' Then
      concat(
      Left(S.e_mail, charindex('+', S.e_mail)-1)
      , substring (S.e_mail, charindex('@', S.e_mail), LENgth(S.e_mail)-charindex('@', S.e_mail)+1))
    Else S.e_mail
  End s_e_mail_cl
  ,*
  from cl_shaw_contact S 
)

-- clean rogers emails for the ones containing + by removing characters between + and @ and adding it as a new column
,rogersEmails as
( select 
  Case 
    when R.e_mail like '%+%@%' Then
      concat(
      Left(R.e_mail, charindex('+', R.e_mail)-1)
      , substring (R.e_mail, charindex('@', R.e_mail), LENgth(R.e_mail)-charindex('@', R.e_mail)+1))
    Else R.e_mail
  End r_e_mail_cl
  ,*
  from cl_rogers_contact R
)

-- join between rogers on shaw only on cleaned emails and providing first name & last name for the tester to judge how many number of records should've matched on ruleset #11
select 
lower(concat (R.first_name, ' ', R.last_name)) as r_name, lower(concat (S.first_name, ' ', S.last_name)) as s_name
, R.e_mail as r_email
, S.e_mail as s_email
, R.rcis_ID_cl as r_rcis_id, R.fa_ID_cl as r_fa_id, S.rcis_ID_cl as s_rcis_id, S.fa_ID as s_fa_id
from rogersEmails R
inner join ShawEmails S
on R.r_e_mail_cl=S.s_e_mail_cl

Left join matchedView M  
  on R.rcis_ID_cl = M.r_rcis_id_cl and S.rcis_ID = M.s_rcis_id_cl
  and R.fa_id_cl = M.r_fa_id_cl and S.fa_id_cl = M.s_fa_id_cl 

where R.e_mail !=S.e_mail 
and lower(concat (R.first_name, ' ', R.last_name))=lower(concat (S.first_name, ' ', S.last_name))
and M.r_rcis_id_cl is null

-- COMMAND ----------

select * from matchedView where r_rcis_id_cl=438991563 and r_fa_id_cl=740685805

-- COMMAND ----------

select * from cl_rogers_contact where rcis_id=438991563 and fa_id=740685805

-- COMMAND ----------

select * from cl_shaw_contact where rcis_id=2602240 and fa_id=01882018209

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TestCase TC-A-06

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Filler Emails

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Rogers # of filler emails in input files

-- COMMAND ----------

select count(*)
from r_rogers_contact R 
where 
R.e_mail like 'DNS%' 
or R.e_mail like '%@DNS.%'
or R.e_mail like 'DONOTREPLY%'
or R.e_mail like 'NOREPLY%'
or R.e_mail like '@.%'
or R.e_mail like 'DONOEMAIL'
or R.e_mail like 'ABC@'
or R.e_mail like 'ROGERS@ROGERS%'
or R.e_mail like 'NONE@%'
or R.e_mail like 'No@%'
or R.e_mail like 'NOBODY@%'
or R.e_mail like 'E_MAIL@NOE_MAIL%'
or R.e_mail like '%E_MAIL@EMAIL%'
or R.e_mail like 'E_MAIL@NO-E_MAIL%'
or R.e_mail like 'NO-E_MAIL@%'
or R.e_mail like 'NO_E_MAIL@%'
or R.e_mail like '%@NOEMAIL%'
or R.e_mail like 'DECLINED@%'


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Shaw # of filler emails in Shaw input files

-- COMMAND ----------

select count(*)
from r_shaw_contact S 
where 
S.e_mail like 'DNS%' 
or S.e_mail like '%@DNS.%'
or S.e_mail like 'DONOTREPLY%'
or S.e_mail like 'NOREPLY%'
or S.e_mail like '@.%'
or S.e_mail like 'DONOEMAIL'
or S.e_mail like 'ABC@'
or S.e_mail like 'ROGERS@ROGERS%'
or S.e_mail like 'NONE@%'
or S.e_mail like 'No@%'
or S.e_mail like 'NOBODY@%'
or S.e_mail like 'E_MAIL@NOE_MAIL%'
or S.e_mail like '%E_MAIL@EMAIL%'
or S.e_mail like 'E_MAIL@NO-E_MAIL%'
or S.e_mail like 'NO-E_MAIL@%'
or S.e_mail like 'NO_E_MAIL@%'
or S.e_mail like '%@NOEMAIL%'
or S.e_mail like 'DECLINED@%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Validate no match on filler emails where email is required

-- COMMAND ----------

With 
shawFillerEmails as
(
  select * from r_shaw_contact S 
  where 
  S.e_mail like 'DNS%' 
  or S.e_mail like '%@DNS.%'
  or S.e_mail like 'DONOTREPLY%'
  or S.e_mail like 'NOREPLY%'
  or S.e_mail like '@.%'
  or S.e_mail like 'DONOEMAIL'
  or S.e_mail like 'ABC@'
  or S.e_mail like 'ROGERS@ROGERS%'
  or S.e_mail like 'NONE@%'
  or S.e_mail like 'No@%'
  or S.e_mail like 'NOBODY@%'
  or S.e_mail like 'E_MAIL@NOE_MAIL%'
  or S.e_mail like '%E_MAIL@EMAIL%'
  or S.e_mail like 'E_MAIL@NO-E_MAIL%'
  or S.e_mail like 'NO-E_MAIL@%'
  or S.e_mail like 'NO_E_MAIL@%'
  or S.e_mail like '%@NOEMAIL%'
  or S.e_mail like 'DECLINED@%'
)

, rogersFillerEmails as 
(
  select * from r_rogers_contact R 
  where 
  R.e_mail like 'DNS%' 
  or R.e_mail like '%@DNS.%'
  or R.e_mail like 'DONOTREPLY%'
  or R.e_mail like 'NOREPLY%'
  or R.e_mail like '@.%'
  or R.e_mail like 'DONOEMAIL'
  or R.e_mail like 'ABC@'
  or R.e_mail like 'ROGERS@ROGERS%'
  or R.e_mail like 'NONE@%'
  or R.e_mail like 'No@%'
  or R.e_mail like 'NOBODY@%'
  or R.e_mail like 'E_MAIL@NOE_MAIL%'
  or R.e_mail like '%E_MAIL@EMAIL%'
  or R.e_mail like 'E_MAIL@NO-E_MAIL%'
  or R.e_mail like 'NO-E_MAIL@%'
  or R.e_mail like 'NO_E_MAIL@%'
  or R.e_mail like '%@NOEMAIL%'
  or R.e_mail like 'DECLINED@%'

)

select count (*)
--S.e_mail, R.e_mail, M.ruleset_id, M.s_contact_id_cl, M.s_rcis_id_cl, M.s_fa_id_cl, M.r_contact_id_cl, M.r_rcis_id_cl, M.r_fa_id_cl
from matchedview M 
inner join shawFillerEmails S 
on M.s_rcis_id_cl=S.rcis_id and M.s_fa_id_cl=S.fa_id 
--and M.s_contact_id_cl=S.contact_id
inner join rogersFillerEmails R
on M.s_rcis_id_cl=R.x_rcis_id and M.s_fa_id_cl=R.fa_id 
--and M.s_contact_id_cl=R.contact_id
where 
ruleset_id in (1,2,3,4,5,6,7,11)  



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TestCase TC-A-07

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Match on all 3 phone numbers

-- COMMAND ----------

select 
S.Primary_phone as s_pphone, S.alternate_phone_1 as s_phone1, S.alternate_phone_2 as s_phone2,
R.Primary_phone as r_pphone, R.alternate_phone_1 as r_phone1, R.alternate_phone_2 as r_phone2,
M.ruleset_ID, M.r_rcis_id_cl, M.r_fa_id_cl, M.r_contact_id_cl, M.s_rcis_id_cl, M.s_fa_id_cl, M.s_contact_id_cl
from matchedView M
inner join r_shaw_contact S on M.s_rcis_id_cl=S.rcis_id and M.s_fa_id_cl=S.fa_id and M.s_contact_id_cl=S.contact_id
inner join r_rogers_contact R on M.r_rcis_id_cl=R.x_rcis_id and M.r_fa_id_cl=R.fa_id and M.r_contact_id_cl=R.contact_id
where 
M.ruleset_ID=12 

and
(S.Primary_phone <> R.Primary_phone 
or (S.Primary_phone is null and R.Primary_phone is not null) or (S.Primary_phone is not null and R.Primary_phone is null)
) and
(S.alternate_phone_1 <> R.alternate_phone_1 
or (S.alternate_phone_1 is null and R.alternate_phone_1 is not null) or (S.alternate_phone_1 is not null and R.alternate_phone_1 is null)
) and
(S.Primary_phone <> R.Primary_phone 
or (S.alternate_phone_2 is null and R.alternate_phone_2 is not null) or (S.alternate_phone_2 is not null and R.alternate_phone_2 is null)
)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TestCase TC-A-08

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Filler phone numbers

-- COMMAND ----------

 With 
 
 distinctMatch as 
(
  select distinct s_contact_id_cl, s_rcis_id_cl, s_fa_id_cl, s_first_name_cl, s_last_name_cl, r_contact_id_cl, r_rcis_id_cl, r_fa_id_cl, r_first_name_cl, r_last_name_cl
  from matchedView
  where ruleset_id in (1,2,3,4,8,9,10,12)
)

-- list of phones that are no fillers or nulls
,shawCleanedPhones as
( select 
    Case 
      when S.primary_phone like '%0000000%' then ''
      when primary_phone like '%0000808%' then ''
      when primary_phone like '%1111111%' then ''
      when primary_phone like '%1111234%' then ''
      when primary_phone like '%1112222%' then ''
      when primary_phone like '%1231234%' then ''
      when primary_phone like '%1234567%' then ''
      when primary_phone like '%2222222%' then ''
      when primary_phone like '%3333333%' then ''
      when primary_phone like '%4111111%' then ''
      when primary_phone like '%4444444%' then ''
      when primary_phone like '%5551111%' then ''
      when primary_phone like '%5551212%' then ''
      when primary_phone like '%5551234%' then ''
      when primary_phone like '%5555555%' then ''
      when primary_phone like '%6111111%' then ''
      when primary_phone like '%6666666%' then ''
      when primary_phone like '%7777777%' then ''
      when primary_phone like '%8888888%' then ''
      when primary_phone like '%9999999%' then ''
      Else primary_phone
    End primary_phone_cl
  ,
  
  Case 
      when S.alternate_phone_1 like '%0000000%' then ''
      when alternate_phone_1 like '%0000808%' then ''
      when alternate_phone_1 like '%1111111%' then ''
      when alternate_phone_1 like '%1111234%' then ''
      when alternate_phone_1 like '%1112222%' then ''
      when alternate_phone_1 like '%1231234%' then ''
      when alternate_phone_1 like '%1234567%' then ''
      when alternate_phone_1 like '%2222222%' then ''
      when alternate_phone_1 like '%3333333%' then ''
      when alternate_phone_1 like '%4111111%' then ''
      when alternate_phone_1 like '%4444444%' then ''
      when alternate_phone_1 like '%5551111%' then ''
      when alternate_phone_1 like '%5551212%' then ''
      when alternate_phone_1 like '%5551234%' then ''
      when alternate_phone_1 like '%5555555%' then ''
      when alternate_phone_1 like '%6111111%' then ''
      when alternate_phone_1 like '%6666666%' then ''
      when alternate_phone_1 like '%7777777%' then ''
      when alternate_phone_1 like '%8888888%' then ''
      when alternate_phone_1 like '%9999999%' then ''
      Else alternate_phone_1
    End alternate_phone1_cl
  ,
  
  Case 
      when S.alternate_phone_2 like '%0000000%' then ''
      when alternate_phone_2 like '%0000808%' then ''
      when alternate_phone_2 like '%1111111%' then ''
      when alternate_phone_2 like '%1111234%' then ''
      when alternate_phone_2 like '%1112222%' then ''
      when alternate_phone_2 like '%1231234%' then ''
      when alternate_phone_2 like '%1234567%' then ''
      when alternate_phone_2 like '%2222222%' then ''
      when alternate_phone_2 like '%3333333%' then ''
      when alternate_phone_2 like '%4111111%' then ''
      when alternate_phone_2 like '%4444444%' then ''
      when alternate_phone_2 like '%5551111%' then ''
      when alternate_phone_2 like '%5551212%' then ''
      when alternate_phone_2 like '%5551234%' then ''
      when alternate_phone_2 like '%5555555%' then ''
      when alternate_phone_2 like '%6111111%' then ''
      when alternate_phone_2 like '%6666666%' then ''
      when alternate_phone_2 like '%7777777%' then ''
      when alternate_phone_2 like '%8888888%' then ''
      when alternate_phone_2 like '%9999999%' then ''
      Else alternate_phone_2
    End alternate_phone2_cl
    ,
    * 
    from r_shaw_contact S 
)
    
-- list of phones that are no fillers or nulls
,rogersCleanedPhones as
( select 
    Case 
      when primary_phone like '%0000000%' then ''
      when primary_phone like '%0000808%' then ''
      when primary_phone like '%1111111%' then ''
      when primary_phone like '%1111234%' then ''
      when primary_phone like '%1112222%' then ''
      when primary_phone like '%1231234%' then ''
      when primary_phone like '%1234567%' then ''
      when primary_phone like '%2222222%' then ''
      when primary_phone like '%3333333%' then ''
      when primary_phone like '%4111111%' then ''
      when primary_phone like '%4444444%' then ''
      when primary_phone like '%5551111%' then ''
      when primary_phone like '%5551212%' then ''
      when primary_phone like '%5551234%' then ''
      when primary_phone like '%5555555%' then ''
      when primary_phone like '%6111111%' then ''
      when primary_phone like '%6666666%' then ''
      when primary_phone like '%7777777%' then ''
      when primary_phone like '%8888888%' then ''
      when primary_phone like '%9999999%' then ''
      Else primary_phone
    End primary_phone_cl
  ,
  
  Case 
      when alternate_phone_1 like '%0000000%' then ''
      when alternate_phone_1 like '%0000808%' then ''
      when alternate_phone_1 like '%1111111%' then ''
      when alternate_phone_1 like '%1111234%' then ''
      when alternate_phone_1 like '%1112222%' then ''
      when alternate_phone_1 like '%1231234%' then ''
      when alternate_phone_1 like '%1234567%' then ''
      when alternate_phone_1 like '%2222222%' then ''
      when alternate_phone_1 like '%3333333%' then ''
      when alternate_phone_1 like '%4111111%' then ''
      when alternate_phone_1 like '%4444444%' then ''
      when alternate_phone_1 like '%5551111%' then ''
      when alternate_phone_1 like '%5551212%' then ''
      when alternate_phone_1 like '%5551234%' then ''
      when alternate_phone_1 like '%5555555%' then ''
      when alternate_phone_1 like '%6111111%' then ''
      when alternate_phone_1 like '%6666666%' then ''
      when alternate_phone_1 like '%7777777%' then ''
      when alternate_phone_1 like '%8888888%' then ''
      when alternate_phone_1 like '%9999999%' then ''
      Else alternate_phone_1
    End alternate_phone1_cl
  ,
  
  Case 
      when alternate_phone_2 like '%0000000%' then ''
      when alternate_phone_2 like '%0000808%' then ''
      when alternate_phone_2 like '%1111111%' then ''
      when alternate_phone_2 like '%1111234%' then ''
      when alternate_phone_2 like '%1112222%' then ''
      when alternate_phone_2 like '%1231234%' then ''
      when alternate_phone_2 like '%1234567%' then ''
      when alternate_phone_2 like '%2222222%' then ''
      when alternate_phone_2 like '%3333333%' then ''
      when alternate_phone_2 like '%4111111%' then ''
      when alternate_phone_2 like '%4444444%' then ''
      when alternate_phone_2 like '%5551111%' then ''
      when alternate_phone_2 like '%5551212%' then ''
      when alternate_phone_2 like '%5551234%' then ''
      when alternate_phone_2 like '%5555555%' then ''
      when alternate_phone_2 like '%6111111%' then ''
      when alternate_phone_2 like '%6666666%' then ''
      when alternate_phone_2 like '%7777777%' then ''
      when alternate_phone_2 like '%8888888%' then ''
      when alternate_phone_2 like '%9999999%' then ''
      Else alternate_phone_2
    End alternate_phone2_cl
    ,
    * 
    from r_rogers_contact  
)
    
-- with filler numbers relaced with "" for both providers, create concatenation of all 3 numbers for both providers
-- if any match pertaining to rulesets with phone numbers are found that either for Rogers or shaw the concatenated form of 3 numbers is small in size (meaning only contaning seperator spaces), the test case fails
select  M.s_contact_id_cl, M.s_rcis_id_cl, M.s_fa_id_cl, 
concat_ws(' ', R.primary_phone_cl, R.alternate_phone1_cl, R.alternate_phone2_cl) as r_allphones,
concat_ws(' ', S.primary_phone_cl, S.alternate_phone1_cl, S.alternate_phone2_cl) as s_allphones
from distinctMatch M
inner join rogersCleanedPhones R
on M.r_rcis_id_cl=R.x_rcis_id and M.r_fa_id_cl=R.fa_id and M.r_contact_id_cl=R.contact_id
inner join shawCleanedPhones S 
on M.s_rcis_id_cl=S.rcis_id and M.s_fa_id_cl=S.fa_id and M.s_contact_id_cl=S.contact_id
where
length(concat_ws(' ', S.primary_phone_cl, S.alternate_phone1_cl, S.alternate_phone2_cl))<3
or 
length(concat_ws(' ', R.primary_phone_cl, R.alternate_phone1_cl, R.alternate_phone2_cl))<3



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Test Case TC_A-09 Customer Level Matching

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### validating QA results against matchedOutput

-- COMMAND ----------

with 

distinctMatch as
(
  select distinct
  s_rcis_id_cl, s_fa_id_cl, s_dq_full_name_cl, r_rcis_id_cl, r_fa_id_cl, s_dq_full_name_cl
  from matchedView
  where ruleset_id<16
)

,matchingRules as
(
select 

  concat_ws(" ", R.dq_first_name_cl, R.dq_last_name_cl) as r_name_cl, concat_ws(" ", S.dq_first_name_cl, S.dq_last_name_cl) as s_name_cl
  ,concat_ws(" ", R.first_name, R.last_name) as r_name, concat_ws(" ", S.first_name, S.last_name) as s_name

  , case
    when  R.dq_e_mail_cl = S.dq_e_mail_cl then 'true'
    else 'false'
    end match_email
  , R.e_mail as r_email, S.e_mail as s_email
  
  , case
 
    when  R.dq_Primary_phone_cl in (S.dq_Primary_phone_cl, S.dq_alternate_phone_1_cl, S.dq_alternate_phone_2_cl)  then 'true'
    when  R.dq_alternate_phone_1_cl in (S.dq_Primary_phone_cl, S.dq_alternate_phone_1_cl, S.dq_alternate_phone_2_cl)  then 'true'
    when  R.dq_alternate_phone_2_cl in (S.dq_Primary_phone_cl, S.dq_alternate_phone_1_cl, S.dq_alternate_phone_2_cl)  then 'true'
    else 'false'
    end match_phones
  , concat_ws(" ", R.dq_primary_phone, R.dq_alternate_phone_1, R.dq_alternate_phone_2) as r_phones
  , concat_ws(" ", S.dq_primary_phone, S.dq_alternate_phone_1, S.dq_alternate_phone_2) as s_phones
 
    , case
  
    when  R.mailing_address_no_zipcode_dq_cl = S.mailing_address_no_zipcode_dq_cl  then 'true'
    when  R.mailing_address_no_zipcode_dq_cl like S.mailing_address_no_zipcode_dq_cl  then 'true'
    else 'false'
    end match_mail_address

    , case
 
    when  RW.service_address_no_zipcode_dq_cl = SW.service_address_no_zipcode_dq_cl  then 'true'
    else 'false'
    end match_service_address
    
    , case
    when  RW.service_address_no_zipcode_dq_cl = S.mailing_address_no_zipcode_dq_cl  then 'true'
    when  RW.service_address_no_zipcode_dq_cl = S.mailing_address_no_zipcode_dq_cl  then 'true'
    when  R.mailing_address_no_zipcode_dq_cl = SW.service_address_no_zipcode_dq_cl  then 'true'
    else 'false'
    end match_mailingservice_address
    
    ,R.address as r_mail_address, S.address as s_mail_address
    ,RW.service_address as r_service_address, SW.service_address as s_service_address
    
 
    , R.objid as r_objid, R.rcis_id_cl as r_rcis_id, R.fa_id_cl as r_fa_id
    , R.objid as s_objid, S.rcis_id_cl as s_rcis_id, S.fa_id_cl as s_fa_id
  
  from cl_rogers_contact R   
  inner join cl_shaw_contact S
  on concat_ws(" ", R.first_name_cl, R.last_name_cl) = concat_ws(" ", S.first_name_cl, S.last_name_cl)
  
  -- find if record did not match to Shaw
  
  left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.rcis_id
  left join cl_shaw_wireline SW on S.contact_id = SW.contact_id and S.rcis_id_cl = SW.rcis_id
)

select 
case

    when  QA.r_objid is not null and QA.s_objid is not null and M.r_rcis_id_cl is not null and M.s_rcis_id_cl is not null then 'Both'
    when  QA.r_objid is null or QA.s_objid is null then 'Matched output only'
    when  M.r_rcis_id_cl is null and M.s_rcis_id_cl is null and 
        (match_email = 'false' or match_phones = 'false' or match_mail_address = 'false' or match_service_address = 'false') then 'QA matching on name only'
    else 'QA Result only'
    end crossmatch   
    
, QA.r_name, QA.s_name, QA.r_rcis_id, QA.s_rcis_id, r_email, s_email, match_email, r_phones, s_phones, match_phones
, r_mail_address, r_service_address, s_mail_address, s_service_address, match_mail_address, match_service_address, match_mailingservice_address
, M.r_rcis_id_cl, M.r_fa_id_cl, M.s_rcis_id_cl, M.s_fa_id_cl
from matchingRules QA
full outer join distinctMatch M
on QA.r_rcis_id = M.r_rcis_id_cl and QA.r_fa_id = M.r_fa_id_cl and QA.s_rcis_id = M.s_rcis_id_cl and QA.s_fa_id = M.s_fa_id_cl
-- where (match_email = 'true' or match_phones = 'true' 
-- or match_mail_address = 'true' or match_service_address = 'true')

--   -- only get 'Matched Result only'
--   and (QA.r_rcis_id is null or QA.s_rcis_id is null)
  
--   -- only get 'QA Result only'
--   and (M.r_rcis_id_cl is null or M.s_rcis_id_cl is null)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### validating ruleset_ID

-- COMMAND ----------

with 

distinctMatch as
(
  select distinct
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
  s_rcis_id_cl, s_fa_id_cl, s_first_name_cl, s_last_name_cl, r_rcis_id_cl, r_fa_id_cl, r_first_name_cl, r_last_name_cl
  from matchedView
  where ruleset_id<16
)

,matchingRules as
(
select 
  -- needs to be updated with dq names
  concat_ws(" ", R.first_name_cl, R.last_name_cl) as r_name_cl, concat_ws(" ", S.first_name_cl, S.last_name_cl) as s_name_cl
  ,concat_ws(" ", R.first_name, R.last_name) as r_name, concat_ws(" ", S.first_name, S.last_name) as s_name

  
  , case
    when  R.e_mail_cl = S.e_mail_cl then 'true'
    else 'false'
    end match_email
  , R.e_mail as r_email, S.e_mail as s_email
  
  , case

    when  R.Primary_phone_cl in (S.Primary_phone_cl, S.alternate_phone_1_cl, S.alternate_phone_2_cl)  then 'true'
    when  R.alternate_phone_1_cl in (S.Primary_phone_cl, S.alternate_phone_1_cl, S.alternate_phone_2_cl)  then 'true'
    when  R.alternate_phone_2_cl in (S.Primary_phone_cl, S.alternate_phone_1_cl, S.alternate_phone_2_cl)  then 'true'
    else 'false'
    end match_phones
  , concat_ws(" ", R.primary_phone, R.alternate_phone_1, R.alternate_phone_2) as r_phones
  , concat_ws(" ", S.primary_phone, S.alternate_phone_1, S.alternate_phone_2) as s_phones
 
    , case

    when  R.mailing_address_no_zipcode_dq_cl = S.mailing_address_no_zipcode_dq_cl  then 'true'
    when  R.mailing_address_no_zipcode_dq_cl like S.mailing_address_no_dq_cl  then 'true'
    else 'false'
    end match_mail_address

    , case

    when  RW.service_address_no_zipcode_dq_cl = SW.service_address_no_zipcode_dq_cl  then 'true'
    else 'false'
    end match_service_address
    
    , case
    when  RW.service_address_no_zipcode_dq_cl = S.mailing_address_no_zipcode_dq_cl  then 'true'
    when  RW.service_address_no_zipcode_dq_cl = S.mailing_address_no_dq_cl  then 'true'
    when  R.mailing_address_no_zipcode_dq_cl = SW.service_address_no_zipcode_dq_cl  then 'true'
    else 'false'
    end match_mailingservice_address
    
    ,R.address as r_mail_address, S.address as s_mail_address
    ,RW.service_address as r_service_address, SW.service_address as s_service_address
    
 
    , R.objid as r_objid, R.rcis_id_cl as r_rcis_id, R.fa_id_cl as r_fa_id
    , S.objid as s_objid, S.rcis_id_cl as s_rcis_id, S.fa_id_cl as s_fa_id
  
  from cl_rogers_contact R   
  inner join cl_shaw_contact S
  on concat_ws(" ", R.first_name_cl, R.last_name_cl) = concat_ws(" ", S.first_name_cl, S.last_name_cl)
  
  -- find if record did not match to Shaw
  
  left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.rcis_id
  left join cl_shaw_wireline SW on S.contact_id = SW.contact_id and S.rcis_id_cl = SW.rcis_id
)

select   
    
 QA.r_name, QA.s_name, match_email, match_phones, match_mail_address, match_service_address, match_mailingservice_address
, M.rule01, M.rule02, M.rule03, M.rule04, M.rule05, M.rule06, M.rule07, M.rule08, M.rule09, M.rule10, M.rule11, M.rule12, M.rule13, M.rule14, M.rule15 
, QA.r_objid, QA.s_objid, M.r_rcis_id_cl, M.r_fa_id_cl, M.s_rcis_id_cl, M.s_fa_id_cl
from matchingRules QA
inner join distinctMatch M
on QA.r_rcis_id = M.r_rcis_id_cl and QA.r_fa_id = M.r_fa_id_cl and QA.s_rcis_id = M.s_rcis_id_cl and QA.s_fa_id = M.s_fa_id_cl
where (match_email = 'true' or match_phones = 'true' 
or match_mail_address = 'true' or match_service_address = 'true')


  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### distinct combinations of rulesets in Matched

-- COMMAND ----------

with 

matchedResults as
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
  
  ,concat_ws(" ", R.first_name_cl, R.last_name_cl) as r_name_cl, concat_ws(" ", S.first_name_cl, S.last_name_cl) as s_name_cl
  ,concat_ws(" ", R.first_name, R.last_name) as r_name, concat_ws(" ", S.first_name, S.last_name) as s_name

  
  , case
    when  R.e_mail_cl = S.e_mail_cl then 'true'
    else 'false'
    end match_email
  , R.e_mail as r_email, S.e_mail as s_email
  
  , case

    when  R.Primary_phone_cl in (S.Primary_phone_cl, S.alternate_phone_1_cl, S.alternate_phone_2_cl)  then 'true'
    when  R.alternate_phone_1_cl in (S.Primary_phone_cl, S.alternate_phone_1_cl, S.alternate_phone_2_cl)  then 'true'
    when  R.alternate_phone_2_cl in (S.Primary_phone_cl, S.alternate_phone_1_cl, S.alternate_phone_2_cl)  then 'true'
    else 'false'
    end match_phones
  , concat_ws(" ", R.primary_phone, R.alternate_phone_1, R.alternate_phone_2) as r_phones
  , concat_ws(" ", S.primary_phone, S.alternate_phone_1, S.alternate_phone_2) as s_phones
 
    , case

    when  R.mailing_address_no_zipcode_dq_cl = S.mailing_address_no_zipcode_dq_cl  then 'true'
    when  R.mailing_address_no_zipcode_dq_cl like S.mailing_address_no_dq_cl  then 'true'
    else 'false'
    end match_mail_address

    , case

    when  RW.service_address_no_zipcode_dq_cl = SW.service_address_no_zipcode_dq_cl  then 'true'
    else 'false'
    end match_service_address
    
    , case
    when  R.mailing_address_no_zipcode_dq_cl = S.mailing_address_no_zipcode_dq_cl  then 'true'
    when  R.mailing_address_no_zipcode_dq_cl like S.mailing_address_no_dq_cl  then 'true'
    
    when  RW.service_address_no_zipcode_dq_cl = SW.service_address_no_zipcode_dq_cl  then 'true'

    when  RW.service_address_no_zipcode_dq_cl = S.mailing_address_no_zipcode_dq_cl  then 'true'
    when  RW.service_address_no_zipcode_dq_cl = S.mailing_address_no_dq_cl  then 'true'
    when  R.mailing_address_no_zipcode_dq_cl = SW.service_address_no_zipcode_dq_cl  then 'true'
    else 'false'
    end match_mailingservice_address
    ,RW.service_address_no_zipcode_dq_cl as r_service_address_no_zipcode_dq_cl
    ,SW.service_address_no_zipcode_dq_cl as s_service_address_no_zipcode_dq_cl
    ,R.address as r_mail_address, S.address as s_mail_address
    ,RW.service_address as r_service_address, SW.service_address as s_service_address
    
 
    , R.objid as r_objid, R.rcis_id_cl as r_rcis_id, R.fa_id_cl as r_fa_id
    , S.objid as s_objid, S.rcis_id_cl as s_rcis_id, S.fa_id_cl as s_fa_id
  
  , s_contact_id_cl, s_rcis_id_cl, s_fa_id_cl, s_first_name_cl, s_last_name_cl
  , r_contact_id_cl, r_rcis_id_cl, r_fa_id_cl, r_first_name_cl, r_last_name_cl
  
  from matchedView M
  inner join cl_rogers_contact R on M.r_rcis_id_cl = R.rcis_id_cl and M.r_fa_id_cl = R.fa_id_cl
  inner join cl_shaw_contact S on M.s_rcis_id_cl = S.rcis_id_cl and M.s_fa_id_cl = S.fa_id_cl
  left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.rcis_id 
    and M.r_service_address_no_zipcode_dq_cl = RW.service_address_no_zipcode_dq_cl
  left join cl_shaw_wireline SW on S.contact_id_cl = SW.contact_id and S.rcis_id_cl = SW.rcis_id
    and M.s_service_address_no_zipcode_dq_cl = SW.service_address_no_zipcode_dq_cl
  where ruleset_id<16
)  
  
select 

 match_email, match_phones, match_mail_address, match_service_address, match_mailingservice_address
, sum (M.rule01) as R01, sum (M.rule02) as R02, sum (M.rule03) as R03, sum (M.rule04) as R04, sum (M.rule05) as R05
, sum (M.rule06) as R06, sum (M.rule07) as R07, sum (M.rule08) as R08, sum (M.rule09) as R09, sum (M.rule10) as R10
, sum (M.rule11) as R11, sum (M.rule12) as R12, sum (M.rule13) as R13, sum (M.rule14) as R14, sum (M.rule15) as R15
from matchedResults M

where (match_email = 'true' or match_phones = 'true' or match_mail_address = 'true' or match_service_address = 'true' or match_mailingservice_address='true')
group by  match_email, match_phones, match_mail_address, match_service_address, match_mailingservice_address
order by R01, R02, R03, R04, R05, R06, R07, R08, R09, R10, R11, R12, R13, R14, R15 desc
    
    


  

-- COMMAND ----------

select * from cl_rogers_wireline M limit 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Test Case TC_A-10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Validate unmatch

-- COMMAND ----------

with 
-- Further clean inout names
cl_shaw as 
(
  select 
    Case   
      when first_name_cl is null then ''
      when first_name_cl ='null' then ''
      when first_name_cl='' then ''
      when first_name_cl = 'blank' then ''
      when first_name_cl = 'unknown' then ''
      when first_name_cl = 'none' then ''
      when first_name_cl = 'empty' then ''
      Else first_name_cl
    End first_name_cl2
  ,
    Case       
      when last_name_cl is null then ''
      when last_name_cl ='null' then ''
      when last_name_cl='' then ''
      when last_name_cl = 'blank' then ''
      when last_name_cl = 'unknown' then ''
      when last_name_cl = 'none' then ''
      when last_name_cl = 'empty' then ''
      Else last_name_cl 
    End last_name_cl2
  , 
  --concat_ws(" ", first_name_cl2, last_name_cl2) as full_name_cl,
  *
  from cl_shaw_contact 
),
  
cl_rogers as 
(
select 
    Case   
      when first_name_cl is null then ''
      when first_name_cl ='null' then ''
      when first_name_cl='' then ''
      when first_name_cl = 'blank' then ''
      when first_name_cl = 'unknown' then ''
      when first_name_cl = 'none' then ''
      when first_name_cl = 'empty' then ''
      Else first_name_cl
    End first_name_cl2
  ,
    Case          
      when last_name_cl is null then ''
      when last_name_cl ='null' then ''
      when last_name_cl='' then ''
      when last_name_cl = 'blank' then ''
      when last_name_cl = 'unknown' then ''
      when last_name_cl = 'none' then ''
      when last_name_cl = 'empty' then ''
      Else last_name_cl
    End last_name_cl2
  , 
  --concat_ws(" ", first_name_cl2, last_name_cl2) as full_name_cl,
  *
  from cl_rogers_contact 
)


-- select concat_ws(" ", R.first_name, R.last_name) as r_name, concat_ws(" ", S.first_name, S.last_name) as s_name
--  , R.x_rcis_id_cl as r_rcic_id_cl, S.rcis_id_cl as s_rcic_id_cl
--  , UN.rcis_id_cl, UN2.rcis_id_cl
--   from cl_rogers R   
--   inner join cl_shaw S
--   on concat_ws(" ", R.first_name_cl2, R.last_name_cl2) = concat_ws(" ", S.first_name_cl2, S.last_name_cl2)
--   -- find if record did not match to Shaw
--   left join unmatchedView UN
--   on UN.rcis_id_cl = R.x_rcis_id and UN.fa_id_cl = R.fa_id
--   left join unmatchedView UN2
--   on UN2.rcis_id_cl = S.rcis_id and UN2.fa_id_cl = S.fa_id
--   where UN.rcis_id_cl is null or UN2.rcis_id_cl is null
  
  -- start with Rogers contact and find unmatched with Shaw only based on first name and last name (join of Rogers contact, Shaw contact, and )
  select concat_ws(" ", R.first_name, R.last_name) as r_name, concat_ws(" ", S.first_name, S.last_name) as s_name
   , R.x_rcis_id_cl as r_rcic_id_cl, S.rcis_id_cl as s_rcic_id_cl
   ,'unmatch with Shaw' as source
   
   , R.e_mail_cl as r_email, S.e_mail_cl as s_email
   , concat_ws(" ", R.primary_phone_cl, R.alternate_phone_1_cl, R.alternate_phone_2_cl) as r_phones
   , concat_ws(" ", S.primary_phone_cl, S.alternate_phone_1_cl, S.alternate_phone_2_cl) as s_phones
  from cl_rogers R   
  inner join cl_shaw S
  on concat_ws(" ", R.first_name_cl2, R.last_name_cl2) = concat_ws(" ", S.first_name_cl2, S.last_name_cl2)
  inner join unmatchedView UN
  on UN.rcis_id_cl = R.x_rcis_id and UN.fa_id_cl = R.fa_id
 
 union
  
  select concat_ws(" ", R.first_name, R.last_name) as r_name, concat_ws(" ", S.first_name, S.last_name) as s_name
   , R.x_rcis_id_cl as r_rcic_id_cl, S.rcis_id_cl as s_rcic_id_cl
  ,'unmatch with Rogers' as source
  
   , R.e_mail_cl as r_email, S.e_mail_cl as s_email
   , concat_ws(" ", R.primary_phone_cl, R.alternate_phone_1_cl, R.alternate_phone_2_cl) as r_phones
   , concat_ws(" ", S.primary_phone_cl, S.alternate_phone_1_cl, S.alternate_phone_2_cl) as s_phones
  from cl_rogers R   
  inner join cl_shaw S
  on concat_ws(" ", R.first_name_cl2, R.last_name_cl2) = concat_ws(" ", S.first_name_cl2, S.last_name_cl2)
  inner join unmatchedView UN
  on UN.rcis_id_cl = S.rcis_id and UN.fa_id_cl = S.fa_id
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Test Case TC_A-11

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Validate match

-- COMMAND ----------

select 
concat_ws(' ', RC.first_name, RC.last_name) as r_name, RC.e_mail as r_email,
concat_ws(' ', RC.primary_phone, RC.alternate_phone_1, RC.alternate_phone_2) as r_phones,  
RC.address as r_address, RC.city as r_city, RC.state as r_state, RC.zipcode as r_zipcode, 
RC.x_street_name as r_street_name, RC.x_street_no as r_street_no,
RC.x_street_type as r_street_type, RC.x_apartment_no as r_apartment_no, RC.x_direction as r_direction, RC.x_quadrant as r_quadrant, RC.x_rural_type as r_rural_type, RC.x_rural_number as r_address, RC.address as r_rural_number,

RW.service_address as r_service_address,
RW.SERVICE_STREET_NAME as r_SERVICE_STREET_NAME, 
RW.SERVICE_STREET_NO as r_SERVICE_STREET_NO, 
RW.SERVICE_STREET_TYPE as r_SERVICE_STREET_TYPE, 
RW.SERVICE_ADDRESS_UNIT as r_SERVICE_ADDRESS_UNIT, 
RW.SERVICE_STREET_DIRECTION as r_SERVICE_STREET_DIRECTION, 
RW.SERVICE_QUADRANT as r_SERVICE_QUADRANT, 
RW.SERVICE_CITY as r_SERVICE_CITY, 
RW.SERVICE_PROVINCE as r_SERVICE_PROVINCE, 
RW.SERVICE_POSTAL_CODE as r_SERVICE_POSTAL_CODE, 
RW.SERVICE_ADDRESS_SUFFIX as r_SERVICE_ADDRESS_SUFFIX, 
RW.SERVICE_RURAL_TYPE as r_SERVICE_RURAL_TYPE, 
RW.SERVICE_RURAL_NUMBER as r_SERVICE_RURAL_NUMBER, 


concat_ws(' ', SC.first_name, SC.last_name) as s_name, SC.e_mail as s_email, 
concat_ws(' ', SC.primary_phone, SC.alternate_phone_1, SC.alternate_phone_2) as s_phones,  
SC.address as s_address, SC.city as s_city, SC.state as s_state, SC.zipcode as s_zipcode, 
SC.x_street_name as s_street_name, SC.x_street_no as s_street_no,
SC.x_street_type as s_street_type, SC.x_apartment_no as s_apartment_no, SC.x_direction as s_direction, SC.x_quadrant as s_quadrant, SC.x_rural_type as s_rural_type, SC.x_rural_number as s_address, SC.address as s_rural_number,

SW.service_address as s_service_address,
SW.SERVICE_STREET_NAME as s_SERVICE_STREET_NAME, 
SW.SERVICE_STREET_NO as s_SERVICE_STREET_NO, 
SW.SERVICE_STREET_TYPE as s_SERVICE_STREET_TYPE, 
SW.SERVICE_ADDRESS_UNIT as s_SERVICE_ADDRESS_UNIT, 
SW.SERVICE_STREET_DIRECTION as s_SERVICE_STREET_DIRECTION, 
SW.SERVICE_QUADRANT as s_SERVICE_QUADRANT, 
SW.SERVICE_CITY as s_SERVICE_CITY, 
SW.SERVICE_PROVINCE as s_SERVICE_PROVINCE, 
SW.SERVICE_POSTAL_CODE as s_SERVICE_POSTAL_CODE, 
SW.SERVICE_ADDRESS_SUFFIX as s_SERVICE_ADDRESS_SUFFIX, 
SW.SERVICE_RURAL_TYPE as s_SERVICE_RURAL_TYPE, 
SW.SERVICE_RURAL_NUMBER as s_SERVICE_RURAL_NUMBER, 

M.r_contact_id_cl, M.r_rcis_id_cl, M.r_fa_id_cl,
M.s_contact_id_cl, M.s_rcis_id_cl, M.s_fa_id_cl 
from matchedView M
inner join r_shaw_contact SC
on M.s_rcis_id_cl=SC.rcis_id and M.s_fa_id_cl=SC.fa_id and M.s_contact_id_cl=SC.contact_id

inner join r_rogers_contact RC on M.r_rcis_id_cl=RC.x_rcis_id and M.r_fa_id_cl=RC.fa_id and M.r_contact_id_cl=RC.contact_id
left join r_rogers_wireline RW on M.r_fa_id_cl = RW.fa_id and M.r_rcis_id_cl = RW.x_rcis_id
left join r_shaw_wireline SW on M.s_contact_id_cl = SW.contact_id and M.s_rcis_id_cl = SW.rcis_id

where ruleset_id=3 and 
(SW.service_address is not null or RW.service_address is not null)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-A-12

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Records that match based on address normalization

-- COMMAND ----------

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


-- COMMAND ----------

select address from r_rogers_contact where 
(address like '%unit%' 
or address like '%Rural%'
or address like '%house%'
or address like '%door%'
or address like '%lower%'
or address like '%upper%'
or address like '%basement%')

-- COMMAND ----------

select service_address from r_rogers_wireline where 
(service_address like '%unit%' 
or service_address like '%Rural%'
or service_address like '%house%'
or service_address like '%door%'
or service_address like '%lower%'
or service_address like '%upper%'
or service_address like '%basement%')

-- COMMAND ----------

-- explorimg behavior of c/o
select "rogers" as source, address, mailing_address_full_cl from cl_rogers_contact where lower(address) like "% c/o %" or lower(city) like "% c/o %"
union 
select "shaw" as source, address, mailing_address_full_cl from cl_rogers_contact where lower(address) like "% c/o %"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### instances where the normalized address is considerably shorter than the original form

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

-- COMMAND ----------

-- Shaw -- Find example records in the raw inout files with addresses that include GD, PO Box, RR, STN

  select 
  "shaw" as source
   ,count(concat_ws("-",S.contact_id, S.rcis_id_cl, S.fa_id_cl))

  from cl_shaw_contact S 
  left join cl_shaw_wireline SW on S.fa_id_cl = SW.fa_id and S.rcis_id_cl = SW.rcis_id
  where 
length(S.mailing_address_full_cl)*2<length(concat_ws(" ", S.address, S.city, S.state))
or 
length(SW.service_address_full_cl)*2<length(concat_ws(" ", SW.service_address, SW.service_city, SW.service_province))

union 

  select 
  "rogers" as source
  ,count(concat_ws("-",R.contact_id, R.rcis_id_cl, R.fa_id_cl))

  from cl_shaw_contact R 
  left join cl_shaw_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.rcis_id
  where 
length(R.mailing_address_full_cl)*2<length(R.address)
or 
length(RW.service_address_full_cl)*2<length(RW.service_address)

-- COMMAND ----------

-- MAGIC %md Code gone wrong
-- MAGIC ### Wrong

-- COMMAND ----------

select * from matchedView limit 3

-- COMMAND ----------

-- Find example records in the matched result set that match on normalized address, and do not otherwise match on original address
-- With the focus on address, only results of Rules #13, #14, #15 are picked to compare results of join btwn rogers & shaw on original address vs on normalized address
with matchedResults as
(
  select 
    ruleset_id
    , R.address as r_mail_address, concat_ws (" ", S.address, S.city, S.State, S.zipcode) as s_mail_address
    , RW.service_address as r_service_address, SW.service_address as s_service_address
    , case
  -- needs to be updated with dq ones
    when  R.address_cleansed = S.address_cleansed  then 'true'
    else 'false'
    end match_mail_address

    , case
  -- needs to be updated with dq ones
    when  RW.service_address_no_zipcode_dq_cl = SW.service_address_no_zipcode_dq_cl  then 'true'
    else 'false'
    end match_service_address
    
    , case
    when  R.address_cleansed = S.address_cleansed  then 'true'
    when  R.address_cleansed = SW.service_address_cleansed  then 'true'
    when  RW.service_address_cleansed = SW.service_address_cleansed  then 'true'
    when  RW.service_address_cleansed = S.address_cleansed  then 'true'

    else 'false'
    end match_mailingservice_address
    
  , r_rcis_id_cl, r_fa_id_cl, s_rcis_id_cl, s_fa_id_cl 
  --, s_first_name_cl, s_last_name_cl , r_first_name_cl, r_last_name_cl
  , M.r_mailing_address_no_zipcode_dq_cl  ,M.r_service_address_no_zipcode_dq_cl
  , M.s_mailing_address_no_zipcode_dq_cl, M.s_service_address_no_zipcode_dq_cl
  
  from matchedView M
  inner join cl_rogers_contact R on M.r_rcis_id_cl = R.rcis_id_cl and M.r_fa_id_cl = R.fa_id_cl
  inner join cl_shaw_contact S on M.s_rcis_id_cl = S.rcis_id_cl and M.s_fa_id_cl = S.fa_id_cl
  left join cl_rogers_wireline RW on M.r_fa_id_cl = RW.fa_id and M.r_rcis_id_cl = RW.rcis_id 
--      and M.s_service_address_no_zipcode_dq_cl = RW.service_address_no_zipcode_dq_cl 

  left join cl_shaw_wireline SW on M.r_fa_id_cl = RW.fa_id and M.s_rcis_id_cl = SW.rcis_id -- M.s_contact_id = SW.contact_id
--      and M.s_service_address_no_zipcode_dq_cl = SW.service_address_no_zipcode_dq_cl

  where ruleset_id in (13,14,15) 
)  

select 
--  r_name, s_name, r_phones, s_phones, r_email, s_email, 
 r_mail_address, r_service_address, s_mail_address, s_service_address
 --, match_mail_address, match_service_address, match_mailingservice_address, ruleset_id
 ,r_service_address_no_zipcode_dq_cl, r_mailing_address_no_zipcode_dq_cl
  ,s_service_address_no_zipcode_dq_cl, s_mailing_address_no_zipcode_dq_cl
  , s_rcis_id_cl, s_fa_id_cl, r_rcis_id_cl, r_fa_id_cl
--  , r_first_name_cl, r_last_name_cl, s_first_name_cl, s_last_name_cl,

from matchedResults M
where match_mail_address = 'false' and match_service_address = 'false' and match_mailingservice_address='false'




-- COMMAND ----------

select 
*
from matchedView M 
where ruleset_id=15 
--and M.s_rcis_id_cl=21192344 and M.s_fa_id_cl=20029934763 
and r_rcis_id_cl in (13734425, 41151740) and r_fa_id_cl=260019551703

-- s_contact_id=21192344 and r_contact_id=13734425
--r_rcis_id_cl 41151740 in matched results

-- COMMAND ----------

select 
 RW.service_address as r_service_address, M.*
from matchedView M 
left join cl_rogers_wireline RW on M.r_fa_id_cl = RW.fa_id and M.r_rcis_id_cl = RW.rcis_id 
--      and M.s_service_address_no_zipcode_dq_cl = RW.service_address_no_zipcode_dq_cl 
where ruleset_id=15 
--and M.s_rcis_id_cl=21192344 and M.s_fa_id_cl=20029934763 
and r_rcis_id_cl in (13734425, 41151740) and r_fa_id_cl=260019551703

-- COMMAND ----------

select 
 * 
from cl_rogers_contact RC
where  
--and M.s_rcis_id_cl=21192344 and M.s_fa_id_cl=20029934763 
rcis_id = 41151740 and fa_id=260019551703

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-A-13

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Alternate Province Codes/Names

-- COMMAND ----------

-- Find example records in the matched data where State is standardized

  select 
  
    R.state as r_state, R.state_cl as r_state_cl
    , S.state as s_state, S.state_cl as s_state_cl
    , RW.service_address as r_service_address, SW.service_address as s_service_address
    , RW.service_province as r_service_povince, SW.service_province as s_service_province
    , RW.service_province_cl as r_service_province_c, SW.service_province_cl as s_service_province_c

  , s_contact_id, s_rcis_id_cl, s_fa_id_cl
  , r_contact_id, r_rcis_id_cl, r_fa_id_cl

  
  from matchedView M
  inner join cl_rogers_contact R on M.r_rcis_id_cl = R.rcis_id_cl and M.r_fa_id_cl = R.fa_id_cl

  inner join cl_shaw_contact S on M.s_rcis_id_cl = S.rcis_id_cl and M.s_fa_id_cl = S.fa_id_cl

  left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.rcis_id
      and M.s_service_address_no_zipcode_dq_cl = RW.service_address_no_zipcode_dq_cl
      
  left join cl_shaw_wireline SW on S.contact_id = SW.contact_id and S.rcis_id_cl = SW.rcis_id
      and M.s_service_address_no_zipcode_dq_cl = SW.service_address_no_zipcode_dq_cl

  where 
  M.ruleset_id =15 
  and 
  (lower(R.state)!= R.state_cl or lower(S.state) != S.state_cl or lower(RW.service_province) != RW.service_province_cl or lower(SW.service_province) != SW.service_province_cl)





-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Rogers raw records vs cleaned records with province captured as PQ, NFL

-- COMMAND ----------

-- Rogers-- Find example records in the raw inout files with province captured as PQ, NFL

  select 
  
    R.state as r_state, R.state_cl as r_state_cl, R.province

    , RW.service_province as r_service_povince, RW.service_province_cl as r_service_province_cl 
    , R.address, R.mailing_address_no_zipcode_dq_cl
  --, s_contact_id_cl, s_rcis_id_cl, s_fa_id_cl
  , R.contact_id, R.rcis_id_cl, R.fa_id_cl

  
  from cl_rogers_contact R 
  left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id = RW.rcis_id

  where (R.state in ('PQ', 'NFL'))
--   (R.state_cl is not null or RW.service_province_cl is not null or R.state_cl!="" or RW.service_province_cl!="''" or R.state!="**")
--   and ( lower(R.state) != R.state_cl or lower(RW.service_province) != RW.service_province_cl)





-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Shaw raw records vs cleaned records with province captured as PQ, NFL

-- COMMAND ----------

-- Find example records in the raw inout files with province captured as PQ, NFL

  select 
  
    S.state as s_state, S.state_cl as s_state_cl, province

    , SW.service_province as s_service_povince, SW.service_province_cl as s_service_province_cl 
    , S.address, S.mailing_address_no_zipcode_dq_cl
    , S.contact_id, S.rcis_id_cl, S.fa_id_cl

  from cl_shaw_contact S
  left join cl_shaw_wireline SW on S.fa_id_cl = SW.fa_id and S.rcis_id = SW.rcis_id

  where (S.state in ('PQ', 'NFL', '**'))
--   (R.state_cl is not null or RW.service_province_cl is not null or R.state_cl!="" or RW.service_province_cl!="''" or R.state!="**")
--   and ( lower(R.state) != R.state_cl or lower(RW.service_province) != RW.service_province_cl)





-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-A-14

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Rogers Alternate street types

-- COMMAND ----------

-- Find example records in the raw inout files with lomg forms of street address type

  select 
  "Rogers" as source,
--     R.x_street_type as r_street_type, R.streettype,
    R.address, R.mailing_address_full_cl
    , RW.service_address, RW.service_address_no_zipcode_dq_cl
    , R.rcis_id_cl, R.fa_id_cl

  
  from cl_rogers_contact R 
  left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.x_rcis_id_cl = RW.x_rcis_id

  where 
--   lower(x_street_type) in ('street', 'drive', 'cresent', 'Avenue', 'Road') 
  --and rcis_id_cl = 408252105
--   or length(x_street_type)>4 
  --or length(service_street_type)>4
--   or lower(R.address) like '% street%'
--    lower(R.address) like '% avenue%'
--   or lower(R.address) like '% drive%'
--   or lower(R.address) like '% road%'
--  lower(R.address) like '% crescent%' or   
  lower(R.mailing_address_no_zipcode_dq) like '% road%' or  lower(R.service_address_no_zipcode_dq) like '% road%'
--   or lower(R.address) like '% avenue%'
--   or lower(R.address) like '% drive%'
--   or lower(R.address) like '% road%'


-- COMMAND ----------

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

-- COMMAND ----------

-- Find behavior against crescent / cresent as wrong spelling

  select 
  "Rogers" as source,
--     R.x_street_type as r_street_type, R.streettype,
    R.address, R.mailing_address_no_zipcode_dq_cl
    , R.mailing_address_no_zipcode_dq
    , R.rcis_id_cl, R.fa_id_cl

  
  from cl_rogers_contact R 
--   left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.x_rcis_id_cl = RW.x_rcis_id

  where 

   lower(R.address) like '% avenue%' or
   lower(R.address) like '% crescent%' or   
   lower(R.address) like '% cresent%'
--   or lower(R.address) like '% avenue%'
--   or lower(R.address) like '% drive%'
--   or lower(R.address) like '% road%'


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Edge cases of street name similare to street type

-- COMMAND ----------

-- Find example records in the raw inout files with Avenue as street name

  select 
  
--    R.x_street_type as r_street_type, R.x_street_type_cl as r_street_type_cl

    RW.service_street_type as r_street_type, RW.service_street_type_cl as r_street_type_cl 
    , R.address, R.mailing_address_no_dq_cl
    , RW.service_address as r_service_address
    , R.contact_id_cl, R.x_rcis_id_cl, R.fa_id_cl

  
  from cl_rogers_contact R 
  left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.x_rcis_id_cl = RW.x_rcis_id

  where 
  lower(address) like '% avenue road%'
  or lower(address) like '% avenue rd%'
  or lower(service_address) like '% avenue road%'
  or lower(address) like '% avenue rd%'
  


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-A-15

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Validating unit numbers

-- COMMAND ----------

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



-- COMMAND ----------

-- Shaw -- Find example records in the raw inout files with addresses that include GD, PO Box, RR, STN

  select 
  
   concat_ws(" ", S.address, S.city, S.state), S.mailing_address_no_zipcode_cl
  ,concat_ws(" ", SW.service_address, SW.service_city, SW.service_province), SW.service_address , SW.service_address_no_zipcode_dq_cl
   , S.contact_id, S.rcis_id_cl, S.fa_id_cl

  from cl_shaw_contact S 
  left join cl_shaw_wireline SW on S.fa_id_cl = SW.fa_id and S.rcis_id_cl = SW.rcis_id

  where 
--   lower(concat(address,service_address)) like '% unit%' 
   lower(concat(address,service_address)) like '% #%' or lower(concat(address,service_address)) like '% number%'
  or lower(concat(address,service_address)) like '% apartment%' 
  or lower(concat(address,service_address)) like '% apt%' 
  or lower(concat(address,service_address)) like '% suite%' 



-- COMMAND ----------

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
  lower(concat_ws(" ", R.address, S.address)) like '% apartment%' 
  or lower(concat_ws(" ", R.address, S.address)) like '% apt%' 
  or lower(concat_ws(" ", R.address, S.address)) like '% suite%' 
  or lower(concat_ws(" ", R.address, S.address)) like '% unit%' 
  )
  

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##TC-A-16 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Validate unmatched Address Level output 

-- COMMAND ----------

-- negative matches on Rule #17 (unknown distribution of false vs true negatives) based on comparing the matches with any customer level matches that did not include address
with distinctCustomerlevelMatchExclAddress as
(
select distinct s_rcis_id_cl, r_rcis_id_cl, s_fa_id_cl, r_fa_id_cl from matchedView 
where Ruleset_id in (4, 11, 12)
)

select 
--count (*)
concat_ws(" ", S.address, S.city, S.State, S.zipcode) as s_address, R.address as r_address
--, S.mailing_address_full_cl as s_fulladdress_cl, R.mailing_address_full_cl as r_fulladdress_cl
, CM.*
-- , CM.s_rcis_id_cl , CM.s_fa_id_cl , CM.r_rcis_id_cl , CM.r_fa_id_cl, 
from distinctCustomerlevelMatchExclAddress CM
left join matchedView AM  
on AM.s_rcis_id_cl=CM.s_rcis_id_cl and AM.s_fa_id_cl = CM.s_fa_id_cl and AM.r_rcis_id_cl=CM.r_rcis_id_cl and AM.r_fa_id_cl = CM.r_fa_id_cl
inner join cl_shaw_contact S on CM.s_rcis_id_cl=S.rcis_id_cl and CM.s_fa_id_cl = S.fa_id_cl
inner join cl_rogers_contact R on CM.r_rcis_id_cl=R.rcis_id_cl and CM.r_fa_id_cl = R.fa_id_cl
-- inner join cl_rogers_contact R on M.r_rcis_id_cl=R.rcis_id_cl and M.r_fa_id_cl = R.fa_id_cl
where 
AM.Ruleset_id >15
and AM.s_rcis_id_cl is null and AM.s_fa_id_cl is null and AM.r_rcis_id_cl is null and AM.r_fa_id_cl is null
--and S.mailing_address_full_cl != R.mailing_address_full_cl

-- COMMAND ----------

select count (distinct (concat_ws(" ", s_rcis_id_cl, r_rcis_id_cl, s_fa_id_cl, r_fa_id_cl))) from matchedView 
where Ruleset_id in (4, 11, 12)

-- COMMAND ----------

select count (distinct (concat_ws(" ", s_rcis_id_cl, r_rcis_id_cl, s_fa_id_cl, r_fa_id_cl))) from matchedView 
where Ruleset_id >15

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
r_rcis_id_cl
,r_fa_id_cl
, concat_ws ("-",s_rcis_id_cl, s_fa_id_cl) as uniqueID

 from matchedView where Ruleset_id=17
 )
 
 select M.r_rcis_id_cl, M.r_fa_id_cl, count(M.uniqueID) as countID
from distinctMatch M
group by r_rcis_id_cl, r_fa_id_cl
order by countID desc

-- COMMAND ----------

-- Specific to Ruleset 17 for matches for Rogers, Get idea of the number of matches that are recorded against how many roger's account receive that many matches
with rogersMatchCount as
(
select 
"Rogers" as source
, r_rcis_id_cl as rcis_id
, r_fa_id_cl as fa_id
, count (concat_ws ("-",s_rcis_id_cl, s_fa_id_cl)) as rateOfMatch
 from matchedView 
 where Ruleset_id=17
 group by r_rcis_id_cl, r_fa_id_cl
 )
 , shawMatchCount as
(
select  
"Shaw" as source
, s_rcis_id_cl as rcis_id
, s_fa_id_cl as fa_id
, count (concat_ws ("-",s_rcis_id_cl, s_fa_id_cl)) as rateOfMatch
 from matchedView 
 where Ruleset_id=17
 group by r_rcis_id_cl, r_fa_id_cl
 )
 
select source, rateOfMatch, count(concat_ws("-", M.r_rcis_id_cl, M.r_fa_id_cl)) as accountCount
from matchCount M
group by source, rateOfMatch
order by rateOfMatch desc

-- COMMAND ----------

with matchCount as
(
select  
r_rcis_id_cl
,r_fa_id_cl
, count (concat_ws ("-",s_rcis_id_cl, s_fa_id_cl)) as counts
 from matchedView 
 where Ruleset_id=17
 group by r_rcis_id_cl, r_fa_id_cl
 )
 select concat_ws(" ", S.address, S.city, S.State, S.zipcode) as s_address, R.address as r_address, S.mailing_address_full_cl as s_clean_address, R.mailing_address_full_cl as r_clean_address
, M.s_rcis_id_cl, M.s_fa_id_cl, M.r_rcis_id_cl, M.r_fa_id_cl, M.*
from matchCount C
inner join matchedView M on C.r_rcis_id_cl=M.r_rcis_id_cl and C.r_fa_id_cl = M.r_fa_id_cl
where M.Ruleset_id=17 and counts > 100

-- COMMAND ----------

with matchCount as
(
select  
r_rcis_id_cl
,r_fa_id_cl
, count (concat_ws ("-",s_rcis_id_cl, s_fa_id_cl)) as counts
-- ,s_dq_full_name_cl
-- ,r_dq_full_name_cl
-- ,s_mailing_address_no_zipcode_dq_cl
-- ,s_service_address_no_zipcode_dq_cl
-- ,r_mailing_address_no_zipcode_dq_cl
-- ,r_service_address_no_zipcode_dq_cl
 from matchedView 
 where Ruleset_id=17
 group by r_rcis_id_cl, r_fa_id_cl
 )
 
select counts
, R.dq_full_name_cl as r_fullname, S.dq_full_name_cl as S_fullname 
, R.address as r_address, concat_ws(" ", S.address, S.city, S.State, S.zipcode) as s_address
, R.mailing_address_no_zipcode_dq_cl as r_clean_address
, S.mailing_address_no_zipcode_dq_cl as s_clean_address
, M.s_rcis_id_cl, M.s_fa_id_cl, M.r_rcis_id_cl, M.r_fa_id_cl, M.*
from matchCount C
inner join matchedView M on C.r_rcis_id_cl=M.r_rcis_id_cl and C.r_fa_id_cl = M.r_fa_id_cl
inner join cl_shaw_contact S on M.s_rcis_id_cl=S.rcis_id_cl and M.s_fa_id_cl = S.fa_id_cl
inner join cl_rogers_contact R on M.r_rcis_id_cl=R.rcis_id_cl and M.r_fa_id_cl = R.fa_id_cl
where M.Ruleset_id=17 and counts > 10
and (S.mailing_address_full_cl != R.mailing_address_full_cl or S.PostalCode != R.PostalCode)

-- COMMAND ----------

select R.dq_full_name_cl as r_fullname, S.dq_full_name_cl as S_fullname 
, R.address as r_address, concat_ws(" ", S.address, S.city, S.State, S.zipcode) as s_address
-- , R.mailing_address_no_zipcode_dq_cl as r_clean_address
-- , S.mailing_address_no_zipcode_dq_cl as s_clean_address
, M.s_rcis_id_cl, M.s_fa_id_cl, M.r_rcis_id_cl, M.r_fa_id_cl
from matchedView M 
inner join cl_shaw_contact S on M.s_rcis_id_cl=S.rcis_id_cl and M.s_fa_id_cl = S.fa_id_cl
inner join cl_rogers_contact R on M.r_rcis_id_cl=R.rcis_id_cl and M.r_fa_id_cl = R.fa_id_cl
where M.Ruleset_id=17 and M.r_rcis_id_cl=438193866 and M.r_fa_id_cl=799153531

-- COMMAND ----------

-- Count of positive matches on Rule #17 (unknown distribution of false vs true positives) based on taking out direction and postal codes, where full adresses are different and otherwise the records do not match on any Customer Level rules
with distinctCustomerMatch as
(
select s_rcis_id_cl, r_rcis_id_cl, s_fa_id_cl, r_fa_id_cl from matchedView
where ruleset_id <16
)

select count (*)
from matchedView M 
left join distinctCustomerMatch C on  M.s_rcis_id_cl=C.s_rcis_id_cl and M.s_fa_id_cl = C.s_fa_id_cl and M.r_rcis_id_cl=C.r_rcis_id_cl and M.r_fa_id_cl = C.r_fa_id_cl
inner join cl_shaw_contact S on M.s_rcis_id_cl=S.rcis_id_cl and M.s_fa_id_cl = S.fa_id_cl
inner join cl_rogers_contact R on M.r_rcis_id_cl=R.rcis_id_cl and M.r_fa_id_cl = R.fa_id_cl
where M.Ruleset_id=17 
and S.mailing_address_full_cl != R.mailing_address_full_cl
and C.s_rcis_id_cl is null and C.r_rcis_id_cl is null and C.r_fa_id_cl is null and C.s_fa_id_cl is null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fasle positives on detection of wrong postal codes

-- COMMAND ----------

select 
 R.service_address as r_address, concat_ws(" ", S.service_address, S.service_city, S.service_province, S.service_postal_code) as s_address
, R.service_address_no_zipcode_dq_cl as r_service_address_no_zipcode_dq_cl
, S.service_address_no_zipcode_dq_cl as s_service_address_no_zipcode_dq_cl
, M.s_rcis_id_cl, M.s_fa_id_cl, M.r_rcis_id_cl, M.r_fa_id_cl, M.*
from matchedView M 
inner join cl_shaw_wireline S on M.s_rcis_id_cl=S.rcis_id_cl and M.s_fa_id_cl = S.fa_id_cl
inner join cl_rogers_wireline R on M.r_rcis_id_cl=R.rcis_id_cl and M.r_fa_id_cl = R.fa_id_cl 
where s_rcis_id_cl = 5798280 and s_fa_id_cl="20007432012" and ruleset_id=16

-- COMMAND ----------

select *
from cl_shaw_wireline where rcis_id_cl = 5798280 and fa_id_cl="20007432012"

-- COMMAND ----------

-- Find the number of accounts in which the parser does not detect the postal code properly

select "Rogers Mailing Address" as source, count (*)
from cl_rogers_contact where 
length(postalcode)>6

union 

select "Rogers Wireline Address" as source, count (*)
from cl_rogers_wireline where 
length(postalcode)>6

union 

select "Shaw Mailing Address" as source, count (*)
from cl_shaw_contact where 
length(postalcode)>6


union
select "Shaw Wireline Address" as source, count (*)
from cl_shaw_wireline where 
length(postalcode)>6

-- COMMAND ----------

-- Find the details of accounts in which the parser does not detect the postal code properly
select 
"Shaw Service Address" as source
, concat_ws(" ", service_address, service_city, service_province) as address
, service_address_no_zipcode_dq_cl as normalized_address
, PostalCode, rcis_id, fa_id, contact_id
from cl_shaw_wireline  where 
length(postalcode)>6

union

select 
"Shaw Contact Address" as source
, concat_ws(" ", address, city, province) as address
, mailing_address_no_zipcode_dq_cl as normalized_address
, PostalCode, rcis_id, fa_id, contact_id
from cl_shaw_contact  where 
length(postalcode)>6

union 

select 
"Rogers Contact Address" as source
, address
, mailing_address_no_zipcode_dq_cl as normalized_address
, PostalCode, rcis_id, fa_id, contact_id
from cl_rogers_contact  where 
length(postalcode)>6

union 

select
"Rogers Service Address" as source
, service_address as address
, service_address_no_zipcode_dq_cl as normalized_address
, PostalCode, rcis_id, fa_id, contact_id
from cl_rogers_wireline where 
length(postalcode)>6

-- COMMAND ----------

with 
TroubledPostalCodes as
(
-- Find the details of accounts in which the parser does not detect the postal code properly
select 
"Shaw Service Address" as source
, concat_ws(" ", service_address, service_city, service_province) as address
, service_address_no_zipcode_dq_cl as normalized_address
, PostalCode, rcis_id, fa_id, contact_id
from cl_shaw_wireline  where 
length(postalcode)>6

union

select 
"Shaw Contact Address" as source
, concat_ws(" ", address, city, province) as address
, mailing_address_no_zipcode_dq_cl as normalized_address
, PostalCode, rcis_id, fa_id, contact_id
from cl_shaw_contact  where 
length(postalcode)>6

union 

select 
"Rogers Contact Address" as source
, address
, mailing_address_no_zipcode_dq_cl as normalized_address
, PostalCode, rcis_id, fa_id, contact_id
from cl_rogers_contact  where 
length(postalcode)>6

union 

select
"Rogers Service Address" as source
, service_address as address
, service_address_no_zipcode_dq_cl as normalized_address
, PostalCode, rcis_id, fa_id, contact_id
from cl_rogers_wireline where 
length(postalcode)>6
)

, distinctMatch as
(
select distinct r_rcis_id_cl, s_rcis_id_cl, s_fa_id_cl, r_fa_id_cl 
from matchedView
where ruleset_id>15
)

select count(*)
from TroubledPostalCodes T
inner join distinctMatch M
on (T.rcis_id=M.r_rcis_id_cl and T.fa_id=r_fa_id_cl) or (T.rcis_id=M.s_rcis_id_cl and T.fa_id=s_fa_id_cl)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Match Rates

-- COMMAND ----------

select count (distinct r_rcis_id_cl, s_rcis_id_cl, s_fa_id_cl, r_fa_id_cl) from matchedView

-- COMMAND ----------

select count (distinct r_rcis_id_cl, s_rcis_id_cl, s_fa_id_cl, r_fa_id_cl) from matchedView
where ruleset_id>15

-- COMMAND ----------

select count (distinct r_rcis_id_cl, s_rcis_id_cl, s_fa_id_cl, r_fa_id_cl) from matchedView
where ruleset_id<=15

-- COMMAND ----------

select count (distinct r_rcis_id_cl, s_rcis_id_cl) from matchedView

-- COMMAND ----------

select count (distinct r_rcis_id_cl, s_rcis_id_cl) from matchedView
where ruleset_id>15

-- COMMAND ----------

select count (distinct r_rcis_id_cl, s_rcis_id_cl) from matchedView
where ruleset_id<=15

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Total accounts and customers

-- COMMAND ----------

-- select count (distinct rcis_id) from cl_rogers_contact
-- select count (distinct rcis_id, fa_id) from cl_rogers_contact
select count (distinct rcis_id) from cl_shaw_contact
-- select count (distinct rcis_id, fa_id) from cl_shaw_contact


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
  where R.StreetDirection_Cleansed not in ('e', 'w', 'n', 's', 'ne', 'nw', 'se', 'sw')



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
  lower(concat(address,service_address)) like '% rr %' or lower(concat(address,service_address)) like '% rural route%'
  or lower(concat(address,service_address)) like '% po box %'
  or lower(concat(address,service_address)) like '% stn %' or lower(address) like '% station%'
  or lower(concat(address,service_address)) like '% GD%' or lower(address) like '% general delivery %'


-- COMMAND ----------

-- Rogers -- Find example records in the raw inout files with addresses that include GD, PO Box, RR, STN

  select 
  
   R.address, R.mailing_address_no_zipcode_cl
   , RW.service_address , RW.service_address_no_zipcode_dq_cl
   , R.contact_id, R.rcis_id_cl, R.fa_id_cl

  from cl_rogers_contact R 
  left join cl_rogers_wireline RW on R.fa_id_cl = RW.fa_id and R.rcis_id_cl = RW.rcis_id

  where 
  lower(concat(R.address,RW.service_address)) like '% rr %' or lower(concat(R.address,RW.service_address)) like '% rural route%'
  or lower(concat(R.address,RW.service_address)) like '% po box %'
  or lower(concat(R.address,RW.service_address)) like '% stn %' or lower(R.address) like '% station%'
  or lower(concat(R.address,RW.service_address)) like '% GD%' or lower(R.address) like '% general delivery %'


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Adhoc exploring data

-- COMMAND ----------

select * from r_shaw_contact limit 3

-- COMMAND ----------

select distinct service_provider from r_shaw_wireless

-- COMMAND ----------

select distinct * from r_shaw_wireline

-- COMMAND ----------


