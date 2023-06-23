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
-- MAGIC s_wln_serviceability = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration4/Shaw/WirelineServiceability")
-- MAGIC r_wln_serviceability = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration4/Rogers/WirelineServiceability")
-- MAGIC r_wls_serviceability = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration4/Rogers/WirelessServiceability")
-- MAGIC
-- MAGIC serviceability_match = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration4/ServiceabilityMatchedAccounts")
-- MAGIC
-- MAGIC ruleset_df = spark.read.format("parquet").load("/mnt/development/referencedata/rulesets")

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
-- MAGIC s_wln_serviceability.createOrReplaceTempView("s_wirelineView")
-- MAGIC r_wln_serviceability.createOrReplaceTempView("r_wirelineView")
-- MAGIC r_wls_serviceability.createOrReplaceTempView("r_wirelessView")
-- MAGIC
-- MAGIC serviceability_match.createOrReplaceTempView("serviceView")
-- MAGIC
-- MAGIC ruleset_df.createOrReplaceTempView("rulesetView")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Sample data exploration

-- COMMAND ----------

select * from rulesetView


-- COMMAND ----------

select * from serviceView limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Query Results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###TC-B-02

-- COMMAND ----------

select Source, Account_Type, count(concat_ws("-", rcis_id, fa_id)) as count_distinct_accounts from serviceView 
group by Source, Account_Type

-- COMMAND ----------

select Source, Account_Type, serviceability_source, serviceability_type, count(*) as count_rows from serviceView 
group by Source, Account_Type, serviceability_source, serviceability_type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### check joins to reference tables

-- COMMAND ----------

-- validate the foregn key referential integrity to Shaw Wireline Serviceability

select Source, Account_Type, serviceability_source, serviceability_type, count(*)
from serviceView V
inner join s_wirelineView reference
on V.SERVICEABILITY_ID_MAILING_ADDRESS = reference.Address_ID
group by Source, Account_Type, serviceability_source, serviceability_type
--Numbers match than we filter on serviceability type and not use joins

-- COMMAND ----------

-- validate the foregn key referential integrity to Rogers Wireless Serviceability
select Source, Account_Type, serviceability_source, serviceability_type, count(*)
from serviceView V
inner join r_wirelessView reference
on V.SERVICEABILITY_ID_MAILING_ADDRESS = reference.postal_code_cl
group by Source, Account_Type, serviceability_source, serviceability_type
--w Exact numbers we get when fiter on serviceability_source="Rogers" and serviceability_type="Wireless"

-- COMMAND ----------

-- explore the mismatch
with referentialdata as
(
  select V.*
  from serviceView V
  inner join r_wirelineView reference
  on V.SERVICEABILITY_ID_MAILING_ADDRESS = reference.sam_key
)
, serviceableResults as
(
  select * 
  from serviceView 
  where serviceability_source = "Rogers" and serviceability_type = "Wireline"
)

select *
from referentialdata V
left outer join serviceableResults N
on V.rcis_id=N.rcis_id and V.fa_id=N.fa_id 
where N.rcis_id is null and N.fa_id is null 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Check on any null values for addresses

-- COMMAND ----------

select *
from serviceView V
where (MAILING_ADDRESS is  null and SERVICE_ADDRESS is  null)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Check number of accounts

-- COMMAND ----------

-- check duplicate rows

select "Rogers" as Source, "Wireless" as Account_Type, count(*) as count_rows, count (distinct (concat_ws("-",rcis_id, fa_id, contact_ID))) as count_unique_IDs
,count(*) - count (distinct (concat_ws("-",rcis_id, fa_id, contact_ID))) as difference
from cl_rogers_wireless


union 
select "Rogers" as Source, "Wireline" as Account_Type, count(*) as count_rows, count (distinct (concat_ws("-",rcis_id, fa_id, sam_key))) as count_unique_IDs
,count(*) - count (distinct (concat_ws("-",rcis_id, fa_id, sam_key))) as difference
from cl_rogers_wireline


union 
select "Shaw" as Source, "Wireless" as Account_Type, count(*) as count_rows, count (distinct (concat_ws("-",rcis_id, fa_id, contact_ID))) as count_unique_IDs
,count(*) - count (distinct (concat_ws("-",rcis_id, fa_id, contact_ID))) as difference
from cl_shaw_wireless


union 
select 
 "Shaw" as Source, "Wireline" as Account_Type, 
count(*) as count_rows, count (distinct (concat_ws("-",rcis_id, fa_id, contact_ID))) as count_unique_IDs
,count(*) - count (distinct (concat_ws("-",rcis_id, fa_id, contact_ID))) as difference
from cl_shaw_wireline


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Duplicate Addresses for Sam_key Rogers Wireline

-- COMMAND ----------


with uniqueIDs
as
(
select rcis_id, fa_id, sam_key, count (*) as countrows
from cl_rogers_wireline
group by rcis_id, fa_id, sam_key
)

select countrows, W.rcis_id, W.fa_id, W.sam_key, W.objid, W.service_address
from cl_rogers_wireline W
inner join uniqueIDs U
on W.rcis_id=U.rcis_id and W.fa_id=U.fa_id and W.sam_key=U.sam_key
where countrows>1
order by countrows desc, W.rcis_id, W.fa_id, W.sam_key

-- COMMAND ----------

select Source, Account_Type, count (distinct (concat_ws("-",rcis_id, fa_id)))
from serviceView V
group by Source, Account_Type
order by Source, Account_Type


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###TC-B-03

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Match to more than one instance in reference tables

-- COMMAND ----------

select 
 Source, Account_Type, serviceability_source, serviceability_type, 
rcis_id, fa_id, count(*) as count_rows 
from serviceView 
-- where Source="Shaw" and Account_Type="Wireline" and serviceability_source="Rogers" and serviceability_type="Wireless"
group by Source, Account_Type, serviceability_source, serviceability_type
, rcis_id, fa_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Rogers Wireless

-- COMMAND ----------

-- Rogers Wireless
-- find accounts that matched to more than 1 line in Serviceability
with numberofMacthes
as
(
select 
 Source, Account_Type, serviceability_source, serviceability_type, 
rcis_id, fa_id, count(*) as count_rows 
from serviceView 
where serviceability_source="Rogers" and serviceability_type="Wireless"
group by Source, Account_Type, serviceability_source, serviceability_type
, rcis_id, fa_id
)

select V.source, V.RCIS_ID, V.FA_ID, V.MAILING_ADDRESS
, r_mail.Postal_Code as MAILING_ADDRESS_MATCHED
, V.SERVICE_ADDRESS, r_service.Postal_Code as SERVICE_ADDRESS_MATCHED 
, V.Account_Type, V.serviceability_source, V.serviceability_type
from serviceView V
inner join numberofMacthes N
on V.rcis_id=N.rcis_id and V.fa_id=N.fa_id 

left join r_wirelessView r_mail
on V.SERVICEABILITY_ID_MAILING_ADDRESS = lower(r_mail.postalcode)
left join r_wirelessView r_service
on V.SERVICEABILITY_ID_SERVICE_ADDRESS = lower(r_service.postalcode)

where V.serviceability_source="Rogers" and V.serviceability_type="Wireless"
and N.count_rows>1
order by  V.Source, V.Account_Type, V.rcis_id, V.fa_id, V.serviceability_source, V.serviceability_type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Rogers Wireline

-- COMMAND ----------

with redundantAddresses
as
(
  select serviceability_address_full, count (sam_key) as redundant_count
  from r_wirelineView 
  group by serviceability_address_full
)
select sum(redundant_count) from redundantAddresses
where redundant_count>1

-- COMMAND ----------

-- Rogers Wireline
-- find accounts that matched to more than 1 line in Serviceability
with 
duplicateServiceable
as
(
  select serviceability_address_full, count (sam_key) 
  from r_wirelineView 
  group by serviceability_address_full
)

, numberofMacthes
as
(
select 
  Source, Account_Type, serviceability_source, serviceability_type, 
  rcis_id, fa_id, count(*) as count_rows 
  from serviceView 
  where serviceability_source="Rogers" and serviceability_type="Wireline"
  group by Source, Account_Type, serviceability_source, serviceability_type
  , rcis_id, fa_id
)

select V.source, V.RCIS_ID, V.FA_ID
, V.MAILING_ADDRESS, r_mail.serviceability_address_cleansed as MAILING_ADDRESS_MATCHED
, V.SERVICE_ADDRESS, r_service.serviceability_address_cleansed as SERVICE_ADDRESS_MATCHED 
, V.SERVICEABILITY_ID_MAILING_ADDRESS, V.SERVICEABILITY_ID_SERVICE_ADDRESS
, V.Account_Type, V.serviceability_source, V.serviceability_type
from serviceView V
inner join numberofMacthes N
on V.rcis_id=N.rcis_id and V.fa_id=N.fa_id 

left join r_wirelineView r_mail
on V.SERVICEABILITY_ID_MAILING_ADDRESS = r_mail.sam_key
left join r_wirelineView r_service
on V.SERVICEABILITY_ID_SERVICE_ADDRESS = r_service.sam_key

where V.serviceability_source="Rogers" and V.serviceability_type="Wireline"
and N.count_rows>1
order by  V.Source, V.Account_Type, V.rcis_id, V.fa_id, V.serviceability_source, V.serviceability_type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Shaw Wireline

-- COMMAND ----------

-- Shaw Wireline
-- find accounts that matched to more than 1 line in Serviceability
with numberofMacthes
as
(
select 
 Source, Account_Type, serviceability_source, serviceability_type, 
rcis_id, fa_id, sam_key, count(*) as count_rows 
from serviceView 
where serviceability_source="Shaw" and serviceability_type="Wireline"
group by Source, Account_Type, serviceability_source, serviceability_type
, rcis_id, fa_id, sam_key
)

select V.source, V.RCIS_ID, V.FA_ID, V.sam_key
, V.MAILING_ADDRESS, r_mail.address_cleansed as MAILING_ADDRESS_MATCHED
, V.SERVICE_ADDRESS, r_service.address_cleansed as SERVICE_ADDRESS_MATCHED 
, V.SERVICEABILITY_ID_MAILING_ADDRESS, V.SERVICEABILITY_ID_SERVICE_ADDRESS
, V.Account_Type, V.serviceability_source, V.serviceability_type
from serviceView V
inner join numberofMacthes N
on V.rcis_id=N.rcis_id and V.fa_id=N.fa_id and V.sam_key=N.sam_key 

left join s_wirelineView r_mail
on V.SERVICEABILITY_ID_MAILING_ADDRESS = r_mail.address_ID
left join s_wirelineView r_service
on V.SERVICEABILITY_ID_SERVICE_ADDRESS = r_service.address_ID

where V.serviceability_source="Shaw" and V.serviceability_type="Wireline" and V.Account_Type="Wireline"
and N.count_rows>1
order by  V.Source, V.Account_Type, V.rcis_id, V.fa_id, V.serviceability_source, V.serviceability_type

-- COMMAND ----------

select address_id, formatted_address, unit, unitNumber, serviceability_address_full_cl from s_wirelineView where address_ID in (5656161, 3250928, 3725649, 3015343)
--  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###TC-B-04

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Check accounts that didn't match

-- COMMAND ----------

-- find what addresses are not servicable by Rogers (The reason being that it is suspected that Rogers will have higher coverage on different addresses and hence it would be easier to spot any false negative matches)
with distinctWirelineAccounts
as
(
  select distinct RCIS_ID, FA_ID from cl_shaw_wireline 
)
, distinctWirelessAccounts
as
(
  select distinct RCIS_ID, FA_ID from cl_shaw_wireless 
)
, distinctServiceable
as
(
  select distinct RCIS_ID, FA_ID from serviceView
  where Source="Shaw"
)

select "Shaw Wireline" as Source, W.fa_id, W.RCIS_ID, C.mailing_address_full, W.service_address_cleansed
, C.city, C.Province
from distinctWirelineAccounts A
inner join cl_shaw_wireline W on A.rcis_id=W.rcis_id and A.fa_id=W.fa_id
inner join cl_shaw_contact C on A.rcis_id=W.rcis_id and C.contact_id=W.contact_id
left outer join distinctServiceable S
on A.rcis_id=S.rcis_id and A.fa_id=S.fa_id
 where S.rcis_id is null and S.fa_id is null

 
 union
 select "Shaw Wireless" as Source, A.fa_id, A.RCIS_ID, C.mailing_address_full, "" as service_address_cleansed
, C.city, C.Province
from distinctWirelessAccounts A
inner join cl_shaw_contact C on A.rcis_id=C.rcis_id and A.fa_id=C.fa_id
left outer join distinctServiceable S
on A.rcis_id=S.rcis_id and A.fa_id=S.fa_id
 where S.rcis_id is null and S.fa_id is null


-- COMMAND ----------

-- find what addresses are not servicable by Rogers (The reason being that it is suspected that Rogers will have higher coverage on different addresses and hence it would be easier to spot any false negative matches)
with distinctWirelineAccounts
as
(
  select distinct RCIS_ID, FA_ID from cl_shaw_wireline 
)
, distinctWirelessAccounts
as
(
  select distinct RCIS_ID, FA_ID from cl_shaw_wireless 
)
, distinctServiceable
as
(
  select distinct RCIS_ID, FA_ID from serviceView
  where Source="Shaw"
)

select distinct "Shaw Wireline" as Source, 
W.fa_id, W.RCIS_ID, C.mailing_address_full, W.service_address_cleansed
, C.city, C.Province

from distinctWirelineAccounts A
inner join cl_shaw_wireline W on A.rcis_id=W.rcis_id and A.fa_id=W.fa_id
inner join cl_shaw_contact C on A.rcis_id=W.rcis_id and C.contact_id=W.contact_id
left outer join distinctServiceable S
on A.rcis_id=S.rcis_id and A.fa_id=S.fa_id
 where S.rcis_id is null and S.fa_id is null
 and upper(C.City) = "VANCOUVER" and lower(C.streetname) = "york"
 --in (select distinct City from s_wirelineView)
