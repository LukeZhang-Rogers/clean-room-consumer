# Databricks notebook source
# MAGIC %md ## Executing Unit Testing for Deterministic Flow
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC
# MAGIC This is the unit testing notebook that supplies mock data to the DeterministicMatchingFlow notebook and asserts that the output computed in that notebook matches the expected output defined here. It also checks that the count of records matches across the two outputs, and if the computed output is different from the expected output, it will generate a dataframe highlighting the differences for easier troubleshooting.
# MAGIC This notebook covers unit testing for initial set of deterministic rules (1-15, 30-32). New deterministic rules are not covered yet.
# MAGIC
# MAGIC
# MAGIC ### Requirements:
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC   <li>DeterministicMatchingFlow notebook is configured to run the entire flow</li>
# MAGIC
# MAGIC </ul>
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Parameters:
# MAGIC
# MAGIC <b>execution_mode:</b> Defaulted to 0 for unit testing.</br>
# MAGIC <b>overwrite_output:</b> Defaulted to 0 for unit testing.</br>

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configuration

# COMMAND ----------

from pyspark.sql.functions import col
from datetime import datetime
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##Mock Data

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rogers WLS

# COMMAND ----------

r_wls_c_joined_schema =\
"r_contact_id	string,\
r_rcis_id_cl	string,\
r_fa_id_cl	string,\
r_dq_full_name_cl	string,\
r_dq_primary_phone_cl	string,\
r_dq_alternate_phone_1_cl	string,\
r_dq_alternate_phone_2_cl	string,\
r_dq_e_mail_cl	string,\
r_address_cl	string,\
r_city_cl	string,\
r_state_cl	string,\
r_zipcode_cl	string,\
r_snapshot_stamp	date,\
r_execution_date	timestamp,\
r_mailing_address_no_zipcode_dq_cl	string,\
r_source_system	string,\
r_account_status	string,\
r_service_provider	string,\
r_service_address_cl	string,\
r_sam_key string,\
r_service_address_no_zipcode_dq_cl	string,\
r_account_type	string"

r_wls_c_joined_data = [(\
"20066395",\
"71926419",\
"563520402",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"V21",\
"O",\
"Wireless Rogers",\
None,\
None,\
None,\
"wireless")]


r_wls_c_joined = spark.createDataFrame(r_wls_c_joined_data, r_wls_c_joined_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rogers WLN

# COMMAND ----------

r_wln_c_joined_schema =\
"r_contact_id	string,\
r_rcis_id_cl	string,\
r_fa_id_cl	string,\
r_dq_full_name_cl	string,\
r_dq_primary_phone_cl	string,\
r_dq_alternate_phone_1_cl	string,\
r_dq_alternate_phone_2_cl	string,\
r_dq_e_mail_cl	string,\
r_address_cl	string,\
r_city_cl	string,\
r_state_cl	string,\
r_zipcode_cl	string,\
r_snapshot_stamp	date,\
r_execution_date	timestamp,\
r_mailing_address_no_zipcode_dq_cl	string,\
r_source_system	string,\
r_account_status string,\
r_service_provider	string,\
r_service_address_cl	string,\
r_sam_key string,\
r_service_address_no_zipcode_dq_cl	string,\
r_account_type	string"

r_wln_c_joined_data = [(\
"20066395",\
"71926419",\
"211283718",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"Maestro",\
"O",\
"Rogers",\
"1005 123a ave nw toronto on m6c2l4",\
"2210000170502",\
"1005 123a ave toronto on",\
"wireline")]


r_wln_c_joined = spark.createDataFrame(r_wln_c_joined_data, r_wln_c_joined_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Shaw WLS

# COMMAND ----------

s_wls_c_joined_schema =\
"s_contact_id	string,\
s_rcis_id_cl	string,\
s_fa_id_cl	string,\
s_dq_full_name_cl	string,\
s_dq_primary_phone_cl	string,\
s_dq_alternate_phone_1_cl	string,\
s_dq_alternate_phone_2_cl	string,\
s_dq_e_mail_cl	string,\
s_address_cl	string,\
s_city_cl	string,\
s_state_cl	string,\
s_zipcode_cl	string,\
s_snapshot_stamp	date,\
s_execution_date	timestamp,\
s_mailing_address_no_zipcode_dq_cl	string,\
s_source_system	string,\
s_account_status	string,\
s_service_provider	string,\
s_service_address_cl	string,\
s_sam_key	string,\
s_service_address_no_zipcode_dq_cl	string,\
s_account_type	string"

s_wls_c_joined_data = [(\
"14662416",\
"1000113",\
"pqr50002071947",\
"john doe",\
"8254403036",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a avenue northwest",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a avenue toronto on",\
"SV",\
None,\
"Shaw Mobile",\
None,\
None,\
None,\
"wireless")]

s_wls_c_joined = spark.createDataFrame(s_wls_c_joined_data, s_wls_c_joined_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Shaw WLN

# COMMAND ----------

s_wln_c_joined_schema =\
"s_contact_id	string,\
s_rcis_id_cl	string,\
s_fa_id_cl	string,\
s_dq_full_name_cl	string,\
s_dq_primary_phone_cl	string,\
s_dq_alternate_phone_1_cl	string,\
s_dq_alternate_phone_2_cl	string,\
s_dq_e_mail_cl	string,\
s_address_cl	string,\
s_city_cl	string,\
s_state_cl	string,\
s_zipcode_cl	string,\
s_snapshot_stamp	date,\
s_execution_date	timestamp,\
s_mailing_address_no_zipcode_dq_cl	string,\
s_source_system	string,\
s_account_status	string,\
s_service_provider	string,\
s_service_address_cl	string,\
s_sam_key	string,\
s_service_address_no_zipcode_dq_cl	string,\
s_account_type	string"

s_wln_c_joined_data = [(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline")]

s_wln_c_joined = spark.createDataFrame(s_wln_c_joined_data, s_wln_c_joined_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Execute MatchingRules

# COMMAND ----------

# MAGIC %run ./DeterministicMatchingFlow $execution_mode="0" $overwrite_output="0"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Expected Data

# COMMAND ----------

# MAGIC %md
# MAGIC ###Results Schema

# COMMAND ----------

results_schema =\
"s_contact_id	string,\
s_rcis_id_cl	string,\
s_fa_id_cl	string,\
s_dq_full_name_cl	string,\
s_dq_primary_phone_cl	string,\
s_dq_alternate_phone_1_cl	string,\
s_dq_alternate_phone_2_cl	string,\
s_dq_e_mail_cl	string,\
s_address_cl	string,\
s_city_cl	string,\
s_state_cl	string,\
s_zipcode_cl	string,\
s_snapshot_stamp	date,\
s_execution_date	timestamp,\
s_mailing_address_no_zipcode_dq_cl	string,\
s_source_system	string,\
s_account_status	string,\
s_service_provider	string,\
s_service_address_cl	string,\
s_sam_key	string,\
s_service_address_no_zipcode_dq_cl	string,\
s_account_type	string,\
r_contact_id	string,\
r_rcis_id_cl	string,\
r_fa_id_cl	string,\
r_dq_full_name_cl	string,\
r_dq_primary_phone_cl	string,\
r_dq_alternate_phone_1_cl	string,\
r_dq_alternate_phone_2_cl	string,\
r_dq_e_mail_cl	string,\
r_address_cl	string,\
r_city_cl	string,\
r_state_cl	string,\
r_zipcode_cl	string,\
r_snapshot_stamp	date,\
r_execution_date	timestamp,\
r_mailing_address_no_zipcode_dq_cl	string,\
r_source_system	string,\
r_account_status	string,\
r_service_provider	string,\
r_service_address_cl	string,\
r_sam_key string,\
r_service_address_no_zipcode_dq_cl	string,\
r_account_type	string,\
Ruleset_ID string"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Results Data

# COMMAND ----------

results_data = [(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"211283718",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"Maestro",\
"O",\
"Rogers",\
"1005 123a ave nw toronto on m6c2l4",\
"2210000170502",\
"1005 123a ave toronto on",\
"wireline",\
"11")\
,(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"563520402",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"V21",\
"O",\
"Wireless Rogers",\
None,\
None,\
None,\
"wireless",\
"11")\
,(\
"14662416",\
"1000113",\
"pqr50002071947",\
"john doe",\
"8254403036",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a avenue northwest",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a avenue toronto on",\
"SV",\
None,\
"Shaw Mobile",\
None,\
None,\
None,\
"wireless",\
"20066395",\
"71926419",\
"563520402",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"V21",\
"O",\
"Wireless Rogers",\
None,\
None,\
None,\
"wireless",\
"11")\
,(\
"14662416",\
"1000113",\
"pqr50002071947",\
"john doe",\
"8254403036",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a avenue northwest",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a avenue toronto on",\
"SV",\
None,\
"Shaw Mobile",\
None,\
None,\
None,\
"wireless",\
"20066395",\
"71926419",\
"211283718",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"Maestro",\
"O",\
"Rogers",\
"1005 123a ave nw toronto on m6c2l4",\
"2210000170502",\
"1005 123a ave toronto on",\
"wireline",\
"11")\
,(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"211283718",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"Maestro",\
"O",\
"Rogers",\
"1005 123a ave nw toronto on m6c2l4",\
"2210000170502",\
"1005 123a ave toronto on",\
"wireline",\
"13")\
,(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"563520402",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"V21",\
"O",\
"Wireless Rogers",\
None,\
None,\
None,\
"wireless",\
"14")\
,(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"211283718",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"Maestro",\
"O",\
"Rogers",\
"1005 123a ave nw toronto on m6c2l4",\
"2210000170502",\
"1005 123a ave toronto on",\
"wireline",\
"14")\
,(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"211283718",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"Maestro",\
"O",\
"Rogers",\
"1005 123a ave nw toronto on m6c2l4",\
"2210000170502",\
"1005 123a ave toronto on",\
"wireline",\
"15")\
,(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"563520402",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"V21",\
"O",\
"Wireless Rogers",\
None,\
None,\
None,\
"wireless",\
"15")\
,(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"211283718",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"Maestro",\
"O",\
"Rogers",\
"1005 123a ave nw toronto on m6c2l4",\
"2210000170502",\
"1005 123a ave toronto on",\
"wireline",\
"30")\
,(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"563520402",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"V21",\
"O",\
"Wireless Rogers",\
None,\
None,\
None,\
"wireless",\
"31")\
,(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"211283718",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"Maestro",\
"O",\
"Rogers",\
"1005 123a ave nw toronto on m6c2l4",\
"2210000170502",\
"1005 123a ave toronto on",\
"wireline",\
"31")\
,(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"563520402",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"V21",\
"O",\
"Wireless Rogers",\
None,\
None,\
None,\
"wireless",\
"32")\
,(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"211283718",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"Maestro",\
"O",\
"Rogers",\
"1005 123a ave nw toronto on m6c2l4",\
"2210000170502",\
"1005 123a ave toronto on",\
"wireline",\
"32")\
,(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"211283718",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"Maestro",\
"O",\
"Rogers",\
"1005 123a ave nw toronto on m6c2l4",\
"2210000170502",\
"1005 123a ave toronto on",\
"wireline",\
"5")\
,(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"563520402",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"V21",\
"O",\
"Wireless Rogers",\
None,\
None,\
None,\
"wireless",\
"6")\
,(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"211283718",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"Maestro",\
"O",\
"Rogers",\
"1005 123a ave nw toronto on m6c2l4",\
"2210000170502",\
"1005 123a ave toronto on",\
"wireline",\
"6")\
,(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"563520402",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"V21",\
"O",\
"Wireless Rogers",\
None,\
None,\
None,\
"wireless",\
"7")\
,(\
"1000113",\
"1000113",\
"3016387410",\
"john doe",\
"4165537455",\
"4165537455",\
None,\
"jdoegmailcom",\
"1005 123a ave nw",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"SBO",\
None,\
None,\
"1005 123a ave nw",\
"717080",\
"1005 123a ave toronto on",\
"Wireline",\
"20066395",\
"71926419",\
"211283718",\
"john doe",\
None,\
"6191118434",\
"7804201062",\
"jdoegmailcom",\
"1005 123a ave nw toronto on m6c2l4",\
"toronto",\
"on",\
"m6c2l4",\
datetime.strptime("2/9/2022", "%d/%m/%Y"),\
datetime.strptime("2022-02-15T16:57:25.965+0000", '%Y-%m-%dT%H:%M:%S.%f%z'),\
"1005 123a ave toronto on",\
"Maestro",\
"O",\
"Rogers",\
"1005 123a ave nw toronto on m6c2l4",\
"2210000170502",\
"1005 123a ave toronto on",\
"wireline",\
"7")]

# COMMAND ----------

# MAGIC %md
# MAGIC ###Results DF

# COMMAND ----------

results_df = spark.createDataFrame(results_data, results_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Compare Counts

# COMMAND ----------

computed_df = matched_dataset
expected_df = results_df

assert(computed_df.count() == expected_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##Show Differences

# COMMAND ----------

diffcomputed_df = computed_df.subtract(expected_df)
diffexpected_df = expected_df.subtract(computed_df)
testResults = diffcomputed_df.withColumn("SourceDF", lit("computed_df")).union(diffexpected_df.withColumn("SourceDF", lit("expected_df")))
display(testResults)

# COMMAND ----------

assert(testResults.count()==0)

# COMMAND ----------

matched_dataset.unpersist()
