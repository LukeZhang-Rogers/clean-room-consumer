# Databricks notebook source
# MAGIC %md ## Common Path Parameters
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC
# MAGIC This notebook lists all the paths based on the ADLS design. All downstream notebooks are inheriting this notebook. This is a common notebook to centrally control all input and output directories.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Landing Zone

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw Landing Datasets

# COMMAND ----------

# -- This command line removed as Landing folder is no more necessary. Files are pointing to Staging layer. Remove data duplicacy.

#################### input Path #################

#LD1/Post-LD1 Paths
# shaw_consumer_contact_landing             = 'dbfs:/mnt/rawstd/clean-room/consumer/consumer_contact'
# shaw_consumer_wireline_account_landing    = 'dbfs:/mnt/rawstd/clean-room/consumer/consumer_wireline_account'
# shaw_consumer_wireless_account_landing    = 'dbfs:/mnt/rawstd/clean-room/consumer/consumer_wireless_account'
# shaw_consumer_wireless_service_landing    = 'dbfs:/mnt/rawstd/clean-room/consumer/consumer_wireless_service'

# rogers_contact_landing                    = 'dbfs:/mnt/derived/app_rci_cr_cons/contact'
# rogers_wireline_account_landing           = 'dbfs:/mnt/derived/app_rci_cr_cons/consumer_wireline_account'
# rogers_wireless_account_landing           = 'dbfs:/mnt/derived/app_rci_cr_cons/consumer_wireless_accounts'
# rogers_wireless_service_landing           = 'dbfs:/mnt/derived/app_rci_cr_cons/consumer_wireless_service'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Serviceability Raw Landing Datasets

# COMMAND ----------

# -- This command line removed as Landing folder is no more necessary. Files are pointing to Staging layer. Remove data duplicacy.

#LD1/Post-LD1 Paths
# rogers_wireless_serviceability_landing    = 'dbfs:/mnt/rawstd/rci_cr_cons/wireless_serviceability'
# rogers_wireline_serviceability_landing    = 'dbfs:/mnt/rawstd/rci_cr_cons/QATest/sample_parquet' #Need to change
# shaw_wireline_serviceability_landing      = 'dbfs:/mnt/rawstd/clean-room/consumer/wireline_serviceability'
# rogers_service_qualification_landing      = "dbfs:/mnt/rawstd/rci_cr_cons/service_qualification"
# rogers_service_qualification_item_landing = "dbfs:/mnt/rawstd/rci_cr_cons/service_qualification_item"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw Staging Zone

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw Staging Datasets

# COMMAND ----------

 #################### input Path #################
#LD1/Post-LD1 Paths
shaw_consumer_contact             = 'dbfs:/mnt/derived/ConsumerInput/shaw_consumer_contact' #rawstd__shaw_cr_cons.consumer_contact
shaw_consumer_wireline_account    = 'dbfs:/mnt/derived/ConsumerInput/shaw_consumer_wireline_account' #rawstd__shaw_cr_cons.consumer_wireline_account
shaw_consumer_wireless_account    = 'dbfs:/mnt/derived/ConsumerInput/shaw_consumer_wireless_account' #rawstd__shaw_cr_cons.consumer_wireless_account
shaw_consumer_wireless_service    = 'dbfs:/mnt/derived/ConsumerInput/shaw_consumer_wireless_service' #rawstd__shaw_cr_cons.consumer_wireless_service

rogers_contact                    = 'dbfs:/mnt/derived/ConsumerInput/rogers_contact' #drvd__app_rci_cr_cons.contact
rogers_wireline_account           = 'dbfs:/mnt/derived/ConsumerInput/rogers_consumer_wireline_account' #drvd__app_rci_cr_cons.consumer_wireline_account
rogers_wireless_account           = 'dbfs:/mnt/derived/ConsumerInput/rogers_consumer_wireless_accounts' #drvd__app_rci_cr_cons.consumer_wireless_accounts
rogers_wireless_service           = 'dbfs:/mnt/derived/ConsumerInput/rogers_consumer_wireless_service' #drvd__app_rci_cr_cons.consumer_wireless_service

# COMMAND ----------

# MAGIC %md
# MAGIC ### Serviceability Raw Staging Datasets

# COMMAND ----------

#LD1/Post-LD1 Paths
rogers_wireless_serviceability    = 'dbfs:/mnt/derived/ConsumerInput/wireless_serviceability' #rawstd__rci_cr_cons.wireless_serviceability
rogers_wireline_serviceability    = 'dbfs:/mnt/derived/ConsumerInput/geographic_address' #rawstd__rci_cr_cons.geographic_address
shaw_wireline_serviceability      = 'dbfs:/mnt/derived/ConsumerInput/wireline_serviceability' #rawstd__shaw_cr_cons.wireline_serviceability
rogers_service_qualification      = "dbfs:/mnt/derived/ConsumerInput/service_qualification" #rawstd__rci_cr_cons.service_qualification
rogers_service_qualification_item = "dbfs:/mnt/derived/ConsumerInput/service_qualification_item" #rawstd__rci_cr_cons.service_qualification_item

# COMMAND ----------

# MAGIC %md
# MAGIC # Processed Zone

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleansed Datasets

# COMMAND ----------

###################### Output Path #####################
cl_shaw_consumer_contact          = 'dbfs:/mnt/derived/Processed/Shaw/Contact'
cl_shaw_consumer_wireline_account = 'dbfs:/mnt/derived/Processed/Shaw/WirelineAccount'
cl_shaw_consumer_wireless_account = 'dbfs:/mnt/derived/Processed/Shaw/WirelessAccount'
cl_shaw_consumer_wireless_service = 'dbfs:/mnt/derived/Processed/Shaw/WirelessService'

cl_rogers_contact                 = 'dbfs:/mnt/derived/Processed/Rogers/Contact'
cl_rogers_wireline_account        = 'dbfs:/mnt/derived/Processed/Rogers/WirelineAccount'
cl_rogers_wireless_account        = 'dbfs:/mnt/derived/Processed/Rogers/WirelessAccount'
cl_rogers_wireless_service        = 'dbfs:/mnt/derived/Processed/Rogers/WirelessService'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Serviceability Cleansed Datasets

# COMMAND ----------

cl_rogers_wireless_serviceability       = 'dbfs:/mnt/derived/Processed/Rogers/WirelessServiceability'
cl_rogers_wireline_serviceability       = 'dbfs:/mnt/derived/Processed/Rogers/WirelineServiceability'
cl_shaw_wireline_serviceability         = 'dbfs:/mnt/derived/Processed/Shaw/WirelineServiceability'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interim Output

# COMMAND ----------

# MAGIC %md
# MAGIC ### Foundation Datasets

# COMMAND ----------

foundation_rogers_wls_c      =  "dbfs:/mnt/derived/Processed/InterimOutput/Rogers/WirelessAndContact"
foundation_rogers_wln_c      =  "dbfs:/mnt/derived/Processed/InterimOutput/Rogers/WirelineAndContact"
foundation_shaw_wls_c        =  "dbfs:/mnt/derived/Processed/InterimOutput/Shaw/WirelessAndContact"
foundation_shaw_wln_c        =  "dbfs:/mnt/derived/Processed/InterimOutput/Shaw/WirelineAndContact"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deterministic

# COMMAND ----------

interimop_det_matched     =  "dbfs:/mnt/derived/Processed/InterimOutput/Deterministic/Matched"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fuzzy and Probabilistic

# COMMAND ----------

interimop_fuzzy_customer = "dbfs:/mnt/derived/Processed/InterimOutput/Fuzzy/CustomerLevelRules"
interimop_fuzzy_address  = "dbfs:/mnt/derived/Processed/InterimOutput/Fuzzy/AddressLevelRules"

interimop_prob_customer  = "dbfs:/mnt/derived/Processed/InterimOutput/Probabilistic/CustomerLevelRules"
interimop_prob_address   = "dbfs:/mnt/derived/Processed/InterimOutput/Probabilistic/AddressLevelRules"

# COMMAND ----------

# MAGIC %md
# MAGIC # Interim Consumption Zone

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

shaw_consumer_wireless_account_df = spark.read.format("delta").load(shaw_consumer_wireless_account)
shaw_consumer_wireline_account_df = spark.read.format("delta").load(shaw_consumer_wireline_account)
shaw_consumer_contact_df          = spark.read.format("delta").load(shaw_consumer_contact)
shaw_consumer_wireless_service_df = spark.read.format("delta").load(shaw_consumer_wireless_service)

rogers_contact_df                 = spark.read.format("delta").load(rogers_contact)
rogers_wireless_account_df        = spark.read.format("delta").load(rogers_wireless_account)
rogers_wireline_account_df        = spark.read.format("delta").load(rogers_wireline_account)
rogers_wireless_service_df        = spark.read.format("delta").load(rogers_wireless_service)

r_date_1 = rogers_contact_df.first()['snapshot_stamp']
r_date_2 = rogers_wireless_account_df.first()['snapshot_stamp']
r_date_3 = rogers_wireline_account_df.first()['snapshot_stamp']
r_date_4 = rogers_wireless_service_df.first()['snapshot_stamp']

s_date_1 = shaw_consumer_contact_df.first()['SNAPSHOT_STAMP']
s_date_2 = shaw_consumer_wireless_account_df.first()['SNAPSHOT_STAMP']
s_date_3 = shaw_consumer_wireline_account_df.first()['SNAPSHOT_STAMP']
s_date_4 = shaw_consumer_wireless_service_df.first()['SNAPSHOT_STAMP']

# COMMAND ----------

# picking max date from all
date_list = [(r_date_1), (r_date_2),(r_date_3),(r_date_4), (s_date_1), (s_date_2),(s_date_3),(s_date_4)]
schema = StructType([StructField('date_column', TimestampType(), True)])
timestamp_list = [(datetime.combine(date_obj, datetime.min.time()),) for date_obj in date_list]
date_df = spark.createDataFrame(timestamp_list, schema)
max_date = date_df.agg(max('date_column')).collect()[0][0]
max_date = max_date.date()
date = max_date.strftime("%Y-%m-%d")
print(date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Models

# COMMAND ----------

# versoning
account_matched_ruleset    = "dbfs:/mnt/derived/OutputStaging/ConsumerAccountMatchedRuleset/" + "SNAPSHOT_STAMP=" + date
matched_entity             = "dbfs:/mnt/derived/OutputStaging/ConsumerMatchedEntity/" + "SNAPSHOT_STAMP=" + date
serviceability_matched     = "dbfs:/mnt/derived/OutputStaging/ServiceabilityMatchedAccounts/" + "SNAPSHOT_STAMP=" + date
customer_marketing_op      = "dbfs:/mnt/derived/OutputStaging/CustomerMarketing/" + "SNAPSHOT_STAMP=" + date

# COMMAND ----------

ruleset = "dbfs:/mnt/derived/OutputStaging/ConsumerRuleset"

# COMMAND ----------

# MAGIC %md
# MAGIC ## CCMM

# COMMAND ----------

unioned_contact      = "dbfs:/mnt/derived/OutputStaging/CCMM/ConsumerContact/" + "SNAPSHOT_STAMP=" + date
unioned_wireless_acc = "dbfs:/mnt/derived/OutputStaging/CCMM/ConsumerWirelessAccount/" + "SNAPSHOT_STAMP=" + date
unioned_wireline_acc = "dbfs:/mnt/derived/OutputStaging/CCMM/ConsumerWirelineAccount/" + "SNAPSHOT_STAMP=" + date
unioned_wireless_ser = "dbfs:/mnt/derived/OutputStaging/CCMM/ConsumerWirelessService/" + "SNAPSHOT_STAMP=" + date

# COMMAND ----------

rogers_geo_serviceability         = "dbfs:/mnt/derived/OutputStaging/CCMM/RogersGeographicAddress"
rogers_ser_qualification      = "dbfs:/mnt/derived/OutputStaging/CCMM/RogersServiceQualification"
rogers_ser_qualification_item = "dbfs:/mnt/derived/OutputStaging/CCMM/RogersServiceQualificationItem"
rogers_wls_serviceability         = "dbfs:/mnt/derived/OutputStaging/CCMM/RogersWirelessServiceability"
shaw_wln_serviceability           = "dbfs:/mnt/derived/OutputStaging/CCMM/ShawWirelineServiceability"

# COMMAND ----------

# MAGIC %md
# MAGIC # Final Consumption Zone

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Models

# COMMAND ----------

#Versioning
#LD1/Post-LD1 Paths
account_matched_ruleset_final = "dbfs:/mnt/derived/Consumption/ConsumerAccountMatchedRuleset/" + "SNAPSHOT_STAMP=" + date
matched_entity_final          = "dbfs:/mnt/derived/Consumption/ConsumerMatchedEntity/" + "SNAPSHOT_STAMP=" + date
serviceability_matched_final  = "dbfs:/mnt/derived/Consumption/ServiceabilityMatchedAccounts/" + "SNAPSHOT_STAMP=" + date
customer_marketing_final      = "dbfs:/mnt/derived/Consumption/CustomerMarketing/" + "SNAPSHOT_STAMP=" + date

# COMMAND ----------

#LD1/Post-LD1 Paths
ruleset_final = "dbfs:/mnt/derived/Consumption/ConsumerRuleset"

# COMMAND ----------

# MAGIC %md
# MAGIC ## CCMM

# COMMAND ----------

#LD1/Post-LD1 Paths
unioned_contact_final      = "dbfs:/mnt/derived/Consumption/CCMM/ConsumerContact/" + "SNAPSHOT_STAMP=" + date
unioned_wireless_acc_final = "dbfs:/mnt/derived/Consumption/CCMM/ConsumerWirelessAccount/" + "SNAPSHOT_STAMP=" + date
unioned_wireline_acc_final = "dbfs:/mnt/derived/Consumption/CCMM/ConsumerWirelineAccount/" + "SNAPSHOT_STAMP=" + date
unioned_wireless_ser_final = "dbfs:/mnt/derived/Consumption/CCMM/ConsumerWirelessService/" + "SNAPSHOT_STAMP=" + date

# COMMAND ----------

#LD1/Post-LD1 Paths
rogers_geo_serviceability_final         = "dbfs:/mnt/derived/Consumption/CCMM/RogersGeographicAddress"
rogers_service_qualification_final      = "dbfs:/mnt/derived/Consumption/CCMM/RogersServiceQualification"
rogers_service_qualification_item_final = "dbfs:/mnt/derived/Consumption/CCMM/RogersServiceQualificationItem"
rogers_wls_serviceability_final         = "dbfs:/mnt/derived/Consumption/CCMM/RogersWirelessServiceability"
shaw_wln_serviceability_final           = "dbfs:/mnt/derived/Consumption/CCMM/ShawWirelineServiceability"
