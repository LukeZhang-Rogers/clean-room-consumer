# Databricks notebook source
# MAGIC %md
# MAGIC #Load data

# COMMAND ----------

from pyspark.sql.functions import *

#Cleaned Data paths

shaw_path = "/mnt/development/Processed/Iteration8/Shaw" 
rogers_path = "/mnt/development/Processed/Iteration8/Rogers" 

shaw_consumer_contact = shaw_path + '/Contact' 
shaw_consumer_wireless_account = shaw_path + '/WirelessAccount' 
shaw_consumer_wireline_account = shaw_path + '/WirelineAccount' 

rogers_contact = rogers_path + '/Contact' 
rogers_wireless_account = rogers_path + '/WirelessAccount' 
rogers_wireline_account = rogers_path + '/WirelineAccount'

#Load dataframes
# cl_shaw_consumer_contact_df                = spark.read.format("parquet").load(shaw_consumer_contact)
# cl_shaw_consumer_wireless_account_df       = spark.read.format("parquet").load(shaw_consumer_wireless_account)
# cl_shaw_consumer_wireline_account_df       = spark.read.format("parquet").load(shaw_consumer_wireline_account)

cl_rogers_contact_df                 = spark.read.format("parquet").load(rogers_contact)
cl_rogers_wireless_account_df        = spark.read.format("parquet").load(rogers_wireless_account)
cl_rogers_wireline_account_df        = spark.read.format("parquet").load(rogers_wireline_account)

AccountMatchedRuleset = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration4/AccountMatchedRuleset")
MatchedEntity = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration4/MatchedEntity")

# COMMAND ----------

# MAGIC %md
# MAGIC #EDA

# COMMAND ----------

#Account_Matched_Ruleset table

#filter Matched and Rogers customer ID not NULL
filtered_AccountMatchedRuleset = AccountMatchedRuleset.filter((col('RULESET_TYPE') == 'UNMATCHED'))
print('filtered_AccountMatchedRuleset = ',filtered_AccountMatchedRuleset.count())

#distinct pairs only
unmatched_distinct_AccountMatchedRuleset = filtered_AccountMatchedRuleset.select('ROGERS_ECID','SHAW_MASTER_PARTY_ID').distinct()
print('unmatched_distinct_AccountMatchedRuleset_distinct = ',unmatched_distinct_AccountMatchedRuleset.count())

#distinct Rogers only
unmatched_distinct_Rogers_AccountMatchedRuleset = filtered_AccountMatchedRuleset.select('ROGERS_ECID').distinct()
print('unmatched_distinct_Rogers_AccountMatchedRuleset = ',unmatched_distinct_Rogers_AccountMatchedRuleset.count())

#distinct Shaw only
unmatched_distinct_Shaw_AccountMatchedRuleset = filtered_AccountMatchedRuleset.select('SHAW_MASTER_PARTY_ID').distinct()
print('unmatched_distinct_Shaw_AccountMatchedRuleset = ',unmatched_distinct_Shaw_AccountMatchedRuleset.count())

# COMMAND ----------

filtered_AccountMatchedRuleset.filter(filtered_AccountMatchedRuleset.ROGERS_ACCOUNT_STATUS.isin(['o','s','t',2,3,4,7])).toPandas()

# COMMAND ----------

filtered_AccountMatchedRuleset.select('ROGERS_ACCOUNT_STATUS').distinct().toPandas()

# COMMAND ----------

#Rogers Wireless table - active accounts only (O,S,T - wireless)

rogers_wireless_active_account_list = ['O','S','T']
cl_rogers_wireless_account_df_active = cl_rogers_wireless_account_df.filter(cl_rogers_wireless_account_df.account_status.isin(rogers_wireless_active_account_list))
cl_rogers_wireless_account_df_active = cl_rogers_wireless_account_df_active.select('rcis_id_cl','fa_id_cl','service_provider','account_status')
cl_rogers_wireless_account_df_active.toPandas()

# COMMAND ----------

#Rogers Wireline table - active accounts only (O,S,T,2,3,4,7 - wireline)

rogers_wireline_active_account_list = ['O','S','T',2,3,4,7]
cl_rogers_wireline_account_df_active = cl_rogers_wireline_account_df.filter(cl_rogers_wireline_account_df.account_status.isin(rogers_wireline_active_account_list))
cl_rogers_wireline_account_df_active = cl_rogers_wireline_account_df_active.select('rcis_id_cl','fa_id_cl','account_status')
cl_rogers_wireline_account_df_active.toPandas()

# COMMAND ----------

#Shaw wireless table ('Active Suspend' + 'Active')

shaw_wireless_active_account_list = ['Active Suspend', 'Active']
cl_shaw_consumer_wireless_account_df_active = cl_shaw_consumer_wireless_account_df.filter(cl_shaw_consumer_wireless_account_df.account_status.isin(shaw_wireless_active_account_list))
cl_shaw_consumer_wireless_account_df_active = cl_shaw_consumer_wireless_account_df.select('rcis_id_cl','fa_id_cl')
cl_shaw_consumer_wireless_account_df_active.toPandas()

# COMMAND ----------

# Shaw wireline table (all active)

cl_shaw_consumer_wireline_account_df_active = cl_shaw_consumer_wireline_account_df.select('rcis_id_cl','fa_id_cl','SHAW_DIRECT_FLAG','INTERNET_FLAG','WIRELINE_PHONE_FLAG','source_system')
cl_shaw_consumer_wireline_account_df_active.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Join tables together

# COMMAND ----------

#Rogers
#from 'filtered_AccountMatchedRuleset'
#Join 'cl_rogers_wireless_account_df' & 'cl_rogers_wireline_account_df_active'

df_amr_joined_rogers_ws_wl = filtered_AccountMatchedRuleset.join(cl_rogers_wireless_account_df_active, (filtered_AccountMatchedRuleset.ROGERS_ECID == cl_rogers_wireless_account_df_active.rcis_id_cl) & (filtered_AccountMatchedRuleset.ROGERS_ACCOUNT_ID == cl_rogers_wireless_account_df_active.fa_id_cl), 'left').join(cl_rogers_wireline_account_df_active, (filtered_AccountMatchedRuleset.ROGERS_ECID == cl_rogers_wireline_account_df_active.rcis_id_cl) & (filtered_AccountMatchedRuleset.ROGERS_ACCOUNT_ID == cl_rogers_wireline_account_df_active.fa_id_cl), 'left')

df_amr_joined_rogers_ws_wl = df_amr_joined_rogers_ws_wl.select('ROGERS_ECID','ROGERS_ACCOUNT_ID','ROGERS_ACCOUNT_TYPE','service_provider')
df_amr_joined_rogers_ws_wl.toPandas()



# #Join 'cl_rogers_wireless_account_df' from 'filtered_AccountMatchedRuleset'

# df_amr_joined_rogers = filtered_AccountMatchedRuleset.join(cl_rogers_wireless_account_df_temp, (filtered_AccountMatchedRuleset.ROGERS_ECID == cl_rogers_wireless_account_df_temp.rcis_id_cl) & (filtered_AccountMatchedRuleset.ROGERS_ACCOUNT_ID == cl_rogers_wireless_account_df_temp.fa_id_cl), 'left')
# df_amr_joined_rogers = df_amr_joined_rogers.filter(df_amr_joined_rogers.account_status.isin(rogers_active_account_list))
# df_amr_joined_rogers = df_amr_joined_rogers.select('ROGERS_ECID','ROGERS_ACCOUNT_ID','account_status','ROGERS_ACCOUNT_TYPE','service_provider')
# df_amr_joined_rogers.toPandas()

# COMMAND ----------

print('df_amr_joined_rogers_ws_wl_distinct = ',df_amr_joined_rogers_ws_wl.select('ROGERS_ECID').distinct().count())


# COMMAND ----------

#Join Shaw
#from 'filtered_AccountMatchedRuleset'
#join 'cl_shaw_consumer_wireless_account_df_active' + 'cl_shaw_consumer_wireline_account_df_active'

df_amr_joined_shaw_ws_wl = filtered_AccountMatchedRuleset.join(cl_shaw_consumer_wireless_account_df_active, (filtered_AccountMatchedRuleset.SHAW_MASTER_PARTY_ID == cl_shaw_consumer_wireless_account_df_active.rcis_id_cl) & (filtered_AccountMatchedRuleset.SHAW_ACCOUNT_ID == cl_shaw_consumer_wireless_account_df_active.fa_id_cl), 'left').join(cl_shaw_consumer_wireline_account_df_active, (filtered_AccountMatchedRuleset.SHAW_MASTER_PARTY_ID == cl_shaw_consumer_wireline_account_df_active.rcis_id_cl) & (filtered_AccountMatchedRuleset.SHAW_ACCOUNT_ID == cl_shaw_consumer_wireline_account_df_active.fa_id_cl), 'left')

df_amr_joined_shaw_ws_wl = df_amr_joined_shaw_ws_wl.select('SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID','SHAW_ACCOUNT_TYPE','SHAW_ACCOUNT_STATUS','SHAW_DIRECT_FLAG','INTERNET_FLAG','WIRELINE_PHONE_FLAG','source_system')
df_amr_joined_shaw_ws_wl.toPandas()


# #Join 'cl_shaw_consumer_wireline_account_df_temp' from 'filtered_AccountMatchedRuleset'

# df_amr_joined_shaw = filtered_AccountMatchedRuleset.join(cl_shaw_consumer_wireline_account_df_temp, (filtered_AccountMatchedRuleset.SHAW_MASTER_PARTY_ID == cl_shaw_consumer_wireline_account_df_temp.rcis_id_cl) & (filtered_AccountMatchedRuleset.SHAW_ACCOUNT_ID == cl_shaw_consumer_wireline_account_df_temp.fa_id_cl), 'left')
# df_amr_joined_shaw = df_amr_joined_shaw.select('SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID','SHAW_ACCOUNT_TYPE','SHAW_ACCOUNT_STATUS','SHAW_DIRECT_FLAG','INTERNET_FLAG','WIRELINE_PHONE_FLAG','source_system')
# df_amr_joined_shaw.toPandas()


# COMMAND ----------

print('df_amr_joined_rogers_ws_wl_distinct = ',df_amr_joined_rogers_ws_wl.select('ROGERS_ECID').distinct().count())

print('df_amr_joined_shaw_ws_wl_distinct = ',df_amr_joined_shaw_ws_wl.select('SHAW_MASTER_PARTY_ID').distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Set flags

# COMMAND ----------

# MAGIC %md
# MAGIC ####Rogers

# COMMAND ----------

#Convert Rogers' service_provider to 2 columns 

df_amr_joined_rogers_1 = df_amr_joined_rogers_ws_wl.withColumn('Wireless_Rogers',when((col('service_provider') == 'Wireless Rogers'), 1).when((col('service_provider') == 'Wireless Fido'), 0).otherwise(None)).withColumn('Wireless_Fido',when((col('service_provider') == 'Wireless Fido'), 1).when((col('service_provider') == 'Wireless Rogers'), 0).otherwise(None))
df_amr_joined_rogers_1.toPandas()

# COMMAND ----------

df_amr_joined_rogers_2 = df_amr_joined_rogers_1.groupBy('ROGERS_ECID').agg(sum('Wireless_Rogers').alias('sum_Wireless_Rogers'),sum('Wireless_Fido').alias('sum_Wireless_Fido'))
df_amr_joined_rogers_2.sort('sum_Wireless_Rogers').toPandas()

# COMMAND ----------

#to get final rogers scenario

import pyspark.sql.functions as F
cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))

df_amr_joined_rogers_3 = df_amr_joined_rogers_2.groupBy('ROGERS_ECID').agg(
    cnt_cond(((col('sum_Wireless_Rogers') == 0) & (col('sum_Wireless_Fido') == 0)) | (col('sum_Wireless_Rogers').isNull() & col('sum_Wireless_Fido').isNull())).alias('Neither_rogers_fido'),
    cnt_cond(((col('sum_Wireless_Rogers') >= 1) & (col('sum_Wireless_Fido') == 0)) | ((col('sum_Wireless_Rogers') >= 1) & col('sum_Wireless_Fido').isNull())).alias('No_rogers_yes_fido'),
    cnt_cond(((col('sum_Wireless_Rogers') == 0) & (col('sum_Wireless_Fido') >= 1)) | (col('sum_Wireless_Rogers').isNull() & (col('sum_Wireless_Fido') >= 1))).alias('No_fido_yes_rogers'),
    cnt_cond((col('sum_Wireless_Rogers') >= 1) & (col('sum_Wireless_Fido') >= 1)).alias('Both_rogers_fido'))


df_amr_joined_rogers_3.toPandas()

# COMMAND ----------

df_amr_joined_rogers_4 = df_amr_joined_rogers_3.withColumn('Rogers_final',when(col('Neither_rogers_fido') == 1, 'Neither_rogers_fido').when(col('No_rogers_yes_fido') == 1, 'No_rogers_yes_fido').when(col('No_fido_yes_rogers') == 1, 'No_fido_yes_rogers').when(col('Both_rogers_fido') == 1, 'Both_rogers_fido'))
df_amr_joined_rogers_4 = df_amr_joined_rogers_4.select('ROGERS_ECID','Rogers_final')
df_amr_joined_rogers_4.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Shaw

# COMMAND ----------

#Set TV flag for Shaw CH

df_amr_joined_shaw_1 = df_amr_joined_shaw_ws_wl.withColumn('BlueCurve_and_Legacy_TV_Flag',when((col('source_system').isNotNull()), 'Y').otherwise(None))
df_amr_joined_shaw_1 = df_amr_joined_shaw_1.withColumn('SHAW_DIRECT_FLAG', when(col('SHAW_DIRECT_FLAG') == 'Y',1).when(col('SHAW_DIRECT_FLAG') == 'N',0).otherwise(None))
df_amr_joined_shaw_1 = df_amr_joined_shaw_1.drop('VIDEO_DELIVERY_TYPE','video_flag','source_system')

#Combine Shaw CH flags

df_amr_joined_shaw_1 = df_amr_joined_shaw_1.withColumn('SHAW_CH', when((col('WIRELINE_PHONE_FLAG') == 'Y') | (col('INTERNET_FLAG') == 'Y') | (col('BlueCurve_and_Legacy_TV_Flag') == 'Y'), 1).when((col('WIRELINE_PHONE_FLAG') == 'N') & (col('INTERNET_FLAG') == 'N') & (col('BlueCurve_and_Legacy_TV_Flag') == 'N'), 0).otherwise(None)).drop('INTERNET_FLAG','WIRELINE_PHONE_FLAG','BlueCurve_and_Legacy_TV_Flag')
df_amr_joined_shaw_1.toPandas()

# COMMAND ----------

df_amr_joined_shaw_2 = df_amr_joined_shaw_1.groupBy('SHAW_MASTER_PARTY_ID').agg(sum('SHAW_DIRECT_FLAG').alias('sum_SHAW_DIRECT_FLAG'),sum('SHAW_CH').alias('sum_SHAW_CH'))
df_amr_joined_shaw_2.sort('sum_SHAW_CH').toPandas()

# COMMAND ----------

#to get final shaw scenario

import pyspark.sql.functions as F
cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
 
df_amr_joined_shaw_3 = df_amr_joined_shaw_2.groupBy('SHAW_MASTER_PARTY_ID').agg(
    cnt_cond(((col('sum_SHAW_DIRECT_FLAG') == 0) & (col('sum_SHAW_CH') == 0)) | (col('sum_SHAW_DIRECT_FLAG').isNull() & col('sum_SHAW_CH').isNull())).alias('Neither_shaw_CH_direct'),
    cnt_cond(((col('sum_SHAW_DIRECT_FLAG') >= 1) & (col('sum_SHAW_CH') == 0)) | ((col('sum_SHAW_DIRECT_FLAG') >= 1) & col('sum_SHAW_CH').isNull())).alias('shaw_Direct_only'),
    cnt_cond(((col('sum_SHAW_DIRECT_FLAG') == 0) & (col('sum_SHAW_CH') >= 1)) | (col('sum_SHAW_DIRECT_FLAG').isNull() & (col('sum_SHAW_CH') >= 1))).alias('shaw_CH_only'),
    cnt_cond((col('sum_SHAW_DIRECT_FLAG') >= 1) & (col('sum_SHAW_CH') >= 1)).alias('Both_shaw_CH_direct'))
df_amr_joined_shaw_3.toPandas()

# COMMAND ----------

df_amr_joined_shaw_4 = df_amr_joined_shaw_3.withColumn('Shaw_final',when(col('Neither_shaw_CH_direct') == 1, 'Neither_shaw_CH_direct').when(col('shaw_Direct_only') == 1, 'shaw_Direct_only').when(col('shaw_CH_only') == 1, 'shaw_CH_only').when(col('Both_shaw_CH_direct') == 1, 'Both_shaw_CH_direct'))
df_amr_joined_shaw_4 = df_amr_joined_shaw_4.select('SHAW_MASTER_PARTY_ID','Shaw_final')
df_amr_joined_shaw_4.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #Province breakdown

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rogers' province

# COMMAND ----------

#Rogers Wireline table => for service address

# Rogers active accounts only
rogers_active_account_list = [2,3,4,7,'O','S','T']
cl_rogers_wireline_account_df_active = cl_rogers_wireline_account_df.filter(cl_rogers_wireline_account_df.account_status.isin(rogers_active_account_list))
cl_rogers_wireline_account_df_active.select('rcis_id_cl','fa_id_cl','account_status','service_provider').toPandas()

# COMMAND ----------

# filtered_AccountMatchedRuleset join on wireline tables for service address - rogers

df_UnmatchedAccount_Rs = filtered_AccountMatchedRuleset.join(cl_rogers_wireline_account_df_active, (cl_rogers_wireline_account_df_active.rcis_id_cl == filtered_AccountMatchedRuleset.ROGERS_ECID) & (cl_rogers_wireline_account_df_active.fa_id_cl == filtered_AccountMatchedRuleset.ROGERS_ACCOUNT_ID), how='left')

df_UnmatchedAccount_Rs = df_UnmatchedAccount_Rs.withColumnRenamed('service_province_cl','service_province_cl_service_rogers').withColumnRenamed('internet_flag','internet_flag_rogers').withColumnRenamed('wireline_phone_flag','wireline_phone_flag_rogers').select('ROGERS_ECID','ROGERS_ACCOUNT_ID','ROGERS_ACCOUNT_TYPE','internet_flag_rogers','wireline_phone_flag_rogers','service_province_cl_service_rogers')

# df_UnmatchedAccount_Rs.toPandas()

# COMMAND ----------

# join on contact table for mailing address - rogers

cl_rogers_contact_df = cl_rogers_contact_df.select('rcis_id_cl','fa_id_cl','Province_Cleansed')
df_UnmatchedAccount_Rs_Rm = df_UnmatchedAccount_Rs.join(cl_rogers_contact_df,(cl_rogers_contact_df.rcis_id_cl == df_UnmatchedAccount_Rs.ROGERS_ECID) & (cl_rogers_contact_df.fa_id_cl == df_UnmatchedAccount_Rs.ROGERS_ACCOUNT_ID), how='left')
df_UnmatchedAccount_Rs_Rm = df_UnmatchedAccount_Rs_Rm.withColumnRenamed('Province_Cleansed','Province_Cleansed_mailing_rogers').drop('rcis_id_cl','fa_id_cl','internet_flag_rogers','wireline_phone_flag_rogers')
# df_UnmatchedAccount_Rs_Rm.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Shaw' province

# COMMAND ----------

# join on wireline tables for service address - shaw

df_UnmatchedAccount_Ss = filtered_AccountMatchedRuleset.join(cl_shaw_consumer_wireline_account_df, (cl_shaw_consumer_wireline_account_df.rcis_id_cl == filtered_AccountMatchedRuleset.SHAW_MASTER_PARTY_ID) & (cl_shaw_consumer_wireline_account_df.fa_id_cl == filtered_AccountMatchedRuleset.SHAW_ACCOUNT_ID), how='left')
df_UnmatchedAccount_Ss = df_UnmatchedAccount_Ss.withColumnRenamed('service_province_cl','service_province_cl_service_shaw').withColumnRenamed('internet_flag','internet_flag_shaw').withColumnRenamed('wireline_phone_flag','wireline_phone_flag_shaw').withColumnRenamed('source_system','source_system_shaw')
df_UnmatchedAccount_Ss = df_UnmatchedAccount_Ss.select('SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID','SHAW_ACCOUNT_TYPE','service_province_cl_service_shaw')
df_UnmatchedAccount_Ss.toPandas()

# COMMAND ----------

# join on contact table for mailing address - shaw

cl_shaw_consumer_contact_df = cl_shaw_consumer_contact_df.select('rcis_id_cl','fa_id_cl','Province_Cleansed')
df_UnmatchedAccount_Ss_Sm = df_UnmatchedAccount_Ss.join(cl_shaw_consumer_contact_df,(cl_shaw_consumer_contact_df.rcis_id_cl == df_UnmatchedAccount_Ss.SHAW_MASTER_PARTY_ID) & (cl_shaw_consumer_contact_df.fa_id_cl == df_UnmatchedAccount_Ss.SHAW_ACCOUNT_ID), how='left')
df_UnmatchedAccount_Ss_Sm = df_UnmatchedAccount_Ss_Sm.withColumnRenamed('Province_Cleansed','Province_Cleansed_mailing_shaw').drop('rcis_id_cl','fa_id_cl')
df_UnmatchedAccount_Ss_Sm.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply rule 1 - Service address > mailing address

# COMMAND ----------

# Rogers
# 1. when service address is NOT NULL, then assign service address to column 'rogers_province'
# 2. when service address is NULL and mailing address NOT NULL, then assign mailling address to column 'rogers_province'
# 3. when both service and mailing are NULL, assign NULL

conditions_rogers = when(col('service_province_cl_service_rogers').isNotNull(), df_UnmatchedAccount_Rs_Rm.service_province_cl_service_rogers).when((col('service_province_cl_service_rogers').isNull()) & (col('Province_Cleansed_mailing_rogers').isNotNull()), df_UnmatchedAccount_Rs_Rm.Province_Cleansed_mailing_rogers).when((col('service_province_cl_service_rogers').isNull()) & (col('Province_Cleansed_mailing_rogers').isNull()), df_UnmatchedAccount_Rs_Rm.service_province_cl_service_rogers)

df_UnmatchedAccount_Rs_Rm = df_UnmatchedAccount_Rs_Rm.withColumn('rogers_province', conditions_rogers).drop('service_province_cl_service_rogers','Province_Cleansed_mailing_rogers')

# Shaw
# 1. when service address is NOT NULL, then assign service address to column 'shaw_province'
# 2. when service address is NULL and mailing address NOT NULL, then assign mailling address to column 'rogers_province'
# 3. when both service and mailing are NULL, assign NULL

conditions_shaw = when(col('service_province_cl_service_shaw').isNotNull(), df_UnmatchedAccount_Ss_Sm.service_province_cl_service_shaw).when((col('service_province_cl_service_shaw').isNull()) & (col('Province_Cleansed_mailing_shaw').isNotNull()), df_UnmatchedAccount_Ss_Sm.Province_Cleansed_mailing_shaw).when((col('service_province_cl_service_shaw').isNull()) & (col('Province_Cleansed_mailing_shaw').isNull()), df_UnmatchedAccount_Ss_Sm.service_province_cl_service_shaw)

df_UnmatchedAccount_Ss_Sm = df_UnmatchedAccount_Ss_Sm.withColumn('shaw_province', conditions_shaw).drop('service_province_cl_service_shaw','Province_Cleansed_mailing_shaw')


# COMMAND ----------

# convert all province columns to lower cases

df_UnmatchedAccount_Rs_Rm = df_UnmatchedAccount_Rs_Rm.withColumn('rogers_province', lower(col('rogers_province')))
df_UnmatchedAccount_Ss_Sm = df_UnmatchedAccount_Ss_Sm.withColumn('shaw_province', lower(col('shaw_province')))

# COMMAND ----------

df_UnmatchedAccount_Rs_Rm.toPandas()

# COMMAND ----------

df_UnmatchedAccount_Ss_Sm.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply rule 2 - province order

# COMMAND ----------

# prioritize West to East (BC -> AB -> SK -> MB -> ON -> QC -> ALT -> NWT)

#Rules
# qc == pq --> 6
# Shaw contact table includes US states --> 99
# Rogers contact table includes not cleansed data --> 99
# 'alt' includes 'ns','pe','nb', 'nl' --> 7
# 'nwt' includes 'nt','yt','nu' --> 8

# COMMAND ----------

#Assign number to Shaw
df_UnmatchedAccount_Ss_Sm = df_UnmatchedAccount_Ss_Sm.withColumn('shaw_province_number',
                               when(col('shaw_province') == 'bc', 1).when(col('shaw_province') == 'ab', 2).when(col('shaw_province') == 'sk', 3).when(col('shaw_province') == 'mb', 4).when(col('shaw_province') == 'on', 5).when(col('shaw_province') == 'qc', 6).when(col('shaw_province') == 'pq', 6).when(col('shaw_province') == 'nl', 7).when(col('shaw_province') == 'ns', 7).when(col('shaw_province') == 'pe', 7).when(col('shaw_province') == 'nb', 7).when(col('shaw_province') == 'nl', 8).when(col('shaw_province') == 'nt', 8).when(col('shaw_province') == 'yt', 8).when(col('shaw_province') == 'nu', 8).when(col('shaw_province').isNull(), None).otherwise(99))

# COMMAND ----------

#Assign number to Rogers
df_UnmatchedAccount_Rs_Rm = df_UnmatchedAccount_Rs_Rm.withColumn('rogers_province_number',
                               when(col('rogers_province') == 'bc', 1).when(col('rogers_province') == 'ab', 2).when(col('rogers_province') == 'sk', 3).when(col('rogers_province') == 'mb', 4).when(col('rogers_province') == 'on', 5).when(col('rogers_province') == 'qc', 6).when(col('rogers_province') == 'pq', 6).when(col('rogers_province') == 'nl', 7).when(col('rogers_province') == 'ns', 7).when(col('rogers_province') == 'pe', 7).when(col('rogers_province') == 'nb', 7).when(col('rogers_province') == 'nl', 8).when(col('rogers_province') == 'nt', 8).when(col('rogers_province') == 'yt', 8).when(col('rogers_province') == 'nu', 8).when(col('rogers_province').isNull(), None).otherwise(99))

# COMMAND ----------

df_UnmatchedAccount_Rs_Rm_grouped = df_UnmatchedAccount_Rs_Rm.groupBy('ROGERS_ECID','rogers_province_number').count().sort('count')
df_UnmatchedAccount_Rs_Rm_grouped.toPandas()

# COMMAND ----------

df_UnmatchedAccount_Ss_Sm_grouped = df_UnmatchedAccount_Ss_Sm.groupBy('SHAW_MASTER_PARTY_ID','shaw_province_number').count().sort('count')
df_UnmatchedAccount_Ss_Sm_grouped.toPandas()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Grouped province stats

# COMMAND ----------

#Shaw
df_final_province_shaw = df_UnmatchedAccount_Ss_Sm_grouped.groupBy('SHAW_MASTER_PARTY_ID').agg(min('shaw_province_number').alias('Shaw_province_final'))
df_final_province_shaw.toPandas()

# COMMAND ----------

#Rogers
df_final_province_rogers = df_UnmatchedAccount_Rs_Rm_grouped.groupBy('ROGERS_ECID').agg(min('rogers_province_number').alias('Rogers_province_final'))
df_final_province_rogers.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #Join province & Rogers Wireless & Shaw Wireline

# COMMAND ----------

#Shaw 
df_final_joined_shaw = df_amr_joined_shaw_4.join(df_final_province_shaw, 'SHAW_MASTER_PARTY_ID', 'left')
df_final_joined_shaw.toPandas()

# COMMAND ----------

#Rogers
df_final_joined_rogers = df_amr_joined_rogers_4.join(df_final_province_rogers, 'ROGERS_ECID', 'left')
df_final_joined_rogers.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rogers province final stats

# COMMAND ----------

df_final_joined_rogers.groupBy('Rogers_final').pivot('Rogers_province_final').count().toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Shaw province final stats

# COMMAND ----------

df_final_joined_shaw.groupBy('Shaw_final').pivot('Shaw_province_final').count().toPandas()
