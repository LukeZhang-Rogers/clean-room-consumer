# Databricks notebook source
# MAGIC %md
# MAGIC #Load data

# COMMAND ----------

from pyspark.sql.functions import *

#Cleaned Data paths

# cl_shaw_consumer_contact          = '/mnt/development/Processed_temp/Shaw/Contact'
cl_shaw_consumer_contact          = '/mnt/development/Processed/Shaw/Contact'
cl_shaw_consumer_wireline_account = '/mnt/development/Processed/Shaw/WirelineAccount'
cl_shaw_consumer_wireless_account = '/mnt/development/Processed/Shaw/WirelessAccount'

cl_rogers_contact                 = '/mnt/development/Processed/Rogers/Contact'
cl_rogers_wireline_account        = '/mnt/development/Processed/Rogers/WirelineAccount'
cl_rogers_wireless_account        = '/mnt/development/Processed/Rogers/WirelessAccount'

#Load dataframes
cl_shaw_consumer_contact_df                = spark.read.format("parquet").load(cl_shaw_consumer_contact)
cl_shaw_consumer_wireless_account_df       = spark.read.format("parquet").load(cl_shaw_consumer_wireless_account)
cl_shaw_consumer_wireline_account_df       = spark.read.format("parquet").load(cl_shaw_consumer_wireline_account)

cl_rogers_contact_df                 = spark.read.format("parquet").load(cl_rogers_contact)
cl_rogers_wireless_account_df        = spark.read.format("parquet").load(cl_rogers_wireless_account)
cl_rogers_wireline_account_df        = spark.read.format("parquet").load(cl_rogers_wireline_account)

AccountMatchedRuleset = spark.read.format("parquet").load("/mnt/development/Processed/InterimOutput/05_19/entiredataset_accountmatchedruleset_withblocking_withthreshold_on_each_rule_activeonly")
MatchedEntity = spark.read.format("parquet").load("/mnt/development/Processed/InterimOutput/05_19/entiredataset_matchedentity_withblocking_withthreshold_on_each_rule_activeonly")


# COMMAND ----------

# MAGIC %md
# MAGIC #EDA

# COMMAND ----------

MatchedEntity.select('BEST_DET_MATCH_RULESET_ID').distinct().sort('BEST_DET_MATCH_RULESET_ID', ascending=False).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Unmatched Rogers & Shaw IDs

# COMMAND ----------

#Rogers - Unmatched ROGERS_ECID

#Matched Rogers distinct customers
MatchedEntity_Matched = MatchedEntity.filter(col('DET_MATCH_IND') == 'Y')
dist_rogers_matched = MatchedEntity_Matched.select('ROGERS_ECID').distinct()

#All Rogers distinct customers
MatchedEntity_R_notnull = MatchedEntity.filter(col('ROGERS_ECID').isNotNull())
df_dist_rogers_all = MatchedEntity_R_notnull.select('ROGERS_ECID').distinct()

#Difference - unmatched
dist_rogers_unmatched = df_dist_rogers_all.subtract(dist_rogers_matched)
dist_rogers_unmatched.toPandas()

# COMMAND ----------

MatchedEntity.select('BEST_DET_MATCH_RULESET_ID').sort('BEST_DET_MATCH_RULESET_ID', ascending=False).toPandas()

# COMMAND ----------

#Shaw - Unmatched SHAW_MASTER_PARTY_ID

#Matched Rogers distinct customers
dist_shaw_matched = MatchedEntity_Matched.select('SHAW_MASTER_PARTY_ID').distinct()

#All Rogers distinct customers
MatchedEntity_S_notnull = MatchedEntity.filter(col('SHAW_MASTER_PARTY_ID').isNotNull())
df_dist_shaw_all = MatchedEntity_S_notnull.select('SHAW_MASTER_PARTY_ID').distinct()

#Difference - unmatched
dist_shaw_unmatched = df_dist_shaw_all.subtract(dist_shaw_matched)
dist_shaw_unmatched.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Active Wireless/Wireline tables

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
# MAGIC ###AMR joining Rogers/Shaw wireless & wireline tables

# COMMAND ----------

#Rogers
#from 'AccountMatchedRuleset', Join 'cl_rogers_wireless_account_df' & 'cl_rogers_wireline_account_df_active'

df_amr_joined_rogers_ws_wl = AccountMatchedRuleset.join(cl_rogers_wireless_account_df_active, (AccountMatchedRuleset.ROGERS_ECID == cl_rogers_wireless_account_df_active.rcis_id_cl) & (AccountMatchedRuleset.ROGERS_ACCOUNT_ID == cl_rogers_wireless_account_df_active.fa_id_cl), 'left').join(cl_rogers_wireline_account_df_active, (AccountMatchedRuleset.ROGERS_ECID == cl_rogers_wireline_account_df_active.rcis_id_cl) & (AccountMatchedRuleset.ROGERS_ACCOUNT_ID == cl_rogers_wireline_account_df_active.fa_id_cl), 'left')

df_amr_joined_rogers_ws_wl = df_amr_joined_rogers_ws_wl.select('ROGERS_ECID','ROGERS_ACCOUNT_ID','ROGERS_ACCOUNT_TYPE','service_provider')
df_amr_joined_rogers_ws_wl.toPandas()

# COMMAND ----------

print('df_amr_joined_rogers_ws_wl_distinct = ',df_amr_joined_rogers_ws_wl.select('ROGERS_ECID').distinct().count())


# COMMAND ----------

#Join Shaw
#from 'AccountMatchedRuleset', join 'cl_shaw_consumer_wireless_account_df_active' + 'cl_shaw_consumer_wireline_account_df_active'

df_amr_joined_shaw_ws_wl = AccountMatchedRuleset.join(cl_shaw_consumer_wireless_account_df_active, (AccountMatchedRuleset.SHAW_MASTER_PARTY_ID == cl_shaw_consumer_wireless_account_df_active.rcis_id_cl) & (AccountMatchedRuleset.SHAW_ACCOUNT_ID == cl_shaw_consumer_wireless_account_df_active.fa_id_cl), 'left').join(cl_shaw_consumer_wireline_account_df_active, (AccountMatchedRuleset.SHAW_MASTER_PARTY_ID == cl_shaw_consumer_wireline_account_df_active.rcis_id_cl) & (AccountMatchedRuleset.SHAW_ACCOUNT_ID == cl_shaw_consumer_wireline_account_df_active.fa_id_cl), 'left')

df_amr_joined_shaw_ws_wl = df_amr_joined_shaw_ws_wl.select('SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID','SHAW_ACCOUNT_TYPE','SHAW_ACCOUNT_STATUS','SHAW_DIRECT_FLAG','INTERNET_FLAG','WIRELINE_PHONE_FLAG','source_system')
df_amr_joined_shaw_ws_wl.toPandas()


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
# df_amr_joined_rogers_1.toPandas()

# COMMAND ----------

df_amr_joined_rogers_2 = df_amr_joined_rogers_1.groupBy('ROGERS_ECID').agg(sum('Wireless_Rogers').alias('sum_Wireless_Rogers'),sum('Wireless_Fido').alias('sum_Wireless_Fido'))
# df_amr_joined_rogers_2.sort('sum_Wireless_Rogers').toPandas()

# COMMAND ----------

#to get final rogers scenario

import pyspark.sql.functions as F
cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))

df_amr_joined_rogers_3 = df_amr_joined_rogers_2.groupBy('ROGERS_ECID').agg(
    cnt_cond(((col('sum_Wireless_Rogers') == 0) & (col('sum_Wireless_Fido') == 0)) | (col('sum_Wireless_Rogers').isNull() & col('sum_Wireless_Fido').isNull())).alias('Neither_R'),
    cnt_cond(((col('sum_Wireless_Rogers') >= 1) & (col('sum_Wireless_Fido') == 0)) | ((col('sum_Wireless_Rogers') >= 1) & col('sum_Wireless_Fido').isNull())).alias('Fido_only'),
    cnt_cond(((col('sum_Wireless_Rogers') == 0) & (col('sum_Wireless_Fido') >= 1)) | (col('sum_Wireless_Rogers').isNull() & (col('sum_Wireless_Fido') >= 1))).alias('Rogers_only'),
    cnt_cond((col('sum_Wireless_Rogers') >= 1) & (col('sum_Wireless_Fido') >= 1)).alias('Both_R'))

# df_amr_joined_rogers_3.toPandas()

# COMMAND ----------

df_amr_joined_rogers_4 = df_amr_joined_rogers_3.withColumn('Rogers_final',when(col('Neither_R') == 1, 'Neither_R').when(col('Fido_only') == 1, 'Fido_only').when(col('Rogers_only') == 1, 'Rogers_only').when(col('Both_R') == 1, 'Both_R'))
df_amr_joined_rogers_4 = df_amr_joined_rogers_4.select('ROGERS_ECID','Rogers_final')
# df_amr_joined_rogers_4.toPandas()

# COMMAND ----------

df_Rogers_service_final  = dist_rogers_unmatched.join(df_amr_joined_rogers_4, on = 'ROGERS_ECID', how = 'left')
df_Rogers_service_final.toPandas()

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
# df_amr_joined_shaw_1.toPandas()

# COMMAND ----------

df_amr_joined_shaw_2 = df_amr_joined_shaw_1.groupBy('SHAW_MASTER_PARTY_ID').agg(sum('SHAW_DIRECT_FLAG').alias('sum_SHAW_DIRECT_FLAG'),sum('SHAW_CH').alias('sum_SHAW_CH'))
# df_amr_joined_shaw_2.sort('sum_SHAW_CH').toPandas()

# COMMAND ----------

#to get final shaw scenario

import pyspark.sql.functions as F
cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
 
df_amr_joined_shaw_3 = df_amr_joined_shaw_2.groupBy('SHAW_MASTER_PARTY_ID').agg(
    cnt_cond(((col('sum_SHAW_DIRECT_FLAG') == 0) & (col('sum_SHAW_CH') == 0)) | (col('sum_SHAW_DIRECT_FLAG').isNull() & col('sum_SHAW_CH').isNull())).alias('Neither_S'),
    cnt_cond(((col('sum_SHAW_DIRECT_FLAG') >= 1) & (col('sum_SHAW_CH') == 0)) | ((col('sum_SHAW_DIRECT_FLAG') >= 1) & col('sum_SHAW_CH').isNull())).alias('Direct_only'),
    cnt_cond(((col('sum_SHAW_DIRECT_FLAG') == 0) & (col('sum_SHAW_CH') >= 1)) | (col('sum_SHAW_DIRECT_FLAG').isNull() & (col('sum_SHAW_CH') >= 1))).alias('CH_only'),
    cnt_cond((col('sum_SHAW_DIRECT_FLAG') >= 1) & (col('sum_SHAW_CH') >= 1)).alias('Both_S'))
# df_amr_joined_shaw_3.toPandas()

# COMMAND ----------

df_amr_joined_shaw_4 = df_amr_joined_shaw_3.withColumn('Shaw_final',when(col('Neither_S') == 1, 'Neither_S').when(col('Direct_only') == 1, 'Direct_only').when(col('CH_only') == 1, 'CH_only').when(col('Both_S') == 1, 'Both_S'))
df_amr_joined_shaw_4 = df_amr_joined_shaw_4.select('SHAW_MASTER_PARTY_ID','Shaw_final')
# df_amr_joined_shaw_4.toPandas()

# COMMAND ----------

df_Shaw_service_final  = dist_shaw_unmatched.join(df_amr_joined_shaw_4, on = 'SHAW_MASTER_PARTY_ID', how = 'left')
df_Shaw_service_final.toPandas()

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

# AccountMatchedRuleset join on wireline tables for service address - rogers

df_UnmatchedAccount_Rs = AccountMatchedRuleset.join(cl_rogers_wireline_account_df_active, (cl_rogers_wireline_account_df_active.rcis_id_cl == AccountMatchedRuleset.ROGERS_ECID) & (cl_rogers_wireline_account_df_active.fa_id_cl == AccountMatchedRuleset.ROGERS_ACCOUNT_ID), how='left')

# COMMAND ----------

df_UnmatchedAccount_Rs = df_UnmatchedAccount_Rs.withColumnRenamed('service_province_cl','service_province_cl_service_rogers').withColumnRenamed('internet_flag','internet_flag_rogers').withColumnRenamed('wireline_phone_flag','wireline_phone_flag_rogers').select('ROGERS_ECID','ROGERS_ACCOUNT_ID','ROGERS_ACCOUNT_TYPE','internet_flag_rogers','wireline_phone_flag_rogers','service_province_cl_service_rogers')

# df_UnmatchedAccount_Rs.toPandas()

# COMMAND ----------

cl_rogers_contact_df = cl_rogers_contact_df.select('rcis_id_cl','fa_id_cl','Province_Cleansed')
# cl_rogers_contact_df.toPandas()

# COMMAND ----------

# join on contact table for mailing address - rogers

df_UnmatchedAccount_Rs_Rm = df_UnmatchedAccount_Rs.join(cl_rogers_contact_df,(cl_rogers_contact_df.rcis_id_cl == df_UnmatchedAccount_Rs.ROGERS_ECID) & (cl_rogers_contact_df.fa_id_cl == df_UnmatchedAccount_Rs.ROGERS_ACCOUNT_ID), how='left')
df_UnmatchedAccount_Rs_Rm = df_UnmatchedAccount_Rs_Rm.withColumnRenamed('Province_Cleansed','Province_Cleansed_mailing_rogers').drop('rcis_id_cl','fa_id_cl','internet_flag_rogers','wireline_phone_flag_rogers')
# df_UnmatchedAccount_Rs_Rm.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Shaw' province

# COMMAND ----------

# join on wireline tables for service address - shaw

df_UnmatchedAccount_Ss = AccountMatchedRuleset.join(cl_shaw_consumer_wireline_account_df, (cl_shaw_consumer_wireline_account_df.rcis_id_cl == AccountMatchedRuleset.SHAW_MASTER_PARTY_ID) & (cl_shaw_consumer_wireline_account_df.fa_id_cl == AccountMatchedRuleset.SHAW_ACCOUNT_ID), how='left')

df_UnmatchedAccount_Ss = df_UnmatchedAccount_Ss.withColumnRenamed('service_province_cl','service_province_cl_service_shaw').withColumnRenamed('internet_flag','internet_flag_shaw').withColumnRenamed('wireline_phone_flag','wireline_phone_flag_shaw').withColumnRenamed('source_system','source_system_shaw')

df_UnmatchedAccount_Ss = df_UnmatchedAccount_Ss.select('SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID','SHAW_ACCOUNT_TYPE','service_province_cl_service_shaw')
# df_UnmatchedAccount_Ss.toPandas()

# COMMAND ----------

# join on contact table for mailing address - shaw

cl_shaw_consumer_contact_df = cl_shaw_consumer_contact_df.select('rcis_id_cl','fa_id_cl','Province_Cleansed')
df_UnmatchedAccount_Ss_Sm = df_UnmatchedAccount_Ss.join(cl_shaw_consumer_contact_df,(cl_shaw_consumer_contact_df.rcis_id_cl == df_UnmatchedAccount_Ss.SHAW_MASTER_PARTY_ID) & (cl_shaw_consumer_contact_df.fa_id_cl == df_UnmatchedAccount_Ss.SHAW_ACCOUNT_ID), how='left')
df_UnmatchedAccount_Ss_Sm = df_UnmatchedAccount_Ss_Sm.withColumnRenamed('Province_Cleansed','Province_Cleansed_mailing_shaw').drop('rcis_id_cl','fa_id_cl')
# df_UnmatchedAccount_Ss_Sm.toPandas()

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

# MAGIC %md
# MAGIC ###Rules
# MAGIC ####prioritize West to East (BC -> AB -> SK -> MB -> ON -> QC -> ALT -> NWT)
# MAGIC
# MAGIC ####qc == pq --> 6
# MAGIC ####Shaw contact table includes US states --> 99
# MAGIC ####Rogers contact table includes not cleansed data --> 99
# MAGIC ####'alt' includes 'ns','pe','nb', 'nl' --> 7
# MAGIC ####'nwt' includes 'nt','yt','nu' --> 8

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
# df_UnmatchedAccount_Rs_Rm_grouped.toPandas()

# COMMAND ----------

df_UnmatchedAccount_Ss_Sm_grouped = df_UnmatchedAccount_Ss_Sm.groupBy('SHAW_MASTER_PARTY_ID','shaw_province_number').count().sort('count')
# df_UnmatchedAccount_Ss_Sm_grouped.toPandas()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Grouped province stats

# COMMAND ----------

#Shaw
df_final_province_shaw = df_UnmatchedAccount_Ss_Sm_grouped.groupBy('SHAW_MASTER_PARTY_ID').agg(min('shaw_province_number').alias('Shaw_province_final'))
# df_final_province_shaw.toPandas()

# COMMAND ----------

#Rogers
df_final_province_rogers = df_UnmatchedAccount_Rs_Rm_grouped.groupBy('ROGERS_ECID').agg(min('rogers_province_number').alias('Rogers_province_final'))
# df_final_province_rogers.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #Join province & service tables

# COMMAND ----------

#Shaw 
df_final_joined_shaw = df_Shaw_service_final.join(df_final_province_shaw, 'SHAW_MASTER_PARTY_ID', 'left')
df_final_joined_shaw.toPandas()

# COMMAND ----------

#Rogers
df_final_joined_rogers = df_Rogers_service_final.join(df_final_province_rogers, 'ROGERS_ECID', 'left')
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
