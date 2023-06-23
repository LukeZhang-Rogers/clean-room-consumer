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

AccountMatchedRuleset = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration4/AccountMatchedRuleset")
MatchedEntity = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration4/MatchedEntity")


# COMMAND ----------

# MAGIC %md
# MAGIC #EDA

# COMMAND ----------

#AccountMatchedRuleset counts
print('AccountMatchedRuleset_total_count = ',AccountMatchedRuleset.count())
print('AccountMatchedRuleset_distinct_pair_count = ',AccountMatchedRuleset.select('ROGERS_ECID','SHAW_MASTER_PARTY_ID').distinct().count())


# COMMAND ----------

#Account_Matched_Ruleset table

#filter Matched and Rogers customer ID not NULL
filtered_AccountMatchedRuleset = AccountMatchedRuleset.filter(AccountMatchedRuleset.ROGERS_ECID.isNotNull() & (col('RULESET_TYPE') == 'DETERMINISTIC'))
print('filtered_AccountMatchedRuleset = ',filtered_AccountMatchedRuleset.count())

#distinct pairs only
deter_distinct_AccountMatchedRuleset = filtered_AccountMatchedRuleset.select('ROGERS_ECID','SHAW_MASTER_PARTY_ID').distinct()
print('deter_AccountMatchedRuleset_distinct = ',deter_distinct_AccountMatchedRuleset.count())

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

#Join Rogers
#from 'filtered_AccountMatchedRuleset'
#Join 'cl_rogers_wireless_account_df' & 'cl_rogers_wireline_account_df_active'

df_amr_joined_rogers_ws_wl = filtered_AccountMatchedRuleset.join(cl_rogers_wireless_account_df_active, (filtered_AccountMatchedRuleset.ROGERS_ECID == cl_rogers_wireless_account_df_active.rcis_id_cl) & (filtered_AccountMatchedRuleset.ROGERS_ACCOUNT_ID == cl_rogers_wireless_account_df_active.fa_id_cl), 'left').join(cl_rogers_wireline_account_df_active, (filtered_AccountMatchedRuleset.ROGERS_ECID == cl_rogers_wireline_account_df_active.rcis_id_cl) & (filtered_AccountMatchedRuleset.ROGERS_ACCOUNT_ID == cl_rogers_wireline_account_df_active.fa_id_cl), 'left')

df_amr_joined_rogers_ws_wl = df_amr_joined_rogers_ws_wl.select('ROGERS_ECID','ROGERS_ACCOUNT_ID','ROGERS_ACCOUNT_TYPE','service_provider','SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID','SHAW_ACCOUNT_TYPE')
df_amr_joined_rogers_ws_wl.toPandas()

# COMMAND ----------

#Join Shaw
#from 'df_amr_joined_rogers_ws_wl'
#join 'cl_shaw_consumer_wireless_account_df_active' + 'cl_shaw_consumer_wireline_account_df_active'

df_amr_joined_both = df_amr_joined_rogers_ws_wl.join(cl_shaw_consumer_wireless_account_df_active, (df_amr_joined_rogers_ws_wl.SHAW_MASTER_PARTY_ID == cl_shaw_consumer_wireless_account_df_active.rcis_id_cl) & (df_amr_joined_rogers_ws_wl.SHAW_ACCOUNT_ID == cl_shaw_consumer_wireless_account_df_active.fa_id_cl), 'left').join(cl_shaw_consumer_wireline_account_df_active, (df_amr_joined_rogers_ws_wl.SHAW_MASTER_PARTY_ID == cl_shaw_consumer_wireline_account_df_active.rcis_id_cl) & (df_amr_joined_rogers_ws_wl.SHAW_ACCOUNT_ID == cl_shaw_consumer_wireline_account_df_active.fa_id_cl), 'left')

df_amr_joined_both = df_amr_joined_both.drop('rcis_id_cl','fa_id_cl')
df_amr_joined_both.toPandas()


# COMMAND ----------

print('df_amr_joined_rogers_joined_shaw_total = ',df_amr_joined_both.count())
print('df_amr_joined_rogers_joined_shaw_distinctpair = ',df_amr_joined_both.select('ROGERS_ECID','SHAW_MASTER_PARTY_ID').distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Set flags

# COMMAND ----------

df_amr_joined_both.select('SHAW_DIRECT_FLAG').distinct().toPandas()

# COMMAND ----------

#Set TV flag for Shaw CH

df_amr_joined_both_1 = df_amr_joined_both.withColumn('BlueCurve_and_Legacy_TV_Flag',when((col('source_system').isNotNull()), 'Y').otherwise(None))
df_amr_joined_both_1 = df_amr_joined_both_1.withColumn('SHAW_DIRECT_FLAG', when(col('SHAW_DIRECT_FLAG') == 'Y',1).when(col('SHAW_DIRECT_FLAG') == 'N',0).otherwise(None))
df_amr_joined_both_1 = df_amr_joined_both_1.drop('VIDEO_DELIVERY_TYPE','video_flag','source_system')

#Combine Shaw CH flags

df_amr_joined_both_1 = df_amr_joined_both_1.withColumn('SHAW_CH', when((col('WIRELINE_PHONE_FLAG') == 'Y') | (col('INTERNET_FLAG') == 'Y') | (col('BlueCurve_and_Legacy_TV_Flag') == 'Y'), 1).when((col('WIRELINE_PHONE_FLAG') == 'N') & (col('INTERNET_FLAG') == 'N') & (col('BlueCurve_and_Legacy_TV_Flag') == 'N'), 0).otherwise(None)).drop('INTERNET_FLAG','WIRELINE_PHONE_FLAG','BlueCurve_and_Legacy_TV_Flag')
df_amr_joined_both_1.toPandas()

# COMMAND ----------

#Convert Rogers' service_provider to 2 columns 

df_amr_joined_both_2 = df_amr_joined_both_1.withColumn('Wireless_Rogers',when((col('service_provider') == 'Wireless Rogers'), 1).when((col('service_provider') == 'Wireless Fido'), 0).otherwise(None)).withColumn('Wireless_Fido',when((col('service_provider') == 'Wireless Fido'), 1).when((col('service_provider') == 'Wireless Rogers'), 0).otherwise(None))
df_amr_joined_both_2.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Total for Rogers/Shaw separately

# COMMAND ----------

#Total distinct customerID counts for Rogers and Shaw
print(df_amr_joined_both_2.select('ROGERS_ECID').distinct().count())
print(df_amr_joined_both_2.select('SHAW_MASTER_PARTY_ID').distinct().count())

# COMMAND ----------

df_amr_joined_both_3 = df_amr_joined_both_2.groupBy('ROGERS_ECID','SHAW_MASTER_PARTY_ID').agg(sum('SHAW_DIRECT_FLAG').alias('sum_SHAW_DIRECT_FLAG'),sum('SHAW_CH').alias('sum_SHAW_CH'),sum('Wireless_Rogers').alias('sum_Wireless_Rogers'),sum('Wireless_Fido').alias('sum_Wireless_Fido'))
df_amr_joined_both_3.sort('sum_SHAW_DIRECT_FLAG', ascending =False).toPandas()

# COMMAND ----------

#to get final shaw scenarios

import pyspark.sql.functions as F
cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
 
df_amr_joined_both_4 = df_amr_joined_both_3.groupBy('ROGERS_ECID','SHAW_MASTER_PARTY_ID').agg(
    cnt_cond(((col('sum_SHAW_DIRECT_FLAG') == 0) & (col('sum_SHAW_CH') == 0)) | (col('sum_SHAW_DIRECT_FLAG').isNull() & col('sum_SHAW_CH').isNull())).alias('Neither_S'),
    cnt_cond(((col('sum_SHAW_DIRECT_FLAG') >= 1) & (col('sum_SHAW_CH') == 0)) | ((col('sum_SHAW_DIRECT_FLAG') >= 1) & col('sum_SHAW_CH').isNull())).alias('Direct_only'),
    cnt_cond(((col('sum_SHAW_DIRECT_FLAG') == 0) & (col('sum_SHAW_CH') >= 1)) | (col('sum_SHAW_DIRECT_FLAG').isNull() & (col('sum_SHAW_CH') >= 1))).alias('CH_only'),
    cnt_cond((col('sum_SHAW_DIRECT_FLAG') >= 1) & (col('sum_SHAW_CH') >= 1)).alias('Both_S'))

df_amr_joined_both_4.toPandas()

# COMMAND ----------

df_amr_joined_both_5 = df_amr_joined_both_4.withColumn('Shaw_final',when(col('Neither_S') == 1, 'Neither_S').when(col('Direct_only') == 1, 'Direct_only').when(col('CH_only') == 1, 'CH_only').when(col('Both_S') == 1, 'Both_S'))
df_amr_joined_both_5 = df_amr_joined_both_5.select('ROGERS_ECID','SHAW_MASTER_PARTY_ID','Shaw_final')
df_amr_joined_both_5.toPandas()

# COMMAND ----------

#to get final rogers scenario

df_amr_joined_both_6 = df_amr_joined_both_3.groupBy('ROGERS_ECID','SHAW_MASTER_PARTY_ID').agg(
    cnt_cond(((col('sum_Wireless_Rogers') == 0) & (col('sum_Wireless_Fido') == 0)) | (col('sum_Wireless_Rogers').isNull() & col('sum_Wireless_Fido').isNull())).alias('Neither_R'),
    cnt_cond(((col('sum_Wireless_Rogers') >= 1) & (col('sum_Wireless_Fido') == 0)) | ((col('sum_Wireless_Rogers') >= 1) & col('sum_Wireless_Fido').isNull())).alias('Fido_only'),
    cnt_cond(((col('sum_Wireless_Rogers') == 0) & (col('sum_Wireless_Fido') >= 1)) | (col('sum_Wireless_Rogers').isNull() & (col('sum_Wireless_Fido') >= 1))).alias('Rogers_only'),
    cnt_cond((col('sum_Wireless_Rogers') >= 1) & (col('sum_Wireless_Fido') >= 1)).alias('Both_R'))


df_amr_joined_both_6.toPandas()

# COMMAND ----------

df_amr_joined_both_7 = df_amr_joined_both_6.withColumn('Rogers_final',when(col('Neither_R') == 1, 'Neither_R').when(col('Fido_only') == 1, 'Fido_only').when(col('Rogers_only') == 1, 'Rogers_only').when(col('Both_R') == 1, 'Both_R'))
df_amr_joined_both_7 = df_amr_joined_both_7.select('ROGERS_ECID','SHAW_MASTER_PARTY_ID','Rogers_final')
df_amr_joined_both_7.toPandas()

# COMMAND ----------

#Join final rogers and final shaw flags

df_amr_joined_rogers_joined_shaw_final = df_amr_joined_both_7.join(df_amr_joined_both_5, on = ['ROGERS_ECID','SHAW_MASTER_PARTY_ID'], how = 'inner')
df_amr_joined_rogers_joined_shaw_final.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Group by Rogers

# COMMAND ----------

df_groupBy_Rogers = df_amr_joined_rogers_joined_shaw_final.groupBy('ROGERS_ECID','Rogers_final','Shaw_final').count()
df_groupBy_Rogers.toPandas()

# COMMAND ----------

df_groupBy_Rogers = df_groupBy_Rogers.select('ROGERS_ECID','Rogers_final','Shaw_final')

df_groupBy_Rogers = df_groupBy_Rogers.withColumn('Rogers_final_num', when(col('Rogers_final') == 'Neither_R', 0).when(col('Rogers_final') == 'Both_R', 100).when(col('Rogers_final') == 'Fido_only', 50).when(col('Rogers_final') == 'Rogers_only', 6))
df_groupBy_Rogers = df_groupBy_Rogers.withColumn('Shaw_final_num', when(col('Shaw_final') == 'Neither_S', 0).when(col('Shaw_final') == 'Both_S', 100).when(col('Shaw_final') == 'Direct_only', 50).when(col('Shaw_final') == 'CH_only', 6))

df_Rogers_agg = df_groupBy_Rogers.select('ROGERS_ECID','Rogers_final_num','Shaw_final_num')
df_Rogers_agg.toPandas()


# COMMAND ----------

#Summing the distinct values for each ROGERS_ECID

df_Rogers_agg_final = df_Rogers_agg.groupBy('ROGERS_ECID').agg(sum_distinct('Rogers_final_num').alias('sum_R'),sum_distinct('Shaw_final_num').alias('sum_S'))
df_Rogers_agg_final.toPandas()

# COMMAND ----------

#Assign numbers back to texts

df_Rogers_agg_final = df_Rogers_agg_final.withColumn('Rogers_final_new', when(col('sum_R')>=56,'Both_R').when(col('sum_R')==0,'Neither_R').when(col('sum_R')==6,'Rogers_only').when(col('sum_R')==50,'Fido_only'))
df_Rogers_agg_final = df_Rogers_agg_final.withColumn('Shaw_final_new', when(col('sum_S')>=56,'Both_S').when(col('sum_S')==0,'Neither_S').when(col('sum_S')==6,'CH_only').when(col('sum_S')==50,'Direct_only'))
df_Rogers_agg_final.toPandas()

# COMMAND ----------

df_Rogers_agg_final = df_Rogers_agg_final.select('ROGERS_ECID','Rogers_final_new','Shaw_final_new')
df_Rogers_agg_final.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Group by Shaw

# COMMAND ----------

df_groupBy_Shaw = df_amr_joined_rogers_joined_shaw_final.groupBy('SHAW_MASTER_PARTY_ID','Rogers_final','Shaw_final').count()
df_groupBy_Shaw.toPandas()

# COMMAND ----------

df_groupBy_Shaw = df_groupBy_Shaw.select('SHAW_MASTER_PARTY_ID','Rogers_final','Shaw_final')

df_groupBy_Shaw = df_groupBy_Shaw.withColumn('Rogers_final_num', when(col('Rogers_final') == 'Neither_R', 0).when(col('Rogers_final') == 'Both_R', 100).when(col('Rogers_final') == 'Fido_only', 50).when(col('Rogers_final') == 'Rogers_only', 6))
df_groupBy_Shaw = df_groupBy_Shaw.withColumn('Shaw_final_num', when(col('Shaw_final') == 'Neither_S', 0).when(col('Shaw_final') == 'Both_S', 100).when(col('Shaw_final') == 'Direct_only', 50).when(col('Shaw_final') == 'CH_only', 6))

df_Shaw_agg = df_groupBy_Shaw.select('SHAW_MASTER_PARTY_ID','Rogers_final_num','Shaw_final_num')
df_Shaw_agg.toPandas()


# COMMAND ----------

#Summing the distinct values for each ROGERS_ECID

df_Shaw_agg_final = df_Shaw_agg.groupBy('SHAW_MASTER_PARTY_ID').agg(sum_distinct('Rogers_final_num').alias('sum_R'),sum_distinct('Shaw_final_num').alias('sum_S'))
df_Shaw_agg_final.toPandas()

# COMMAND ----------

#Assign numbers back to texts

df_Shaw_agg_final = df_Shaw_agg_final.withColumn('Rogers_final_new', when(col('sum_R')>=56,'Both_R').when(col('sum_R')==0,'Neither_R').when(col('sum_R')==6,'Rogers_only').when(col('sum_R')==50,'Fido_only'))
df_Shaw_agg_final = df_Shaw_agg_final.withColumn('Shaw_final_new', when(col('sum_S')>=56,'Both_S').when(col('sum_S')==0,'Neither_S').when(col('sum_S')==6,'CH_only').when(col('sum_S')==50,'Direct_only'))
df_Shaw_agg_final.toPandas()

# COMMAND ----------

df_Shaw_agg_final = df_Shaw_agg_final.select('SHAW_MASTER_PARTY_ID','Rogers_final_new','Shaw_final_new')
df_Shaw_agg_final.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #Province breakdown

# COMMAND ----------

#Rogers Wireline table => for service address

# Rogers active accounts only
rogers_active_account_list = [2,3,4,7,'O','S','T']
cl_rogers_wireline_account_df_active = cl_rogers_wireline_account_df.filter(cl_rogers_wireline_account_df.account_status.isin(rogers_active_account_list))
cl_rogers_wireline_account_df_active.select('rcis_id_cl','fa_id_cl','account_status','service_provider').toPandas()

# COMMAND ----------

# filtered_AccountMatchedRuleset join on wireline tables for service address - rogers

df_MatchedAccount = filtered_AccountMatchedRuleset.join(cl_rogers_wireline_account_df_active, (cl_rogers_wireline_account_df_active.rcis_id_cl == filtered_AccountMatchedRuleset.ROGERS_ECID) & (cl_rogers_wireline_account_df_active.fa_id_cl == filtered_AccountMatchedRuleset.ROGERS_ACCOUNT_ID), how='left')

df_MatchedAccount = df_MatchedAccount.withColumnRenamed('service_province_cl','service_province_cl_service_rogers')
df_MatchedAccount = df_MatchedAccount.withColumnRenamed('internet_flag','internet_flag_rogers')
df_MatchedAccount = df_MatchedAccount.withColumnRenamed('wireline_phone_flag','wireline_phone_flag_rogers')

df_MatchedAccount = df_MatchedAccount.select('ROGERS_ECID','ROGERS_ACCOUNT_ID','ROGERS_ACCOUNT_TYPE','SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID','internet_flag_rogers','wireline_phone_flag_rogers','SHAW_ACCOUNT_TYPE','service_province_cl_service_rogers')

# COMMAND ----------

# join on wireline tables for service address - shaw

df_MatchedAccount = df_MatchedAccount.join(cl_shaw_consumer_wireline_account_df, (cl_shaw_consumer_wireline_account_df.rcis_id_cl == df_MatchedAccount.SHAW_MASTER_PARTY_ID) & (cl_shaw_consumer_wireline_account_df.fa_id_cl == df_MatchedAccount.SHAW_ACCOUNT_ID), how='left')
df_MatchedAccount = df_MatchedAccount.withColumnRenamed('service_province_cl','service_province_cl_service_shaw')
df_MatchedAccount = df_MatchedAccount.withColumnRenamed('internet_flag','internet_flag_shaw')
df_MatchedAccount = df_MatchedAccount.withColumnRenamed('wireline_phone_flag','wireline_phone_flag_shaw')
df_MatchedAccount = df_MatchedAccount.withColumnRenamed('source_system','source_system_shaw')
df_MatchedAccount = df_MatchedAccount.select('ROGERS_ECID','ROGERS_ACCOUNT_ID','ROGERS_ACCOUNT_TYPE','SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID','SHAW_ACCOUNT_TYPE','service_province_cl_service_rogers','service_province_cl_service_shaw')

# COMMAND ----------

# join on contact table for mailing address - rogers

cl_rogers_contact_df = cl_rogers_contact_df.select('rcis_id_cl','fa_id_cl','Province_Cleansed')
df_MatchedAccount = df_MatchedAccount.join(cl_rogers_contact_df,(cl_rogers_contact_df.rcis_id_cl == df_MatchedAccount.ROGERS_ECID) & (cl_rogers_contact_df.fa_id_cl == df_MatchedAccount.ROGERS_ACCOUNT_ID), how='left')
df_MatchedAccount = df_MatchedAccount.withColumnRenamed('Province_Cleansed','Province_Cleansed_mailing_rogers')
df_MatchedAccount = df_MatchedAccount.select('ROGERS_ECID','ROGERS_ACCOUNT_ID','ROGERS_ACCOUNT_TYPE','SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID','SHAW_ACCOUNT_TYPE','service_province_cl_service_rogers','service_province_cl_service_shaw','Province_Cleansed_mailing_rogers')

# COMMAND ----------

# join on contact table for mailing address - shaw

cl_shaw_consumer_contact_df = cl_shaw_consumer_contact_df.select('rcis_id_cl','fa_id_cl','Province_Cleansed')
df_MatchedAccount = df_MatchedAccount.join(cl_shaw_consumer_contact_df,(cl_shaw_consumer_contact_df.rcis_id_cl == df_MatchedAccount.SHAW_MASTER_PARTY_ID) & (cl_shaw_consumer_contact_df.fa_id_cl == df_MatchedAccount.SHAW_ACCOUNT_ID), how='left')
df_MatchedAccount = df_MatchedAccount.withColumnRenamed('Province_Cleansed','Province_Cleansed_mailing_shaw')
df_MatchedAccount_1 = df_MatchedAccount.select('ROGERS_ECID','ROGERS_ACCOUNT_ID','ROGERS_ACCOUNT_TYPE','SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID','SHAW_ACCOUNT_TYPE','service_province_cl_service_rogers','service_province_cl_service_shaw','Province_Cleansed_mailing_rogers','Province_Cleansed_mailing_shaw')

# COMMAND ----------

df_MatchedAccount_1.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply rule 1 - Service address > mailing address

# COMMAND ----------

# Rogers
# 1. when service address is NOT NULL, then assign service address to column 'rogers_province'
# 2. when service address is NULL and mailing address NOT NULL, then assign mailling address to column 'rogers_province'
# 3. when both service and mailing are NULL, assign NULL

conditions_rogers = when(col('service_province_cl_service_rogers').isNotNull(), df_MatchedAccount_1.service_province_cl_service_rogers).when((col('service_province_cl_service_rogers').isNull()) & (col('Province_Cleansed_mailing_rogers').isNotNull()), df_MatchedAccount_1.Province_Cleansed_mailing_rogers).when((col('service_province_cl_service_rogers').isNull()) & (col('Province_Cleansed_mailing_rogers').isNull()), df_MatchedAccount_1.service_province_cl_service_rogers)

df_MatchedAccount_1 = df_MatchedAccount_1.withColumn('rogers_province', conditions_rogers)

# Shaw
# 1. when service address is NOT NULL, then assign service address to column 'shaw_province'
# 2. when service address is NULL and mailing address NOT NULL, then assign mailling address to column 'rogers_province'
# 3. when both service and mailing are NULL, assign NULL

conditions_shaw = when(col('service_province_cl_service_shaw').isNotNull(), df_MatchedAccount_1.service_province_cl_service_shaw).when((col('service_province_cl_service_shaw').isNull()) & (col('Province_Cleansed_mailing_shaw').isNotNull()), df_MatchedAccount_1.Province_Cleansed_mailing_shaw).when((col('service_province_cl_service_shaw').isNull()) & (col('Province_Cleansed_mailing_shaw').isNull()), df_MatchedAccount_1.service_province_cl_service_shaw)

df_MatchedAccount_1 = df_MatchedAccount_1.withColumn('shaw_province', conditions_shaw)

# reorder columns
df_MatchedAccount_1 = df_MatchedAccount_1.select('ROGERS_ECID','ROGERS_ACCOUNT_ID','ROGERS_ACCOUNT_TYPE','SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID','SHAW_ACCOUNT_TYPE','rogers_province','shaw_province')


# COMMAND ----------

# convert 'Province_Cleansed_mailing_shaw' to lower cases

df_MatchedAccount_1 = df_MatchedAccount_1.withColumn('rogers_province', lower(col('rogers_province')))
df_MatchedAccount_1 = df_MatchedAccount_1.withColumn('shaw_province', lower(col('shaw_province')))

df_MatchedAccount_1.toPandas()

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

df_MatchedAccount_2 = df_MatchedAccount_1.withColumn('shaw_province_number',
                               when(col('shaw_province') == 'bc', 1).when(col('shaw_province') == 'ab', 2).when(col('shaw_province') == 'sk', 3).when(col('shaw_province') == 'mb', 4).when(col('shaw_province') == 'on', 5).when(col('shaw_province') == 'qc', 6).when(col('shaw_province') == 'pq', 6).when(col('shaw_province') == 'nl', 7).when(col('shaw_province') == 'ns', 7).when(col('shaw_province') == 'pe', 7).when(col('shaw_province') == 'nb', 7).when(col('shaw_province') == 'nl', 8).when(col('shaw_province') == 'nt', 8).when(col('shaw_province') == 'yt', 8).when(col('shaw_province') == 'nu', 8).when(col('shaw_province').isNull(), None).otherwise(99))

# COMMAND ----------

df_MatchedAccount_2 = df_MatchedAccount_2.withColumn('rogers_province_number',
                               when(col('rogers_province') == 'bc', 1).when(col('rogers_province') == 'ab', 2).when(col('rogers_province') == 'sk', 3).when(col('rogers_province') == 'mb', 4).when(col('rogers_province') == 'on', 5).when(col('rogers_province') == 'qc', 6).when(col('rogers_province') == 'pq', 6).when(col('rogers_province') == 'nl', 7).when(col('rogers_province') == 'ns', 7).when(col('rogers_province') == 'pe', 7).when(col('rogers_province') == 'nb', 7).when(col('rogers_province') == 'nl', 8).when(col('rogers_province') == 'nt', 8).when(col('rogers_province') == 'yt', 8).when(col('rogers_province') == 'nu', 8).when(col('rogers_province').isNull(), None).otherwise(99))

# COMMAND ----------

df_MatchedAccount_2.toPandas()

# COMMAND ----------

df_MatchedAccount_3_grouped = df_MatchedAccount_2.groupBy('ROGERS_ECID','SHAW_MASTER_PARTY_ID','shaw_province_number','rogers_province_number').count().sort('count')
df_MatchedAccount_3_grouped.toPandas()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Final province stats

# COMMAND ----------

df_final_province = df_MatchedAccount_3_grouped.groupBy('ROGERS_ECID','SHAW_MASTER_PARTY_ID').agg(min('rogers_province_number').alias('Rogers_province_final'),min('shaw_province_number').alias('Shaw_province_final'))
df_final_province.toPandas()

# COMMAND ----------

df_province_Rogers = df_final_province.groupBy('ROGERS_ECID').agg(min('Rogers_province_final').alias('Rogers_province_final'), min('Shaw_province_final').alias('Shaw_province_final'))
df_province_Rogers.toPandas()

# COMMAND ----------

df_province_Shaw = df_final_province.groupBy('SHAW_MASTER_PARTY_ID').agg(min('Rogers_province_final').alias('Rogers_province_final'), min('Shaw_province_final').alias('Shaw_province_final'))
df_province_Shaw.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #Join province & Rogers Wireless & Shaw Wireline

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rogers Province

# COMMAND ----------

df_joined_Rogers = df_Rogers_agg_final.join(df_province_Rogers, on = ['ROGERS_ECID'], how = 'left')
df_joined_Rogers.toPandas()

# COMMAND ----------

df_joined_Rogers.groupBy('Rogers_final_new','Shaw_final_new').count().sort('Rogers_final_new').toPandas()

# COMMAND ----------

df_joined_Rogers.groupBy('Rogers_final_new','Shaw_final_new').pivot('Rogers_province_final').count().sort('Rogers_final_new').toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Shaw province

# COMMAND ----------

df_joined_Shaw = df_Shaw_agg_final.join(df_province_Shaw, on = ['SHAW_MASTER_PARTY_ID'], how = 'left')
df_joined_Shaw.toPandas()

# COMMAND ----------

df_joined_Shaw.groupBy('Rogers_final_new','Shaw_final_new').pivot('Shaw_province_final').count().sort('Shaw_final_new').toPandas()

# COMMAND ----------


