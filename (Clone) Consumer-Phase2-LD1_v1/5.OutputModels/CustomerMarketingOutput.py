# Databricks notebook source
# MAGIC %md ## Building Customer Marketing Output Model
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC
# MAGIC This notebook is the final step and consumes all previously created datasets in the Processed Zone and Interim Consumption Zone to build the Customer Marketing Output Model (Skinny Model). The output models contains both matched and unmatched records for Rogers and only matched for Shaw. The mappings have been done as per the mapping sheet.
# MAGIC
# MAGIC
# MAGIC ### Requirements:
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC   <li>MatchedEntity dataset, ServiceabilityMatchedAccounts and Ruleset datasets available in the Interim Consumption Zone</li>
# MAGIC   <li>Shaw Wireless Account, Wireline Account, Wireline Serviceability and Wireless Services datasets are available in the Processed Zone</li>
# MAGIC
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurations

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../2.Common/ADLSDesignedPaths

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Matched Entity & Rulesets

# COMMAND ----------

matched_entity_df   = spark.read.format("parquet").load(matched_entity)

# COMMAND ----------

ruleset_df   = spark.read.format("parquet").load(ruleset)

# COMMAND ----------

ruleset_df_selected = ruleset_df.select('RULESET_ID', 'IS_CUSTOMER_MATCH', 'IS_ADDRESS_MATCH')

# COMMAND ----------

#Removing unmatched Shaw customers from the MatchedEntity table before modelling it
matched_entity_filtered = matched_entity_df.filter(col('ROGERS_ECID').isNotNull())

# COMMAND ----------

#Fetching ruleset attributes from the reference table
matched_entity_with_deter = matched_entity_filtered.join(ruleset_df_selected, (matched_entity_filtered.BEST_DET_MATCH_RULESET_ID == ruleset_df_selected.RULESET_ID), 'left').drop('RULESET_ID')

matched_entity_with_deter = matched_entity_with_deter.withColumn('DET_CUSTOMER_MATCH_FLAG', when(col('IS_CUSTOMER_MATCH') == 'TRUE', 'Y').otherwise(lit('N')))\
                                                     .withColumn('DET_ADDRESS_MATCH_FLAG', when(col('IS_ADDRESS_MATCH') == 'TRUE', 'Y').otherwise(lit('N')))\
                                                     .drop('IS_CUSTOMER_MATCH', 'IS_ADDRESS_MATCH')

# COMMAND ----------

#Fetching ruleset attributes from the reference table
matched_entity_with_fuzzy = matched_entity_with_deter.join(ruleset_df_selected, (matched_entity_with_deter.BEST_FUZZY_MATCH_RULESET_ID == ruleset_df_selected.RULESET_ID), 'left').drop('RULESET_ID')

matched_entity_with_fuzzy = matched_entity_with_fuzzy.withColumn('FUZZY_CUSTOMER_MATCH_FLAG', when(col('IS_CUSTOMER_MATCH') == 'TRUE', 'Y').otherwise(lit('N')))\
                                                     .withColumn('FUZZY_ADDRESS_MATCH_FLAG', when(col('IS_ADDRESS_MATCH') == 'TRUE', 'Y').otherwise(lit('N')))\
                                                     .drop('IS_CUSTOMER_MATCH', 'IS_ADDRESS_MATCH')

# COMMAND ----------

#Fetching ruleset attributes from the reference table
matched_entity_with_prob  = matched_entity_with_fuzzy.join(ruleset_df_selected, (matched_entity_with_fuzzy.BEST_PROB_MATCH_RULESET_ID == ruleset_df_selected.RULESET_ID), 'left').drop('RULESET_ID')
matched_entity_with_prob  = matched_entity_with_prob.withColumn('PROB_CUSTOMER_MATCH_FLAG', when(col('IS_CUSTOMER_MATCH') == 'TRUE', 'Y').otherwise(lit('N')))\
                                                    .withColumn('PROB_ADDRESS_MATCH_FLAG', when(col('IS_ADDRESS_MATCH') == 'TRUE', 'Y').otherwise(lit('N')))\
                                                    .drop('IS_CUSTOMER_MATCH', 'IS_ADDRESS_MATCH')

# COMMAND ----------

matched_entity_final = matched_entity_with_prob.select(col('ROGERS_ECID').alias('ECID'),\
                                                'SHAW_MASTER_PARTY_ID',\
                                               col('DET_MATCH_IND').alias('DET_MATCH_FLAG'),\
                                               col('BEST_DET_MATCH_RULESET_ID').alias('DET_MATCH_RULESET_ID'),\
                                               col('CUSTOMER_DET_PRIORITY').alias('DET_MATCH_PRIORITY'),\
                                               col('FUZZY_MATCH_IND').alias('FUZZY_MATCH_FLAG'),\
                                               col('BEST_FUZZY_MATCH_RULESET_ID').alias('FUZZY_MATCH_RULESET_ID'),\
                                               col('CUSTOMER_FUZZY_SCORE').alias('FUZZY_MATCH_SCORE'),\
                                               col('PROB_MATCH_IND').alias('PROB_MATCH_FLAG'),\
                                               col('BEST_PROB_MATCH_RULESET_ID').alias('PROB_MATCH_RULESET_ID'),\
                                               col('CUSTOMER_PROB_CONFIDENCE_LVL').alias('PROB_MATCH_CONFIDENCE_LVL'),\
                                               'DET_CUSTOMER_MATCH_FLAG',\
                                                'DET_ADDRESS_MATCH_FLAG',\
                                                'FUZZY_CUSTOMER_MATCH_FLAG',\
                                                'FUZZY_ADDRESS_MATCH_FLAG',\
                                                'PROB_CUSTOMER_MATCH_FLAG',\
                                                'PROB_ADDRESS_MATCH_FLAG',\
                                                'SNAPSHOT_STAMP'
                                               )

# COMMAND ----------

# MAGIC %md
# MAGIC ### ServiceabilityMatched & Shaw_WLN_Serviceability Datasets

# COMMAND ----------

shaw_wln_sa_df  =  spark.read.format("parquet").load(cl_shaw_wireline_serviceability)

# COMMAND ----------

sa_df           = spark.read.format("parquet").load(serviceability_matched)

# COMMAND ----------

sa_df_filtered  = sa_df.filter((col('SERVICEABILITY_SOURCE') == 'Shaw'))

# COMMAND ----------

sa_df_selected = sa_df_filtered.select('RCIS_ID', 'SERVICEABILITY_ID_MAILING_ADDRESS', 'SERVICEABILITY_ID_SERVICE_ADDRESS').dropDuplicates()

# COMMAND ----------

shaw_ch_sa_df = sa_df_selected.join(shaw_wln_sa_df, (sa_df_selected.SERVICEABILITY_ID_MAILING_ADDRESS == shaw_wln_sa_df.address_id) | (sa_df_selected.SERVICEABILITY_ID_SERVICE_ADDRESS == shaw_wln_sa_df.address_id), 'left')

# COMMAND ----------

#Setting flags at the Account level
shaw_ch_sa_df_temp = shaw_ch_sa_df.withColumn('INT_SERVICEABILITY_FLAG',when(col('int_serviceability_flag')=='Y', 'Y').otherwise(lit('N')))\
                                  .withColumn('HP_SERVICEABILITY_FLAG',when(col('hp_serviceability_flag')=='Y', 'Y').otherwise(lit('N')))\
                                  .withColumn('C_TV_SERVICEABILITY_TYPE',when(col('tv_service_type')=='BLUECURVE TV', 'BLUECURVE TV')\
                                                                        .when(col('tv_service_type')=='LEGACY', 'LEGACY TV')\
                                                                        .otherwise(lit(None)))\
                                  .withColumn('S_TV_SERVICEABILITY_FLAG', lit('Y'))\
                                  .withColumn('C_TV_SERVICEABILITY_FLAG', when((col('tv_service_type')=='LEGACY') | (col('tv_service_type')=='BLUECURVE TV'), 'Y').otherwise(lit('N')))

# COMMAND ----------

#Aggregated at customer level
shaw_ch_sa_df_temp_agg = shaw_ch_sa_df_temp.groupBy('RCIS_ID').agg(first("S_TV_SERVICEABILITY_FLAG").alias('S_TV_SERVICEABILITY_FLAG'),\
                                                                                      collect_set("C_TV_SERVICEABILITY_FLAG"),\
                                                                                      collect_set("C_TV_SERVICEABILITY_TYPE"),\
                                                                                      collect_set("HP_SERVICEABILITY_FLAG"),\
                                                                                      collect_set("INT_SERVICEABILITY_FLAG"),\
                                                                                      max('max_download_speed').alias('MAX_DOWN_SPEED'),\
                                                                                      max('max_upload_speed').alias('MAX_UP_SPEED'))

# COMMAND ----------

#Flags at customer level
shaw_ch_sa_df_agg = shaw_ch_sa_df_temp_agg.withColumn('INT_SERVICEABILITY_FLAG', when(array_contains(col('collect_set(INT_SERVICEABILITY_FLAG)'),"Y"), 'Y').otherwise(lit('N')))\
                                          .withColumn('HP_SERVICEABILITY_FLAG', when(array_contains(col('collect_set(HP_SERVICEABILITY_FLAG)'),"Y"), 'Y').otherwise(lit('N')))\
                                          .withColumn('C_TV_SERVICEABILITY_TYPE', when(array_contains(col('collect_set(C_TV_SERVICEABILITY_TYPE)'),"BLUECURVE TV"), 'BLUECURVE TV')\
                                                                             .when(array_contains(col('collect_set(C_TV_SERVICEABILITY_TYPE)'),"LEGACY TV"), 'LEGACY TV')\
                                                                             .otherwise(lit(None)))\
                                          .withColumn('C_TV_SERVICEABILITY_FLAG', when(array_contains(col('collect_set(C_TV_SERVICEABILITY_FLAG)'),"Y"), 'Y').otherwise(lit('N')))

# COMMAND ----------

shaw_ch_sa_df_final = shaw_ch_sa_df_agg.select('RCIS_ID', 'S_TV_SERVICEABILITY_FLAG', 'INT_SERVICEABILITY_FLAG', 'HP_SERVICEABILITY_FLAG',\
                                               'C_TV_SERVICEABILITY_TYPE', 'C_TV_SERVICEABILITY_FLAG', 'MAX_DOWN_SPEED', 'MAX_UP_SPEED' )

# COMMAND ----------

matchedentity_shaw_ch_sa_df_final = matched_entity_final.join(shaw_ch_sa_df_final, matched_entity_final.ECID == shaw_ch_sa_df_final.RCIS_ID, 'left').drop('RCIS_ID')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shaw Wireline & Wireless Datasets

# COMMAND ----------

shaw_consumer_wireless_account_df = spark.read.format("parquet").load(cl_shaw_consumer_wireless_account)
shaw_consumer_wireline_account_df = spark.read.format("parquet").load(cl_shaw_consumer_wireline_account)
shaw_consumer_wireless_service_df = spark.read.format("parquet").load(cl_shaw_consumer_wireless_service)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Wireless Account & Services

# COMMAND ----------

#Follows the mapping sheet logic
shaw_c_wls_acc_ser_df = shaw_consumer_wireless_account_df.join(shaw_consumer_wireless_service_df, shaw_consumer_wireless_account_df.fa_id_cl == shaw_consumer_wireless_service_df.fa_id_cl, 'left').drop(shaw_consumer_wireless_service_df.fa_id_cl)

# COMMAND ----------

shaw_consumer_wireless_account_df.createOrReplaceTempView("consumer_wireless_account")
shaw_consumer_wireless_service_df.createOrReplaceTempView("consumer_wireless_service")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select t1.rcis_id_cl, sum(t2.no_of_live_ctns) as no_of_live_ctns, sum(t2.no_of_inact_ctns) as no_of_inact_ctns, max(case when t2.no_of_live_ctns > 0 then null else t2.deactivation_date end) as deact_date
# MAGIC from consumer_wireless_account t1
# MAGIC left join (Select k1.fa_id_cl, sum(decode(upper(service_status),'CANCELLED',0,1)) no_of_live_ctns, sum(decode(upper(service_status),'CANCELLED',1,0)) no_of_inact_ctns,
# MAGIC             max(deactivation_date) as deactivation_date
# MAGIC             from (Select v1.*, rank() over (partition by v1.fa_id_cl, v1.subscriber_no order by decode(upper(service_status),'ACTIVE',1, 'SUSPENDED',2, 'CANCELLED',3), service_activation_date desc, deactivation_date desc, objid desc) as rnk
# MAGIC            from consumer_wireless_service v1) k1 where rnk=1 group by k1.fa_id_cl) t2 on t1.fa_id_cl = t2.fa_id_cl
# MAGIC group by t1.rcis_id_cl;

# COMMAND ----------

shaw_c_wls_acc_ser_df_joined = shaw_c_wls_acc_ser_df.join(_sqldf, shaw_c_wls_acc_ser_df.rcis_id_cl == _sqldf.rcis_id_cl, 'left').drop(_sqldf.rcis_id_cl, _sqldf.no_of_inact_ctns)

# COMMAND ----------

temp_df = shaw_c_wls_acc_ser_df_joined.withColumn('SHAW_WIRELESS_STATUS', when(col('no_of_live_ctns')>0, 'Active')\
                                                .when(upper(col('account_status'))=='SOFT SUSPEND', 'Suspended')\
                                                .when((upper(col('account_status'))=='HARD SUSPEND') | (upper(col('account_status'))=='DEACTIVATED') | (col('no_of_live_ctns')==0), 'Cancelled')\
                                                .otherwise(lit('No Match')))\
            .withColumn('SHAW_WIRELESS_DEACT_DT', when(col('no_of_live_ctns')>0, lit(None))\
                                                .when(col('no_of_live_ctns')==0, col('deact_date'))\
                                                .otherwise(lit(None)))

# COMMAND ----------

shaw_c_wls_acc_ser_df_temp_agg = temp_df.groupBy('RCIS_ID_CL').agg(collect_set("SHAW_WIRELESS_STATUS"),first('SHAW_WIRELESS_DEACT_DT').alias('SHAW_WIRELESS_DEACT_DT'))

# COMMAND ----------

#Setting flags based on the values in the sets generated in the previous step
shaw_c_wls_acc_ser_df_agg = shaw_c_wls_acc_ser_df_temp_agg.withColumn('SHAW_WIRELESS_STATUS', when(array_contains(col('collect_set(SHAW_WIRELESS_STATUS)'),"Active"), 'Active')\
                                                                                             .when(array_contains(col('collect_set(SHAW_WIRELESS_STATUS)'),"Suspended"), 'Suspended')\
                                                                                             .when(array_contains(col('collect_set(SHAW_WIRELESS_STATUS)'),"Cancelled"), 'Cancelled')\
                                                                                             .otherwise(lit('No Match')))\
                                                          .drop('collect_set(SHAW_WIRELESS_STATUS)')\
                                                          .withColumn('SHAW_WIRELESS_DEACT_DT', col('SHAW_WIRELESS_DEACT_DT'))

marketing_output_with_wls = matchedentity_shaw_ch_sa_df_final.join(shaw_c_wls_acc_ser_df_agg, matchedentity_shaw_ch_sa_df_final.SHAW_MASTER_PARTY_ID ==shaw_c_wls_acc_ser_df_agg.RCIS_ID_CL, 'left').drop('RCIS_ID_CL')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Wireline

# COMMAND ----------

# change-1 :: New sheet
shaw_c_wln_acc_df_temp = shaw_consumer_wireline_account_df.withColumn('SHAW_WIRELINE_STATUS', when(upper(col('ACCOUNT_STATUS'))=='ACTIVE', 'Active')\
                                                                                             .when(upper(col('ACCOUNT_STATUS'))=='INACTIVE', 'Cancelled')\
                                                                                             .otherwise(lit('No Match')))\
                                                          .withColumn('SHAW_WIRELINE_DEACT_DT', when(upper(col('ACCOUNT_STATUS'))=='ACTIVE', lit(None))\
                                                                                             .when(upper(col('ACCOUNT_STATUS'))=='INACTIVE',\
                                                                                                   greatest('video_disconnect_date', 'internet_disconnect_date', 'wireline_phone_disconnect_date', 'shm_disconnect_date'))\
                                                                                             .otherwise(lit(None)))\
                                                          .withColumn('SHAW_INTERNET_FLAG', when(col('internet_flag')=='Y', 'Y').otherwise(lit('N')))\
                                                          .withColumn('SHAW_INTERNET_DEACT_DT', when(col('internet_disconnect_flag')=='Y', col('internet_disconnect_date')).otherwise(lit(None)))\
                                                          .withColumn('SHAW_CABLE_TV_FLAG', when(((col('video_delivery_type')=='BLUECURVE TV') | (col('source_system')=='SBO'))\
                                                                                                 & (col('video_flag') == 'Y'), 'Y').otherwise(lit('N')))\
                                                          .withColumn('SHAW_LEGACY_TV_FLAG', when(((col('video_delivery_type')=='LEGACY TV') | (col('source_system')=='CBS'))\
                                                                                                  & (col('video_flag') == 'Y'), 'Y').otherwise(lit('N')))\
                                                          .withColumn('SHAW_SATELLITE_TV_FLAG', when((col('video_delivery_type')=='SATELLITE') & (col('video_flag') == 'Y'), 'Y').otherwise(lit('N')))\
                                                          .withColumn('SHAW_HP_FLAG', when(col('wireline_phone_flag') == 'Y', 'Y').otherwise(lit('N')))

# COMMAND ----------

shaw_c_wln_acc_df_temp = shaw_c_wln_acc_df_temp.withColumn('SHAW_CABLE_DEACT_DT', when(((col('video_delivery_type')=='BLUECURVE TV') | (col('source_system')=='SBO')) & (col('video_disconnect_flag') == 'Y') &\
                                                                                       (col('SHAW_CABLE_TV_FLAG') =='N'),col('video_disconnect_date')).otherwise(lit(None)))\
                                               .withColumn('SHAW_LEGACY_TV_DEACT_DT', when(((col('video_delivery_type')=='LEGACY TV') | (col('source_system')=='CBS')) & (col('video_disconnect_flag') == 'Y') &\
                                                                                           (col('SHAW_LEGACY_TV_FLAG') == 'N'),col('video_disconnect_date')).otherwise(lit(None)))\
                                               .withColumn('SHAW_SATELLITE_TV_DEACT_DT', when((col('video_delivery_type')=='SATELLITE') & (col('video_disconnect_flag') == 'Y') &\
                                                                                              (col('SHAW_SATELLITE_TV_FLAG') == 'N'), col('video_disconnect_date')).otherwise(lit(None)))\
                                               .withColumn('SHAW_HP_DEACT_DT', when((col('wireline_phone_disconnect_flag') == 'Y') & (col('SHAW_HP_FLAG') == 'N'),\
                                                                                    col('wireline_phone_disconnect_date')).otherwise(lit(None)))

# COMMAND ----------

shaw_c_wln_acc_df_temp_agg = shaw_c_wln_acc_df_temp.groupBy('RCIS_ID_CL').agg(collect_set("SHAW_WIRELINE_STATUS"),\
                                                                              collect_set("SHAW_INTERNET_FLAG"),\
                                                                              collect_set("SHAW_CABLE_TV_FLAG"),\
                                                                              collect_set("SHAW_HP_FLAG"),\
                                                                              collect_set("SHAW_LEGACY_TV_FLAG"),\
                                                                              collect_set("SHAW_SATELLITE_TV_FLAG"),\
                                                                              collect_set("SHAW_WIRELINE_DEACT_DT"),\
                                                                              max("SHAW_CABLE_DEACT_DT").alias("SHAW_CABLE_DEACT_DT"),\
                                                                              max("SHAW_LEGACY_TV_DEACT_DT").alias("SHAW_LEGACY_TV_DEACT_DT"),\
                                                                              max("SHAW_SATELLITE_TV_DEACT_DT").alias("SHAW_SATELLITE_TV_DEACT_DT"),\
                                                                              max("SHAW_HP_DEACT_DT").alias("SHAW_HP_DEACT_DT"),\
                                                                              max("SHAW_INTERNET_DEACT_DT").alias("SHAW_INTERNET_DEACT_DT")
                                                                              )

# COMMAND ----------

shaw_c_wln_acc_df_agg = shaw_c_wln_acc_df_temp_agg.withColumn('SHAW_WIRELINE_STATUS', when(array_contains(col('collect_set(SHAW_WIRELINE_STATUS)'),"Active"), 'Active')\
                                                                                     .when(array_contains(col('collect_set(SHAW_WIRELINE_STATUS)'),"Cancelled"), 'Cancelled')\
                                                                                     .otherwise(lit('No Match')))\
                                                  .withColumn('SHAW_INTERNET_FLAG', when(array_contains(col('collect_set(SHAW_INTERNET_FLAG)'),"Y"), 'Y').otherwise(lit('N')))\
                                                  .withColumn('SHAW_CABLE_TV_FLAG', when(array_contains(col('collect_set(SHAW_CABLE_TV_FLAG)'),"Y"), 'Y').otherwise(lit('N')))\
                                                  .withColumn('SHAW_HP_FLAG', when(array_contains(col('collect_set(SHAW_HP_FLAG)'),"Y"), 'Y').otherwise(lit('N')))\
                                                  .withColumn('SHAW_LEGACY_TV_FLAG', when(array_contains(col('collect_set(SHAW_LEGACY_TV_FLAG)'),"Y"), 'Y').otherwise(lit('N')))\
                                                  .withColumn('SHAW_SATELLITE_TV_FLAG', when(array_contains(col('collect_set(SHAW_SATELLITE_TV_FLAG)'),"Y"), 'Y').otherwise(lit('N')))\
                                                  .withColumn('SHAW_WIRELINE_DEACT_DT', when(col('SHAW_WIRELINE_STATUS') == "Active", lit(None))\
                                                                                       .when(col('SHAW_WIRELINE_STATUS') == "Cancelled", array_max(col('collect_set(SHAW_WIRELINE_DEACT_DT)')))\
                                                                                       .otherwise(lit(None)))

# COMMAND ----------

shaw_c_wln_acc_df_agg_final = shaw_c_wln_acc_df_agg.select('RCIS_ID_CL', 'SHAW_WIRELINE_STATUS', 'SHAW_WIRELINE_DEACT_DT', 'SHAW_INTERNET_FLAG', 'SHAW_INTERNET_DEACT_DT', 'SHAW_CABLE_TV_FLAG',\
                                                           'SHAW_CABLE_DEACT_DT', 'SHAW_LEGACY_TV_FLAG', 'SHAW_LEGACY_TV_DEACT_DT','SHAW_SATELLITE_TV_FLAG', 'SHAW_SATELLITE_TV_DEACT_DT',\
                                                           'SHAW_HP_FLAG', 'SHAW_HP_DEACT_DT')

# COMMAND ----------

marketing_output_final_df = marketing_output_with_wls.join(shaw_c_wln_acc_df_agg_final, marketing_output_with_wls.SHAW_MASTER_PARTY_ID ==  shaw_c_wln_acc_df_agg_final.RCIS_ID_CL, 'left').drop('RCIS_ID_CL')

# COMMAND ----------

marketing_output_final_df =marketing_output_final_df.withColumn('DET_MATCH_FLAG', when(col('DET_MATCH_FLAG')== 'Y', 'Y').when(col('DET_MATCH_FLAG')== 'N', 'N').otherwise('N'))\
                                                    .withColumn('FUZZY_MATCH_FLAG', when(col('FUZZY_MATCH_FLAG')== 'Y', 'Y').when(col('FUZZY_MATCH_FLAG')== 'N', 'N').otherwise('N'))\
                                                    .withColumn('PROB_MATCH_FLAG', when(col('PROB_MATCH_FLAG')== 'Y', 'Y').when(col('PROB_MATCH_FLAG')== 'N', 'N').otherwise('N'))\
                                                    .withColumn('S_TV_SERVICEABILITY_FLAG', when(col('S_TV_SERVICEABILITY_FLAG')== 'Y', 'Y').when(col('S_TV_SERVICEABILITY_FLAG')== 'N', 'N').otherwise('N'))\
                                                    .withColumn('INT_SERVICEABILITY_FLAG', when(col('INT_SERVICEABILITY_FLAG')== 'Y', 'Y').when(col('INT_SERVICEABILITY_FLAG')== 'N', 'N').otherwise('N'))\
                                                    .withColumn('HP_SERVICEABILITY_FLAG', when(col('HP_SERVICEABILITY_FLAG')== 'Y', 'Y').when(col('HP_SERVICEABILITY_FLAG')== 'N', 'N').otherwise('N'))\
                                                    .withColumn('C_TV_SERVICEABILITY_FLAG', when(col('C_TV_SERVICEABILITY_FLAG')== 'Y', 'Y').when(col('C_TV_SERVICEABILITY_FLAG')== 'N', 'N').otherwise('N'))\
                                                    .withColumn('SHAW_INTERNET_FLAG', when(col('SHAW_INTERNET_FLAG')== 'Y', 'Y').when(col('SHAW_INTERNET_FLAG')== 'N', 'N').otherwise('N'))\
                                                    .withColumn('SHAW_CABLE_TV_FLAG', when(col('SHAW_CABLE_TV_FLAG')== 'Y', 'Y').when(col('SHAW_CABLE_TV_FLAG')== 'N', 'N').otherwise('N'))\
                                                    .withColumn('SHAW_LEGACY_TV_FLAG', when(col('SHAW_LEGACY_TV_FLAG')== 'Y', 'Y').when(col('SHAW_LEGACY_TV_FLAG')== 'N', 'N').otherwise('N'))\
                                                    .withColumn('SHAW_HP_FLAG', when(col('SHAW_HP_FLAG')== 'Y', 'Y').when(col('SHAW_HP_FLAG')== 'N', 'N').otherwise('N'))\
                                                    .withColumn('SHAW_SATELLITE_TV_FLAG', when(col('SHAW_SATELLITE_TV_FLAG')== 'Y', 'Y').when(col('SHAW_SATELLITE_TV_FLAG')== 'N', 'N').otherwise('N'))\
                                                    .withColumn('SHAW_WIRELINE_STATUS', when(col('SHAW_WIRELINE_STATUS')=='Active', 'Active')\
                                                                                       .when(col('SHAW_WIRELINE_STATUS')=='Cancelled', 'Cancelled')\
                                                                                       .otherwise(lit('No Match')))\
                                                    .withColumn('SHAW_WIRELESS_STATUS', when(col('SHAW_WIRELESS_STATUS')=='Active', 'Active')\
                                                                                       .when(col('SHAW_WIRELESS_STATUS')=='Suspended', 'Suspended')\
                                                                                       .when(col('SHAW_WIRELESS_STATUS')=='Cancelled', 'Cancelled')\
                                                                                       .otherwise(lit('No Match')))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Storage

# COMMAND ----------

marketing_output_final_df.write.parquet(customer_marketing_op,mode='overwrite')
