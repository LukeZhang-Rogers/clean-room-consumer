# Databricks notebook source
# MAGIC %md
# MAGIC ## Probabilistic Matching Customer : Executing All Matching Rules
# MAGIC ## Description:
# MAGIC This Notebook is called to execute the all customer matching rules
# MAGIC
# MAGIC ## Requirements:
# MAGIC - 'ADLSDesignedPaths' notebook loads Foundational tables of Rogers and Shaw Data. Foundational tables are created by doing Data Standarization/Normlization on Raw Data. 
# MAGIC - Notebook "ProbabilisticCustomerUtils" is also an input to this notebook. It has to be placed in the same folder as this notebook.
# MAGIC - Libraries : splink 2.1.12 and altair 4.2.0
# MAGIC - Please refer to Splink Documentation for more information around Splink and it's usage https://github.com/moj-analytical-services/splink

# COMMAND ----------

# MAGIC %sh
# MAGIC ifconfig

# COMMAND ----------

from pyspark.sql import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List
from pyspark.sql import Column

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Utils

# COMMAND ----------

# MAGIC %run ./ProbabilisticCustomerUtils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# MAGIC %run ../../2.Common/ADLSDesignedPaths

# COMMAND ----------

#read parquet
foundation_rogers_wls_c_df      =  spark.read.format("parquet").load(foundation_rogers_wls_c)
foundation_rogers_wln_c_df      =  spark.read.format("parquet").load(foundation_rogers_wln_c)
foundation_shaw_wls_c_df        =  spark.read.format("parquet").load(foundation_shaw_wls_c)
foundation_shaw_wln_c_df        =  spark.read.format("parquet").load(foundation_shaw_wln_c)

# COMMAND ----------

#union of Rogers Wireless and Rogers Wireline
rogers_c_wln_wls = foundation_rogers_wls_c_df.unionByName(foundation_rogers_wln_c_df)

# COMMAND ----------

#union of Shaw Wireless and Shaw Wireline
shaw_c_wln_wls = foundation_shaw_wls_c_df.unionByName(foundation_shaw_wln_c_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Pre Processing
# MAGIC Two Main tasks :
# MAGIC 1. Selecting the coloumns/attributes from DE processed data that need to be used and needed in Output Table. 
# MAGIC 2. Renaming columns as part of Splink requirement.

# COMMAND ----------

# pre-processing for shaw
shaw_splink_ready = shaw_select_rename(shaw_c_wln_wls)

# COMMAND ----------

# pre-processing for rogers
rogers_splink_ready = rogers_select_rename(rogers_c_wln_wls)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rule 23
# MAGIC Name, Email, Phone Number and Mailing/Service Address

# COMMAND ----------

# apply splink for rule 23 - Name, Email, Phone Number and Mailing/Service Address
linker_rule23 = Splink(settings_rule23, [rogers_splink_ready, shaw_splink_ready], spark)
linker_rule23_df = linker_rule23.get_scored_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Processing 
# MAGIC Splink is not able to generate accurate probability scores when there is null in the attributes. Post-processing is required to filter out record pairs with null attributes to improve matching outcome

# COMMAND ----------

# filter out pairs with null attribtes for rule 23 - Name, Email, Phone Number and Mailing/Service Address
rule23_interim = linker_rule23_df.filter(linker_rule23_df.first_name_cl_l.isNotNull() & linker_rule23_df.first_name_cl_r.isNotNull()) \
                         .filter(linker_rule23_df.e_mail_cl_l.isNotNull() & linker_rule23_df.e_mail_cl_r.isNotNull()) \
                         .filter(~(linker_rule23_df.primary_phone_cl_l.isNull() & linker_rule23_df.alternate_phone_1_cl_l.isNull() & linker_rule23_df.alternate_phone_2_cl_l.isNull()))\
                         .filter(~(linker_rule23_df.primary_phone_cl_r.isNull() & linker_rule23_df.alternate_phone_1_cl_r.isNull() & linker_rule23_df.alternate_phone_2_cl_r.isNull()))\
                         .filter(~(linker_rule23_df.service_address_no_zipcode_dq_cl_l.isNull() & linker_rule23_df.mailing_address_no_zipcode_dq_cl_l.isNull()))\
                         .filter(~(linker_rule23_df.service_address_no_zipcode_dq_cl_r.isNull() & linker_rule23_df.mailing_address_no_zipcode_dq_cl_r.isNull()))

# COMMAND ----------

# rename coloumns back to original names as raw data to create output
rule23_results = rule23_interim.withColumnRenamed('match_probability','23')\
                                 .withColumnRenamed('rcis_id_cl_l','r_rcis_id_cl')\
                                 .withColumnRenamed('rcis_id_cl_r','s_rcis_id_cl')\
                                 .withColumnRenamed('fa_id_cl_l','r_fa_id_cl')\
                                 .withColumnRenamed('fa_id_cl_r','s_fa_id_cl')\
                                 .withColumnRenamed('sam_key_l','r_sam_key')\
                                 .withColumnRenamed('sam_key_r','s_sam_key')\
                                 .withColumnRenamed('account_type_l','r_account_type')\
                                 .withColumnRenamed('account_type_r','s_account_type')\
                                 .withColumnRenamed('account_status_l','r_account_status')\
                                 .withColumnRenamed('account_status_r','s_account_status')\
                                 .withColumnRenamed('snapshot_stamp_l','r_snapshot_stamp')\
                                 .withColumnRenamed('snapshot_stamp_r','s_snapshot_stamp')\
                                 .select(['r_rcis_id_cl','s_rcis_id_cl','r_fa_id_cl','s_fa_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','23'])

# COMMAND ----------

# create output
cols = ['23']
rule_23_longtable  = rule23_results.withColumn('array', f.explode(f.arrays_zip(f.array(*map(lambda x: f.lit(x), cols)), f.array(*cols), ))) \
  .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','array.*') \
  .toDF('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','ruleset_id','ruleset_attr')

# COMMAND ----------

# MAGIC %md
# MAGIC # Rule 24
# MAGIC Name, Email and Phone Number

# COMMAND ----------

# apply splink for rule 24 - Name, Email and Phone Number
linker_rule24 = Splink(settings_rule24, [rogers_splink_ready, shaw_splink_ready], spark)
linker_rule24_df = linker_rule24.get_scored_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Processing 
# MAGIC Splink is not able to generate accurate probability scores when there is null in the attributes. Post-processing is required to filter out record pairs with null attributes to improve matching outcome

# COMMAND ----------

# filter out pairs with null attribtes for rule 24 - Name, Email and Phone Number
rule24_interim = linker_rule24_df.filter(linker_rule24_df.first_name_cl_l.isNotNull() & linker_rule24_df.first_name_cl_r.isNotNull()) \
                         .filter(linker_rule24_df.e_mail_cl_l.isNotNull() & linker_rule24_df.e_mail_cl_r.isNotNull()) \
                         .filter(~(linker_rule24_df.primary_phone_cl_l.isNull() & linker_rule24_df.alternate_phone_1_cl_l.isNull() & linker_rule24_df.alternate_phone_2_cl_l.isNull()))\
                         .filter(~(linker_rule24_df.primary_phone_cl_r.isNull() & linker_rule24_df.alternate_phone_1_cl_r.isNull() & linker_rule24_df.alternate_phone_2_cl_r.isNull()))

# COMMAND ----------

# rename coloumns back to original names as raw data to create output
rule24_results = rule24_interim.withColumnRenamed('match_probability','24')\
                                 .withColumnRenamed('rcis_id_cl_l','r_rcis_id_cl')\
                                 .withColumnRenamed('rcis_id_cl_r','s_rcis_id_cl')\
                                 .withColumnRenamed('fa_id_cl_l','r_fa_id_cl')\
                                 .withColumnRenamed('fa_id_cl_r','s_fa_id_cl')\
                                 .withColumnRenamed('sam_key_l','r_sam_key')\
                                 .withColumnRenamed('sam_key_r','s_sam_key')\
                                 .withColumnRenamed('account_type_l','r_account_type')\
                                 .withColumnRenamed('account_type_r','s_account_type')\
                                 .withColumnRenamed('account_status_l','r_account_status')\
                                 .withColumnRenamed('account_status_r','s_account_status')\
                                 .withColumnRenamed('snapshot_stamp_l','r_snapshot_stamp')\
                                 .withColumnRenamed('snapshot_stamp_r','s_snapshot_stamp')\
                                 .select(['r_rcis_id_cl','s_rcis_id_cl','r_fa_id_cl','s_fa_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','24'])

# COMMAND ----------

# create output
cols = ['24']
rule_24_longtable  = rule24_results.withColumn('array', f.explode(f.arrays_zip(f.array(*map(lambda x: f.lit(x), cols)), f.array(*cols), ))) \
  .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','array.*') \
  .toDF('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','ruleset_id','ruleset_attr')

# COMMAND ----------

# MAGIC %md 
# MAGIC # Rule 25
# MAGIC Name, Email and Mailing/Service Address

# COMMAND ----------

# apply splink for rule 25 - Name, Email and Mailing/Service Address
linker_rule25 = Splink(settings_rule25, [rogers_splink_ready, shaw_splink_ready], spark)
linker_rule25_df = linker_rule25.get_scored_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Processing

# COMMAND ----------

# filter out pairs with null attribtes for rule 25 - Name, Email and Mailing/Service Address
rule25_interim = linker_rule25_df.filter(linker_rule25_df.first_name_cl_l.isNotNull() & linker_rule25_df.first_name_cl_r.isNotNull()) \
                         .filter(linker_rule25_df.e_mail_cl_l.isNotNull() & linker_rule25_df.e_mail_cl_r.isNotNull()) \
                         .filter(~(linker_rule25_df.service_address_no_zipcode_dq_cl_l.isNull() & linker_rule25_df.mailing_address_no_zipcode_dq_cl_l.isNull()))\
                         .filter(~(linker_rule25_df.service_address_no_zipcode_dq_cl_r.isNull() & linker_rule25_df.mailing_address_no_zipcode_dq_cl_r.isNull()))

# COMMAND ----------

# rename coloumns back to original names as raw data to create output
rule25_results = rule25_interim.withColumnRenamed('match_probability','25')\
                                 .withColumnRenamed('rcis_id_cl_l','r_rcis_id_cl')\
                                 .withColumnRenamed('rcis_id_cl_r','s_rcis_id_cl')\
                                 .withColumnRenamed('fa_id_cl_l','r_fa_id_cl')\
                                 .withColumnRenamed('fa_id_cl_r','s_fa_id_cl')\
                                 .withColumnRenamed('sam_key_l','r_sam_key')\
                                 .withColumnRenamed('sam_key_r','s_sam_key')\
                                 .withColumnRenamed('account_type_l','r_account_type')\
                                 .withColumnRenamed('account_type_r','s_account_type')\
                                 .withColumnRenamed('account_status_l','r_account_status')\
                                 .withColumnRenamed('account_status_r','s_account_status')\
                                 .withColumnRenamed('snapshot_stamp_l','r_snapshot_stamp')\
                                 .withColumnRenamed('snapshot_stamp_r','s_snapshot_stamp')\
                                 .select(['r_rcis_id_cl','s_rcis_id_cl','r_fa_id_cl','s_fa_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','25'])

# COMMAND ----------

# create output
cols = ['25']
rule_25_longtable  = rule25_results.withColumn('array', f.explode(f.arrays_zip(f.array(*map(lambda x: f.lit(x), cols)), f.array(*cols), ))) \
  .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','array.*') \
  .toDF('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','ruleset_id','ruleset_attr')

# COMMAND ----------

# MAGIC %md
# MAGIC # Rule 26
# MAGIC Name, Phone and Mailing/Service Address

# COMMAND ----------

# apply splink for rule 26 - Name, Phone and Mailing/Service Address
linker_rule26 = Splink(settings_rule26, [rogers_splink_ready, shaw_splink_ready], spark)
linker_rule26_df = linker_rule26.get_scored_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Processing 

# COMMAND ----------

# filter out pairs with null attribtes for rule 26 - Name, Phone and Mailing/Service Address
rule26_interim = linker_rule26_df.filter(linker_rule26_df.first_name_cl_l.isNotNull() & linker_rule26_df.first_name_cl_r.isNotNull()) \
                         .filter(~(linker_rule26_df.primary_phone_cl_l.isNull() & linker_rule26_df.alternate_phone_1_cl_l.isNull() & linker_rule26_df.alternate_phone_2_cl_l.isNull()))\
                         .filter(~(linker_rule26_df.primary_phone_cl_r.isNull() & linker_rule26_df.alternate_phone_1_cl_r.isNull() & linker_rule26_df.alternate_phone_2_cl_r.isNull()))\
                         .filter(~(linker_rule26_df.service_address_no_zipcode_dq_cl_l.isNull() & linker_rule26_df.mailing_address_no_zipcode_dq_cl_l.isNull()))\
                         .filter(~(linker_rule26_df.service_address_no_zipcode_dq_cl_r.isNull() & linker_rule26_df.mailing_address_no_zipcode_dq_cl_r.isNull()))

# COMMAND ----------

# rename coloumns back to original names as raw data to create output
rule26_results = rule26_interim.withColumnRenamed('match_probability','26')\
                                 .withColumnRenamed('rcis_id_cl_l','r_rcis_id_cl')\
                                 .withColumnRenamed('rcis_id_cl_r','s_rcis_id_cl')\
                                 .withColumnRenamed('fa_id_cl_l','r_fa_id_cl')\
                                 .withColumnRenamed('fa_id_cl_r','s_fa_id_cl')\
                                 .withColumnRenamed('sam_key_l','r_sam_key')\
                                 .withColumnRenamed('sam_key_r','s_sam_key')\
                                 .withColumnRenamed('account_type_l','r_account_type')\
                                 .withColumnRenamed('account_type_r','s_account_type')\
                                 .withColumnRenamed('account_status_l','r_account_status')\
                                 .withColumnRenamed('account_status_r','s_account_status')\
                                 .withColumnRenamed('snapshot_stamp_l','r_snapshot_stamp')\
                                 .withColumnRenamed('snapshot_stamp_r','s_snapshot_stamp')\
                                 .select(['r_rcis_id_cl','s_rcis_id_cl','r_fa_id_cl','s_fa_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','26'])

# COMMAND ----------

# create output
cols = ['26']
rule_26_longtable  = rule26_results.withColumn('array', f.explode(f.arrays_zip(f.array(*map(lambda x: f.lit(x), cols)), f.array(*cols), ))) \
  .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','array.*') \
  .toDF('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','ruleset_id','ruleset_attr')

# COMMAND ----------

# MAGIC %md 
# MAGIC # Rule 27 
# MAGIC Name and Email

# COMMAND ----------

# apply splink for rule 27 - Name and Email
linker_rule27 = Splink(settings_rule27, [rogers_splink_ready, shaw_splink_ready], spark)
linker_rule27_df = linker_rule27.get_scored_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Processing 

# COMMAND ----------

# filter out pairs with null attribtes for rule 27 - Name and Email
rule27_interim = linker_rule27_df.filter(linker_rule27_df.first_name_cl_l.isNotNull() & linker_rule27_df.first_name_cl_r.isNotNull()) \
                         .filter(linker_rule27_df.e_mail_cl_l.isNotNull() & linker_rule27_df.e_mail_cl_r.isNotNull())

# COMMAND ----------

# rename coloumns back to original names as raw data to create output
rule27_results = rule27_interim.withColumnRenamed('match_probability','27')\
                                 .withColumnRenamed('rcis_id_cl_l','r_rcis_id_cl')\
                                 .withColumnRenamed('rcis_id_cl_r','s_rcis_id_cl')\
                                 .withColumnRenamed('fa_id_cl_l','r_fa_id_cl')\
                                 .withColumnRenamed('fa_id_cl_r','s_fa_id_cl')\
                                 .withColumnRenamed('sam_key_l','r_sam_key')\
                                 .withColumnRenamed('sam_key_r','s_sam_key')\
                                 .withColumnRenamed('account_type_l','r_account_type')\
                                 .withColumnRenamed('account_type_r','s_account_type')\
                                 .withColumnRenamed('account_status_l','r_account_status')\
                                 .withColumnRenamed('account_status_r','s_account_status')\
                                 .withColumnRenamed('snapshot_stamp_l','r_snapshot_stamp')\
                                 .withColumnRenamed('snapshot_stamp_r','s_snapshot_stamp')\
                                 .select(['r_rcis_id_cl','s_rcis_id_cl','r_fa_id_cl','s_fa_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','27'])

# COMMAND ----------

# create output
cols = ['27']
rule_27_longtable  = rule27_results.withColumn('array', f.explode(f.arrays_zip(f.array(*map(lambda x: f.lit(x), cols)), f.array(*cols), ))) \
  .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','array.*') \
  .toDF('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','ruleset_id','ruleset_attr')

# COMMAND ----------

# MAGIC %md
# MAGIC # Rule 28
# MAGIC Name and Phone

# COMMAND ----------

# apply splink for rule 28 - Name and Phone
linker_rule28 = Splink(settings_rule28, [rogers_splink_ready, shaw_splink_ready], spark)
linker_rule28_df = linker_rule28.get_scored_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Processing 

# COMMAND ----------

# filter out pairs with null attribtes for rule 28 - Name and Phone
rule28_interim = linker_rule28_df.filter(linker_rule28_df.first_name_cl_l.isNotNull() & linker_rule28_df.first_name_cl_r.isNotNull()) \
                         .filter(~(linker_rule28_df.primary_phone_cl_l.isNull() & linker_rule28_df.alternate_phone_1_cl_l.isNull() & linker_rule28_df.alternate_phone_2_cl_l.isNull()))\
                         .filter(~(linker_rule28_df.primary_phone_cl_r.isNull() & linker_rule28_df.alternate_phone_1_cl_r.isNull() & linker_rule28_df.alternate_phone_2_cl_r.isNull()))

# COMMAND ----------

# rename coloumns back to original names as raw data to create output
rule28_results = rule28_interim.withColumnRenamed('match_probability','28')\
                                 .withColumnRenamed('rcis_id_cl_l','r_rcis_id_cl')\
                                 .withColumnRenamed('rcis_id_cl_r','s_rcis_id_cl')\
                                 .withColumnRenamed('fa_id_cl_l','r_fa_id_cl')\
                                 .withColumnRenamed('fa_id_cl_r','s_fa_id_cl')\
                                 .withColumnRenamed('sam_key_l','r_sam_key')\
                                 .withColumnRenamed('sam_key_r','s_sam_key')\
                                 .withColumnRenamed('account_type_l','r_account_type')\
                                 .withColumnRenamed('account_type_r','s_account_type')\
                                 .withColumnRenamed('account_status_l','r_account_status')\
                                 .withColumnRenamed('account_status_r','s_account_status')\
                                 .withColumnRenamed('snapshot_stamp_l','r_snapshot_stamp')\
                                 .withColumnRenamed('snapshot_stamp_r','s_snapshot_stamp')\
                                 .select(['r_rcis_id_cl','s_rcis_id_cl','r_fa_id_cl','s_fa_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','28'])

# COMMAND ----------

# create output
cols = ['28']
rule_28_longtable  = rule28_results.withColumn('array', f.explode(f.arrays_zip(f.array(*map(lambda x: f.lit(x), cols)), f.array(*cols), ))) \
  .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','array.*') \
  .toDF('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','ruleset_id','ruleset_attr') 

# COMMAND ----------

# MAGIC %md
# MAGIC # Rule 29
# MAGIC Name and Mailing/Service

# COMMAND ----------

# apply splink for rule 29 - Name and Mailing/Service
linker_rule29 = Splink(settings_rule29, [rogers_splink_ready, shaw_splink_ready], spark)
linker_rule29_df = linker_rule29.get_scored_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Processing 

# COMMAND ----------

# filter out pairs with null attribtes for rule 29 - Name and Mailing/Service
rule29_interim = linker_rule29_df.filter(linker_rule29_df.first_name_cl_l.isNotNull() & linker_rule29_df.first_name_cl_r.isNotNull()) \
                         .filter(~(linker_rule29_df.service_address_no_zipcode_dq_cl_l.isNull() & linker_rule29_df.mailing_address_no_zipcode_dq_cl_l.isNull()))\
                         .filter(~(linker_rule29_df.service_address_no_zipcode_dq_cl_r.isNull() & linker_rule29_df.mailing_address_no_zipcode_dq_cl_r.isNull()))

# COMMAND ----------

# rename coloumns back to original names as raw data to create output
rule29_results = rule29_interim.withColumnRenamed('match_probability','29')\
                                 .withColumnRenamed('rcis_id_cl_l','r_rcis_id_cl')\
                                 .withColumnRenamed('rcis_id_cl_r','s_rcis_id_cl')\
                                 .withColumnRenamed('fa_id_cl_l','r_fa_id_cl')\
                                 .withColumnRenamed('fa_id_cl_r','s_fa_id_cl')\
                                 .withColumnRenamed('sam_key_l','r_sam_key')\
                                 .withColumnRenamed('sam_key_r','s_sam_key')\
                                 .withColumnRenamed('account_type_l','r_account_type')\
                                 .withColumnRenamed('account_type_r','s_account_type')\
                                 .withColumnRenamed('account_status_l','r_account_status')\
                                 .withColumnRenamed('account_status_r','s_account_status')\
                                 .withColumnRenamed('snapshot_stamp_l','r_snapshot_stamp')\
                                 .withColumnRenamed('snapshot_stamp_r','s_snapshot_stamp')\
                                 .select(['r_rcis_id_cl','s_rcis_id_cl','r_fa_id_cl','s_fa_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','29'])

# COMMAND ----------

# create output
cols = ['29']
rule_29_longtable  = rule29_results.withColumn('array', f.explode(f.arrays_zip(f.array(*map(lambda x: f.lit(x), cols)), f.array(*cols), ))) \
  .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','array.*') \
  .toDF('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','ruleset_id','ruleset_attr') 

# COMMAND ----------

# MAGIC %md
# MAGIC # Account Level Probablistic Output

# COMMAND ----------

# union of each rule's individual table
output = rule_23_longtable.union(rule_24_longtable).union(rule_25_longtable).union(rule_26_longtable).union(rule_27_longtable).union(rule_28_longtable).union(rule_29_longtable).withColumn('ruleset_type', lit('PROBABILISTIC'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Apply Threshold
# MAGIC Thresholds were identified based on PEI data. We didn't do any threshold adjustment/tuning for Entire Data.

# COMMAND ----------

# filter based on each rule's individual threshold
output = output.filter(((output.ruleset_id == 23) & (output.ruleset_attr > 0.99981)) | \
                        ((output.ruleset_id == 24) & (output.ruleset_attr > 0.95)) | \
                        ((output.ruleset_id == 25) & (output.ruleset_attr > 0.95)) | \
                        ((output.ruleset_id == 26) & (output.ruleset_attr > 0.94)) | \
                        ((output.ruleset_id == 27) & (output.ruleset_attr > 0.96)) | \
                        ((output.ruleset_id == 28) & (output.ruleset_attr > 0.99)) | \
                        ((output.ruleset_id == 29) & (output.ruleset_attr > 0.99))).dropDuplicates(["s_fa_id_cl","r_fa_id_cl"]) \
.select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','s_sam_key','r_sam_key','s_account_type','r_account_type','s_account_status','r_account_status','s_snapshot_stamp','r_snapshot_stamp','ruleset_id','ruleset_attr','ruleset_type')

# COMMAND ----------

# MAGIC %md
# MAGIC # Store Output

# COMMAND ----------

# write output
output.write.parquet(interimop_prob_customer, mode='overwrite')

# COMMAND ----------


