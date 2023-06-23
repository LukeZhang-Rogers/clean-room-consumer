# Databricks notebook source
# MAGIC %md
# MAGIC ## Probabilistic Matching Address : Rule 34 - Match on Mailing/Service Address
# MAGIC ## Description:
# MAGIC This Notebook is called to execute the address matching rule 34 : Match on Mailing/Service Address.
# MAGIC
# MAGIC ## Requirements:
# MAGIC - 'ADLSDesignedPaths' notebook loads Foundational tables of Rogers and Shaw Data. Foundational tables are created by doing Data Standarization/Normlization on Raw Data. 
# MAGIC - Notebook "ProbabilisticAddressUtils" is also an input to this notebook. It has to be placed in the same folder as this notebook.
# MAGIC - Libraries : splink 2.1.12 and altair 4.2.0
# MAGIC - Please refer to Splink Documentation for more information around Splink and it's usage https://github.com/moj-analytical-services/splink

# COMMAND ----------

#spark.conf.set("spark.databricks.pyspark.enablePy4JSecurity", False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Utils

# COMMAND ----------

# MAGIC %run ./ProbabilisticAddressUtils

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
# MAGIC ## Rule 34
# MAGIC Mailing/Service Address

# COMMAND ----------

# apply splink for rule 34 - Mailing/Service Address
linker_rule34 = Splink(settings_rule34, [rogers_splink_ready, shaw_splink_ready], spark)
linker_rule34_df = linker_rule34.get_scored_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Processing
# MAGIC Splink is not able to generate accurate probability scores when there is null in the attributes. Post-processing is required to filter out record pairs with null attributes to improve matching outcome

# COMMAND ----------

# filter out pairs with null attribtes for rule 34 - Mailing/Service Address
rule34_interim = linker_rule34_df.filter(~(linker_rule34_df.service_address_no_zipcode_dq_cl_l.isNull() & linker_rule34_df.mailing_address_no_zipcode_dq_cl_l.isNull()))\
                         .filter(~(linker_rule34_df.service_address_no_zipcode_dq_cl_r.isNull() & linker_rule34_df.mailing_address_no_zipcode_dq_cl_r.isNull()))

# COMMAND ----------

# create output
rule34_results = rule34_interim.withColumnRenamed('match_probability','ruleset_attr') \
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
                                  .withColumn('ruleset_type', lit('PROBABILISTIC'))\
                                  .withColumn('ruleset_id', lit(34))\
                                  .select(['s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','s_sam_key','r_sam_key','s_account_type','r_account_type','s_account_status','r_account_status','s_snapshot_stamp','r_snapshot_stamp','ruleset_id','ruleset_attr','ruleset_type'])

# COMMAND ----------

# MAGIC %md
# MAGIC # Apply Threshold
# MAGIC Thresholds were identified based on PEI data. We didn't do any threshold adjustment/tuning for Entire Data.

# COMMAND ----------

#filter based on Threshold
output = rule34_results.filter(rule34_results.ruleset_attr >= 0.95)

# COMMAND ----------

# display(output)

# COMMAND ----------

# MAGIC %md
# MAGIC # Store Output

# COMMAND ----------

#write output
output.write.parquet(interimop_prob_address, mode='overwrite')
