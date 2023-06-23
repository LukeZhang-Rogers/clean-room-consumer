# Databricks notebook source
# MAGIC %md ## Building Rogers Foundation Datasets
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC
# MAGIC This notebook reads the cleansed and standardized datasets from Rogers and Shaw to build the foundation datasets to be used in all downstream processes.
# MAGIC
# MAGIC
# MAGIC ### Requirements:
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC   <li>The processed datasets are available at: '/mnt/development/Processed/'</li>
# MAGIC
# MAGIC </ul>
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Parameters:
# MAGIC
# MAGIC <b>overwrite_output:</b> Set to 1 to overwrite the outputs. Set to 0 (default) to not overwrite the outputs .</br>

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configuration

# COMMAND ----------

from pyspark.sql.functions import col,desc
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col,desc,count, rtrim,substring,lit, concat
from pyspark.sql.types import StringType

# COMMAND ----------

# DBTITLE 1,Uncomment and execute if widgets are not imported
# dbutils.widgets.text(name = "execution_mode", defaultValue = "1")
# dbutils.widgets.text(name = "overwrite_output", defaultValue = "0")

# COMMAND ----------

var_overwrite_output = dbutils.widgets.get("overwrite_output")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Source Data

# COMMAND ----------

#################### input Path #################

mockup_cl_path =  "/mnt/development/InterimOutput/Fuzzy/QAMockData/Sprint3/CleanedInput"  

rogers_contact                 = mockup_cl_path + '/rogers_contact'
rogers_wireline_account        = mockup_cl_path + '/rogers_wireline_account'
rogers_wireless_account        = mockup_cl_path + '/rogers_wireless_account'

# COMMAND ----------

#Interim Output Paths
rogers_interim_path =  "/mnt/development/InterimOutput/Fuzzy/QAMockData/Sprint3/Interim/Rogers"

foundation_rogers_wln_c  = rogers_interim_path + '/WirelineAndContact'
foundation_rogers_wls_c  = rogers_interim_path + '/WirelessAndContact'




# COMMAND ----------

# DBTITLE 1,Read cleansed datasets
rogers_contact_df                 = spark.read.format("parquet").load(rogers_contact)
rogers_wireless_account_df        = spark.read.format("parquet").load(rogers_wireless_account)
rogers_wireline_account_df        = spark.read.format("parquet").load(rogers_wireline_account)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Contact

# COMMAND ----------



# COMMAND ----------

# # Originally by the DE team:
# fil_cols = ["contact_id","rcis_id_cl","fa_id_cl","dq_full_name_cl","dq_primary_phone_cl","dq_alternate_phone_1_cl","dq_alternate_phone_2_cl","dq_e_mail_cl","address_cl","city_cl","state_cl","zipcode_cl","snapshot_stamp","execution_date", 'mailing_address_no_zipcode_dq_cl']

fil_cols = ["rcis_id_cl","fa_id_cl","dq_full_name_cl","dq_primary_phone_cl","dq_alternate_phone_1_cl","dq_alternate_phone_2_cl","dq_e_mail_cl","address_cl","city_cl","state_cl","zipcode_cl",'mailing_address_no_zipcode_dq_cl', "mailing_address_full_cl"]

r_contact_filtered = rogers_contact_df.select(fil_cols).dropDuplicates()

r_contact_filtered = r_contact_filtered.select([col(c).alias("r_"+c) for c in r_contact_filtered.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Wireless

# COMMAND ----------

# For mock up routine
inp2_cols = ["rcis_id_cl","fa_id_cl"]

# # Originally by the DE team:
# inp2_cols = ["rcis_id_cl","fa_id_cl","source_system","contact_id","account_status","service_provider","snapshot_stamp","execution_date"]

r_wls_account_filtered = rogers_wireless_account_df.select(inp2_cols).dropDuplicates()
r_wls_account_filtered = r_wls_account_filtered.select([col(c).alias("r_"+c) for c in r_wls_account_filtered.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Wireline

# COMMAND ----------

# For mock up routine
inp3_wn_cols = ["rcis_id_cl","fa_id_cl", "service_address_cl", "service_address_no_zipcode_dq_cl", "contact_id", "service_address_full_cl"]

# # Originally by the DE team:
# inp3_wn_cols = ["rcis_id_cl","fa_id_cl","source_system","contact_id","account_status","service_provider","service_address_cl","sam_key","snapshot_stamp","execution_date", "service_address_no_zipcode_dq_cl"]

r_wln_account_filtered = rogers_wireline_account_df.select(inp3_wn_cols).dropDuplicates()
r_wln_account_filtered = r_wln_account_filtered.select([col(c).alias("r_"+c) for c in r_wln_account_filtered.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Combine Contact & WLS

# COMMAND ----------

r_wls_c_joined_temp = r_contact_filtered.alias('cf').join(r_wls_account_filtered.alias('wsf'),col("cf.r_fa_id_cl")==col("wsf.r_fa_id_cl"),how='inner')\
                                           .drop(r_wls_account_filtered.r_rcis_id_cl)\
                                           .drop(r_wls_account_filtered.r_fa_id_cl)
#                                            .drop(r_wls_account_filtered.r_contact_id)
#                                            .drop(r_wls_account_filtered.r_snapshot_stamp)\
#                                            .drop(r_wls_account_filtered.r_execution_date)

r_wls_c_final   = r_wls_c_joined_temp.withColumn('r_service_address_cl', lit(None).cast(StringType()))\
                                      .withColumn('r_sam_key', lit(None).cast(StringType()))\
                                      .withColumn('r_service_address_no_zipcode_dq_cl', lit(None).cast(StringType()))\
                                      .withColumn('r_service_address_full_cl', lit(None).cast(StringType()))\
                                      .withColumn("r_account_type",lit("wireless"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Combine Contact & WLN

# COMMAND ----------

#changed to left join for mock up data
r_wln_c_final = r_contact_filtered.alias('cf').join(r_wln_account_filtered.alias('wnf'),col("cf.r_fa_id_cl")==col("wnf.r_fa_id_cl"),how='left')\
                                           .withColumn("r_account_type",lit("wireline"))\
                                           .drop(r_wln_account_filtered.r_rcis_id_cl)\
                                           .drop(r_wln_account_filtered.r_fa_id_cl)\
                                           .drop(r_wln_account_filtered.r_contact_id)
#                                            .drop(r_wln_account_filtered.r_snapshot_stamp)\
#                                            .drop(r_wln_account_filtered.r_execution_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Storage

# COMMAND ----------

#output 
r_wln_c_final.write.parquet(foundation_rogers_wln_c,mode='overwrite')
r_wls_c_final.write.parquet(foundation_rogers_wls_c,mode='overwrite')

# COMMAND ----------

display(r_wln_c_final)

# COMMAND ----------


