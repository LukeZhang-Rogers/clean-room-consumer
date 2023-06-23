# Databricks notebook source
# MAGIC %md ## Building Shaw Foundation Datasets
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
# MAGIC <b>overwrite_output:</b> Set to 1 if you want to overwrite the outputs. Set to 0 if you do not want to overwrite the outputs .</br>

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configuration

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc,count, concat, lit, avg, regexp_replace
from pyspark.sql.types import StringType

# COMMAND ----------

#Widgets

# dbutils.widgets.remove("execution_mode")
# dbutils.widgets.text(name = "overwrite_output", defaultValue = "0")

# COMMAND ----------

var_overwrite_output = dbutils.widgets.get("overwrite_output")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Source Data

# COMMAND ----------

mockup_cl_path =  "/mnt/development/InterimOutput/Fuzzy/QAMockData/Sprint3/CleanedInput"  

shaw_consumer_contact                 = mockup_cl_path + '/shaw_contact'
shaw_consumer_wireline_account        = mockup_cl_path + '/shaw_wireline_account'
shaw_consumer_wireless_account        = mockup_cl_path + '/shaw_wireless_account'


# shaw_path = "/mnt/development/Processed/Iteration6/Shaw"
# rogers_path =  "/mnt/development/Processed/Iteration6/Rogers"


# shaw_consumer_wireless_account = shaw_path + '/WirelessAccount'
# shaw_consumer_wireline_account = shaw_path + '/WirelineAccount'
# shaw_consumer_contact          = shaw_path + '/Contact'

# rogers_contact                 = rogers_path + '/Contact'
# rogers_wireless_account        = rogers_path + '/WirelessAccount'
# rogers_wireline_account        = rogers_path + '/WirelineAccount'

# COMMAND ----------

#Interim Output Paths
shaw_interim_path    = "/mnt/development/InterimOutput/Fuzzy/QAMockData/Sprint3/Interim/Shaw"

foundation_shaw_wln_c  = shaw_interim_path + '/WirelineAndContact'
foundation_shaw_wls_c  = shaw_interim_path + '/WirelessAndContact'


# COMMAND ----------

shaw_consumer_wireless_account_df       = spark.read.format("parquet").load(shaw_consumer_wireless_account)
shaw_consumer_wireline_account_df       = spark.read.format("parquet").load(shaw_consumer_wireline_account)
shaw_consumer_contact_df                = spark.read.format("parquet").load(shaw_consumer_contact)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Contact

# COMMAND ----------

#modifications for mockup
fil_cols = ["contact_id","rcis_id_cl","fa_id_cl","dq_full_name_cl","dq_primary_phone_cl","dq_alternate_phone_1_cl","dq_alternate_phone_2_cl","dq_e_mail_cl","address_cl","city_cl","state_cl","zipcode_cl", "mailing_address_no_zipcode_dq_cl", "mailing_address_full_cl"]

# #originally by DE team
# fil_cols = ["contact_id","rcis_id_cl","fa_id_cl","dq_full_name_cl","dq_primary_phone_cl","dq_alternate_phone_1_cl","dq_alternate_phone_2_cl","dq_e_mail_cl","address_cl","city_cl","state_cl","zipcode_cl","snapshot_stamp","execution_date", 'mailing_address_no_zipcode_dq_cl']

s_contact_filtered = shaw_consumer_contact_df.select(fil_cols).dropDuplicates()

s_contact_filtered = s_contact_filtered.select([col(c).alias("s_"+c) for c in s_contact_filtered.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Wireless

# COMMAND ----------

#modifications for mockup
inp2_cols = ["rcis_id_cl","fa_id_cl"]

# #originally by DE team
# inp2_cols = ["rcis_id_cl","fa_id_cl","source_system","contact_id","account_status","service_provider","snapshot_stamp","execution_date"]


s_wls_account_filtered = shaw_consumer_wireless_account_df.select(inp2_cols).dropDuplicates()

s_wls_account_filtered = s_wls_account_filtered.select([col(c).alias("s_"+c) for c in s_wls_account_filtered.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Wireline

# COMMAND ----------

#modifications for mockup
inp3_wn_cols = ["rcis_id_cl","fa_id_cl","service_address_cl","service_address_no_zipcode_dq_cl", "contact_id", "service_address_full_cl"]

# #originally by DE team
# inp3_wn_cols = ["rcis_id_cl","fa_id_cl","source_system","contact_id","account_status","service_provider","service_address_cl","sam_key","snapshot_stamp","execution_date", "service_address_no_zipcode_dq_cl"]

s_wln_account_filtered = shaw_consumer_wireline_account_df.select(inp3_wn_cols).dropDuplicates()

s_wln_account_filtered = s_wln_account_filtered.select([col(c).alias("s_"+c) for c in s_wln_account_filtered.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Combine Contact & WLS

# COMMAND ----------

s_wls_c_joined_temp = s_contact_filtered.alias('cf').join(s_wls_account_filtered.alias('wsf'),(col('cf.s_rcis_id_cl') == col('wsf.s_rcis_id_cl')) &
                                                     (col('cf.s_fa_id_cl') == col('wsf.s_fa_id_cl')),how="inner")\
                                               .drop(s_wls_account_filtered.s_rcis_id_cl)\
                                               .drop(s_wls_account_filtered.s_fa_id_cl)
#Modified for the mockup data
#                                                .drop(s_wls_account_filtered.s_contact_id)\
#                                                .drop(s_wls_account_filtered.s_snapshot_stamp)\
#                                                .drop(s_wls_account_filtered.s_execution_date)

s_wls_c_final = s_wls_c_joined_temp.withColumn('s_service_address_cl', lit(None).cast(StringType()))\
                                      .withColumn('s_sam_key', lit(None).cast(StringType()))\
                                      .withColumn('s_service_address_no_zipcode_dq_cl', lit(None).cast(StringType()))\
                                      .withColumn('s_service_address_full_cl', lit(None).cast(StringType()))\
                                      .withColumn("s_account_type",lit("wireless"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Combine Contact & WLN

# COMMAND ----------

# Changed to left join for mockup data
s_wln_c_final = s_contact_filtered.alias('cf')\
                                    .join(s_wln_account_filtered.alias('wnf'),col("cf.s_contact_id")==col("wnf.s_contact_id"),how='left')\
                                    .withColumn("s_account_type",lit("Wireline"))\
                                    .drop(s_wln_account_filtered.s_fa_id_cl)\
                                    .drop(s_wln_account_filtered.s_rcis_id_cl)\
                                    .drop(s_wln_account_filtered.s_contact_id)
#Modified for the mockup data
#                                     
#                                     .drop(s_wln_account_filtered.s_execution_date)\
#                                     .drop(s_wln_account_filtered.s_snapshot_stamp)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Storage

# COMMAND ----------

#output 
s_wln_c_final.write.parquet(foundation_shaw_wln_c,mode='overwrite')
s_wls_c_final.write.parquet(foundation_shaw_wls_c,mode='overwrite')

# COMMAND ----------

display (s_wln_c_final)
