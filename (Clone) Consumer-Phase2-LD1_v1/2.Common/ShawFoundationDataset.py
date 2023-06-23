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
# MAGIC <b>overwrite_output:</b> Set to 1 if you want to overwrite the outputs. Set to 0 (default) if you do not want to overwrite the outputs .</br>
# MAGIC <b>include_shawmobile:</b> Set to 1 (default) if you want to include Shaw Mobile customers. Set to 0 if you do not want to include Shaw Mobile customers .</br>

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configuration

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc,count, concat, lit, avg, regexp_replace
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %run ../2.Common/ADLSDesignedPaths

# COMMAND ----------

# DBTITLE 1,Uncomment and execute if widgets do not get imported
# dbutils.widgets.text(name = "overwrite_output", defaultValue = "0")
# dbutils.widgets.text(name = "include_shawmobile", defaultValue = "1")

# COMMAND ----------

var_overwrite_output   = dbutils.widgets.get("overwrite_output")
var_include_shawmobile = dbutils.widgets.get("include_shawmobile")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Source Data

# COMMAND ----------

shaw_consumer_wireless_account = cl_shaw_consumer_wireless_account
shaw_consumer_wireline_account = cl_shaw_consumer_wireline_account
shaw_consumer_contact          = cl_shaw_consumer_contact

rogers_contact                 = cl_rogers_contact
rogers_wireless_account        = cl_rogers_wireless_account
rogers_wireline_account        = cl_rogers_wireline_account

# COMMAND ----------

shaw_consumer_wireless_account_df       = spark.read.format("parquet").load(shaw_consumer_wireless_account)
shaw_consumer_wireline_account_df       = spark.read.format("parquet").load(shaw_consumer_wireline_account)
shaw_consumer_contact_df                = spark.read.format("parquet").load(shaw_consumer_contact)

# COMMAND ----------

shaw_consumer_wireline_account_df = shaw_consumer_wireline_account_df.withColumnRenamed('PostalCode_Cleansed', 'service_postalcode_cl')
shaw_consumer_contact_df = shaw_consumer_contact_df.withColumnRenamed('PostalCode_Cleansed', 'mailing_postalcode_cl')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Contact

# COMMAND ----------

fil_cols = ["contact_id","rcis_id_cl","fa_id_cl","dq_first_name_cl","dq_last_name_cl","dq_full_name_cl","dq_primary_phone_cl","dq_alternate_phone_1_cl","dq_alternate_phone_2_cl","dq_e_mail_cl","address_cl","city_cl","state_cl","zipcode_cl","snapshot_stamp","execution_date", 'mailing_address_no_zipcode_dq_cl','mailing_address_full_cl', 'mailing_postalcode_cl']

s_contact_filtered = shaw_consumer_contact_df.select(fil_cols).dropDuplicates()

s_contact_filtered = s_contact_filtered.select([col(c).alias("s_"+c) for c in s_contact_filtered.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Wireless

# COMMAND ----------

inp2_cols = ["rcis_id_cl","fa_id_cl","source_system","contact_id","account_status","service_provider","snapshot_stamp","execution_date"]

s_wls_account_filtered = shaw_consumer_wireless_account_df.select(inp2_cols).dropDuplicates()

s_wls_account_filtered = s_wls_account_filtered.select([col(c).alias("s_"+c) for c in s_wls_account_filtered.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Wireline

# COMMAND ----------

inp3_wn_cols = ["rcis_id_cl","fa_id_cl","source_system","contact_id","account_status","service_provider","service_address_cl","sam_key","snapshot_stamp","execution_date", "service_address_no_zipcode_dq_cl","service_address_full_cl", 'service_postalcode_cl']

s_wln_account_filtered = shaw_consumer_wireline_account_df.select(inp3_wn_cols).dropDuplicates()

s_wln_account_filtered = s_wln_account_filtered.select([col(c).alias("s_"+c) for c in s_wln_account_filtered.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Combine Contact & WLS

# COMMAND ----------

s_wls_c_joined_temp = s_contact_filtered.alias('cf').join(s_wls_account_filtered.alias('wsf'),(col('cf.s_rcis_id_cl') == col('wsf.s_rcis_id_cl')) & (col('cf.s_fa_id_cl') == col('wsf.s_fa_id_cl')),how="inner")\
                                               .drop(s_wls_account_filtered.s_rcis_id_cl)\
                                               .drop(s_wls_account_filtered.s_fa_id_cl)\
                                               .drop(s_wls_account_filtered.s_contact_id)\
                                               .drop(s_wls_account_filtered.s_snapshot_stamp)\
                                               .drop(s_wls_account_filtered.s_execution_date)

s_wls_c_final = s_wls_c_joined_temp.withColumn('s_service_address_cl', lit(None).cast(StringType()))\
                                      .withColumn('s_sam_key', lit(None).cast(StringType()))\
                                      .withColumn('s_service_address_no_zipcode_dq_cl', lit(None).cast(StringType()))\
                                      .withColumn("s_account_type",lit("wireless"))\
                                      .withColumn('s_service_address_full_cl', lit(None).cast(StringType()))\
                                      .withColumn("s_service_postalcode_cl",lit(None).cast(StringType()))

# COMMAND ----------

#Step to exclude Shaw Mobile customers if 'include_shawmobile' toggle is set to 0

if var_include_shawmobile =='0':
    s_wls_c_final = s_wls_c_final.filter(col("s_service_provider")!='Shaw Mobile')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Combine Contact & WLN

# COMMAND ----------

s_wln_c_final = s_contact_filtered.alias('cf')\
                                    .join(s_wln_account_filtered.alias('wnf'),col("cf.s_contact_id")==col("wnf.s_contact_id"),how='inner')\
                                    .withColumn("s_account_type",lit("wireline"))\
                                    .drop(s_wln_account_filtered.s_contact_id)\
                                    .drop(s_wln_account_filtered.s_execution_date)\
                                    .drop(s_wln_account_filtered.s_fa_id_cl)\
                                    .drop(s_wln_account_filtered.s_rcis_id_cl)\
                                    .drop(s_wln_account_filtered.s_snapshot_stamp)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Storage

# COMMAND ----------

#output 
if var_overwrite_output=='1':
    s_wln_c_final.write.parquet(foundation_shaw_wln_c,mode='overwrite')
    
if var_overwrite_output=='1':
    s_wls_c_final.write.parquet(foundation_shaw_wls_c,mode='overwrite')
