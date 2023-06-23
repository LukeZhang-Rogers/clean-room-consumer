# Databricks notebook source
# MAGIC %md ## Building UCA
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC
# MAGIC This notebooks builds the Unified Customer Attributes (Input Models) required to be build from the raw data.
# MAGIC
# MAGIC
# MAGIC ### Requirements:
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC   <li>All raw data should be available in the Raw Zone</li>
# MAGIC
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurations

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../2.Common/ADLSDesignedPaths

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Datasets

# COMMAND ----------

rogers_contact_df                 = spark.read.format("delta").load(rogers_contact)
rogers_wireless_account_df        = spark.read.format("delta").load(rogers_wireless_account)
rogers_wireline_account_df        = spark.read.format("delta").load(rogers_wireline_account)
rogers_wireless_service_df        = spark.read.format("delta").load(rogers_wireless_service)

shaw_consumer_wireless_account_df = spark.read.format("delta").load(shaw_consumer_wireless_account)
shaw_consumer_wireline_account_df = spark.read.format("delta").load(shaw_consumer_wireline_account)
shaw_consumer_contact_df          = spark.read.format("delta").load(shaw_consumer_contact)
shaw_consumer_wireless_service_df = spark.read.format("delta").load(shaw_consumer_wireless_service)

# COMMAND ----------

rogers_contact_df = rogers_contact_df.withColumnRenamed('x_rcis_id', 'rcis_id')
rogers_wireline_account_df = rogers_wireline_account_df.withColumnRenamed('x_rcis_id', 'rcis_id')
rogers_wireless_account_df = rogers_wireless_account_df.withColumnRenamed('x_rcis_id', 'rcis_id')

# COMMAND ----------

rogers_wireline_account_df = rogers_wireline_account_df.withColumnRenamed('bulk_tennant', 'bulk_tenant')
shaw_consumer_wireline_account_df = shaw_consumer_wireline_account_df.withColumnRenamed('SERVICE_ADDRESS_WIRELESS_COVERAGE_QUALITY', 'sa_wireless_coverage_quality')

shaw_consumer_wireless_service_df = shaw_consumer_wireless_service_df.withColumnRenamed('CURRENT_RATE_PLAN_EFFECTIVE_DATE', 'current_rate_plan_effective_dt')
shaw_consumer_wireless_service_df = shaw_consumer_wireless_service_df.withColumnRenamed('CURRENT_RATE_PLAN_DATA_ALLOTMENT', 'current_rate_plan_data_allotmt')

# COMMAND ----------

rogers_contact_df          = rogers_contact_df.withColumn('source', lit('Rogers'))
rogers_wireline_account_df = rogers_wireline_account_df.withColumn('source', lit('Rogers'))
rogers_wireless_account_df = rogers_wireless_account_df.withColumn('source', lit('Rogers'))
rogers_wireless_service_df = rogers_wireless_service_df.withColumn('source', lit('Rogers'))

shaw_consumer_contact_df          = shaw_consumer_contact_df.withColumn('source', lit('Shaw'))
shaw_consumer_wireline_account_df = shaw_consumer_wireline_account_df.withColumn('source', lit('Shaw'))
shaw_consumer_wireless_account_df = shaw_consumer_wireless_account_df.withColumn('source', lit('Shaw'))
shaw_consumer_wireless_service_df = shaw_consumer_wireless_service_df.withColumn('source', lit('Shaw'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union

# COMMAND ----------

merged_contact  = rogers_contact_df.unionByName(shaw_consumer_contact_df, allowMissingColumns=True)
merged_wln_acc  = rogers_wireline_account_df.unionByName(shaw_consumer_wireline_account_df, allowMissingColumns=True)
merged_wls_acc  = rogers_wireless_account_df.unionByName(shaw_consumer_wireless_account_df, allowMissingColumns=True)
merged_wls_ser  = rogers_wireless_service_df.unionByName(shaw_consumer_wireless_service_df, allowMissingColumns=True)

# COMMAND ----------

merged_contact = merged_contact.withColumn('ingestion_date', current_timestamp())
merged_wln_acc = merged_wln_acc.withColumn('ingestion_date', current_timestamp())
merged_wls_acc = merged_wls_acc.withColumn('ingestion_date', current_timestamp())
merged_wls_ser = merged_wls_ser.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

merged_contact = merged_contact.withColumn('snapshot_stamp', lit(date).cast('date'))
merged_wln_acc = merged_wln_acc.withColumn('snapshot_stamp', lit(date).cast('date'))
merged_wls_acc = merged_wls_acc.withColumn('snapshot_stamp', lit(date).cast('date'))
merged_wls_ser = merged_wls_ser.withColumn('snapshot_stamp', lit(date).cast('date'))

# COMMAND ----------

#Changing all column names to uppercase as requested by business on LD1
for col in merged_contact.columns:
    merged_contact = merged_contact.withColumnRenamed(col, col.upper())

for col in merged_wln_acc.columns:
    merged_wln_acc = merged_wln_acc.withColumnRenamed(col, col.upper())

for col in merged_wls_acc.columns:
    merged_wls_acc = merged_wls_acc.withColumnRenamed(col, col.upper())

for col in merged_wls_ser.columns:
    merged_wls_ser = merged_wls_ser.withColumnRenamed(col, col.upper())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Storage

# COMMAND ----------

merged_contact.write.parquet(unioned_contact,mode='overwrite')
merged_wln_acc.write.parquet(unioned_wireline_acc,mode='overwrite')
merged_wls_acc.write.parquet(unioned_wireless_acc,mode='overwrite')
merged_wls_ser.write.parquet(unioned_wireless_ser,mode='overwrite')

# COMMAND ----------

#Load input data, change columns to upper case and write to target storage
shaw_wireline_serviceability_inputdf      = spark.read.format('delta').load(shaw_wireline_serviceability)
rogers_wireline_serviceability_inputdf    = spark.read.format('delta').load(rogers_wireline_serviceability)
rogers_wireless_serviceability_inputdf    = spark.read.format('delta').load(rogers_wireless_serviceability)
rogers_service_qualification_inputdf      = spark.read.format('delta').load(rogers_service_qualification)
rogers_service_qualification_item_inputdf = spark.read.format('delta').load(rogers_service_qualification_item)

#Changing all column names to uppercase as requested by business on LD1
for col in shaw_wireline_serviceability_inputdf.columns:
    shaw_wireline_serviceability_inputdf = shaw_wireline_serviceability_inputdf.withColumnRenamed(col, col.upper())

for col in rogers_wireline_serviceability_inputdf.columns:
    rogers_wireline_serviceability_inputdf = rogers_wireline_serviceability_inputdf.withColumnRenamed(col, col.upper())

for col in rogers_wireless_serviceability_inputdf.columns:
    rogers_wireless_serviceability_inputdf = rogers_wireless_serviceability_inputdf.withColumnRenamed(col, col.upper())

for col in rogers_service_qualification_inputdf.columns:
    rogers_service_qualification_inputdf = rogers_service_qualification_inputdf.withColumnRenamed(col, col.upper())

for col in rogers_service_qualification_item_inputdf.columns:
    rogers_service_qualification_item_inputdf = rogers_service_qualification_item_inputdf.withColumnRenamed(col, col.upper())


# COMMAND ----------

#Write all files to Output Staging

shaw_wireline_serviceability_inputdf.write.parquet(shaw_wln_serviceability,mode='overwrite')
rogers_wireline_serviceability_inputdf.write.parquet(rogers_geo_serviceability,mode='overwrite')
rogers_wireless_serviceability_inputdf.write.parquet(rogers_wls_serviceability,mode='overwrite')
rogers_service_qualification_inputdf.write.parquet(rogers_ser_qualification,mode='overwrite')
rogers_service_qualification_item_inputdf.write.parquet(rogers_ser_qualification_item,mode='overwrite')
