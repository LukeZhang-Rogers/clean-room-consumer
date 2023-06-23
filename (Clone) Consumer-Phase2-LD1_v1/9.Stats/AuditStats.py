# Databricks notebook source
# MAGIC %md ## Generating Audit Stats 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC This notebook does the sanity checks of counts in output files and input files
# MAGIC
# MAGIC
# MAGIC ### Requirements:
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC   <li>The matched entity and raw data should be available at the storage layer.
# MAGIC
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the Datasets

# COMMAND ----------

# MAGIC %run ../2.Common/ADLSDesignedPaths

# COMMAND ----------

rogers_contact_df                 = spark.read.format("parquet").load(rogers_contact)
rogers_wireless_account_df        = spark.read.format("parquet").load(rogers_wireless_account)
rogers_wireline_account_df        = spark.read.format("parquet").load(rogers_wireline_account)
shaw_consumer_wireless_account_df = spark.read.format("parquet").load(shaw_consumer_wireless_account)
shaw_consumer_wireline_account_df = spark.read.format("parquet").load(shaw_consumer_wireline_account)
shaw_consumer_contact_df          = spark.read.format("parquet").load(shaw_consumer_contact)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input records

# COMMAND ----------

r_combine = rogers_contact_df.select('x_rcis_id').unionAll(rogers_wireless_account_df.select('x_rcis_id')).unionAll(rogers_wireline_account_df.select('x_rcis_id')).distinct()
s_combine = shaw_consumer_contact_df.select('rcis_id').unionAll(shaw_consumer_wireless_account_df.select('rcis_id')).unionAll(shaw_consumer_wireline_account_df.select('rcis_id')).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dropped Records

# COMMAND ----------

r_wls_c_joined      =  spark.read.format("parquet").load(foundation_rogers_wls_c)
r_wln_c_joined      =  spark.read.format("parquet").load(foundation_rogers_wln_c)
s_wls_c_joined      =  spark.read.format("parquet").load(foundation_shaw_wls_c)
s_wln_c_joined      =  spark.read.format("parquet").load(foundation_shaw_wln_c)

# COMMAND ----------

r_df = r_wls_c_joined.unionByName(r_wln_c_joined).dropDuplicates(['r_rcis_id_cl'])
s_df = s_wls_c_joined.unionByName(s_wln_c_joined).dropDuplicates(['s_rcis_id_cl'])

# COMMAND ----------

r_dropped = r_combine.join(r_df, r_combine.x_rcis_id == r_df.r_rcis_id_cl, how='left_anti')
s_dropped = s_combine.join(s_df, s_combine.rcis_id == s_df.s_rcis_id_cl, how='left_anti')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Matched & Unmatched Records

# COMMAND ----------

matched_entity_df = spark.read.format("parquet").load(matched_entity_final)

# COMMAND ----------

#unique customer
r_matched = matched_entity_df.filter((col('ROGERS_ECID').isNotNull()) & (col('SHAW_MASTER_PARTY_ID').isNotNull())).dropDuplicates(['ROGERS_ECID'])
s_matched = matched_entity_df.filter((col('ROGERS_ECID').isNotNull()) & (col('SHAW_MASTER_PARTY_ID').isNotNull())).dropDuplicates(['SHAW_MASTER_PARTY_ID'])

# COMMAND ----------

s_unmatched = matched_entity_df.filter((col('ROGERS_ECID').isNull()) & (col('SHAW_MASTER_PARTY_ID').isNotNull())).dropDuplicates(['SHAW_MASTER_PARTY_ID'])
r_unmatched = matched_entity_df.filter((col('ROGERS_ECID').isNotNull()) & (col('SHAW_MASTER_PARTY_ID').isNull())).dropDuplicates(['ROGERS_ECID'])

# COMMAND ----------

r_date = rogers_contact_df.first()['snapshot_stamp']
s_date = shaw_consumer_contact_df.first()['SNAPSHOT_STAMP']

m_date = matched_entity_df.first()['MATCHED_TIMESTAMP']


# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Stats

# COMMAND ----------

cols = ['Source', 'CustRawData', 'CustDropped', 'CustMatched','CustUnmatched', 'InputSnapshotStamp', 'MatchedTimestamp']
vals = [('ROGERS', r_combine.count(), r_dropped.count(), r_matched.count(), r_unmatched.count(), r_date ,  m_date),
        ('SHAW', s_combine.count(), s_dropped.count(), s_matched.count(), s_unmatched.count(), s_date , m_date)]

df = spark.createDataFrame(vals, cols)
# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Create and Store in DB

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS AUDIT_SUMMARY;
# MAGIC SHOW DATABASES

# COMMAND ----------

df.createOrReplaceTempView('df')

# COMMAND ----------

# MAGIC %sql
# MAGIC USE AUDIT_SUMMARY;
# MAGIC SELECT CURRENT_DATABASE();

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC CREATE TABLE IF NOT EXISTS audit_summary (
# MAGIC Source STRING,
# MAGIC CustRawData INT,
# MAGIC CustDropped INT, 
# MAGIC CustMatched INT,
# MAGIC CustUnmatched INT,
# MAGIC InputSnapshotStamp DATE,
# MAGIC MatchedTimestamp TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO audit_summary
# MAGIC SELECT * FROM df

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from audit_summary
