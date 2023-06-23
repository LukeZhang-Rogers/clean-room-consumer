# Databricks notebook source
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
# MAGIC ## Droped records

# COMMAND ----------

r_wls_c_joined      =  spark.read.format("parquet").load(foundation_rogers_wls_c)
r_wln_c_joined      =  spark.read.format("parquet").load(foundation_rogers_wln_c)
s_wls_c_joined      =  spark.read.format("parquet").load(foundation_shaw_wls_c)
s_wln_c_joined      =  spark.read.format("parquet").load(foundation_shaw_wln_c)

# COMMAND ----------

r_df = r_wls_c_joined.unionAll(r_wln_c_joined).dropDuplicates(['r_rcis_id_cl'])
s_df = s_wls_c_joined.unionAll(s_wln_c_joined).dropDuplicates(['s_rcis_id_cl'])

# COMMAND ----------

r_dropped = r_combine.join(r_df, r_combine.x_rcis_id == r_df.r_rcis_id_cl, how='left_anti')
s_dropped = s_combine.join(s_df, s_combine.rcis_id == s_df.s_rcis_id_cl, how='left_anti')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Matched & Unmatched Records

# COMMAND ----------

matched_entity_df = spark.read.format("parquet").load(matched_entity)

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
display(df)

# COMMAND ----------

# MAGIC %md
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rogers Validation

# COMMAND ----------

display(r_dropped)

# COMMAND ----------

# rogers_contact_df                 = spark.read.format("parquet").load(rogers_contact)
# rogers_wireless_account_df        = spark.read.format("parquet").load(rogers_wireless_account)
# rogers_wireline_account_df        = spark.read.format("parquet").load(rogers_wireline_account)

# COMMAND ----------

display(rogers_contact_df.filter(col('x_rcis_id') == '454278299'))
display(rogers_wireless_account_df.filter(col('x_rcis_id') == '454278299'))
display(rogers_wireline_account_df.filter(col('x_rcis_id') == '454278299'))

# COMMAND ----------

l = [data[0] for data in r_dropped.select('x_rcis_id').collect()]

# COMMAND ----------

display(rogers_wireless_account_df.filter(col('x_rcis_id').isin(l)))
display(rogers_wireline_account_df.filter(col('x_rcis_id').isin(l)))

# COMMAND ----------

display(rogers_contact_df.filter(col('X_rcis_id') == '38565124'))

# COMMAND ----------

display(r_wln_c_joined.filter(col('r_rcis_id_cl') == '38565124'))

# COMMAND ----------

cl_rogers_contact_df                 = spark.read.format("parquet").load(cl_rogers_contact)

display(cl_rogers_contact_df.filter(col('rcis_id_cl') == '38565124'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shaw Validation

# COMMAND ----------

display(s_dropped)

# COMMAND ----------

# shaw_consumer_wireless_account_df = spark.read.format("parquet").load(shaw_consumer_wireless_account)
# shaw_consumer_wireline_account_df = spark.read.format("parquet").load(shaw_consumer_wireline_account)
# shaw_consumer_contact_df          = spark.read.format("parquet").load(shaw_consumer_contact)

# COMMAND ----------

display(shaw_consumer_contact_df.filter(col('rcis_id') == '25913316'))
display(shaw_consumer_wireless_account_df.filter(col('rcis_id') == '25913316'))
display(shaw_consumer_wireline_account_df.filter(col('rcis_id') == '25913316'))

# COMMAND ----------

v = [data[0] for data in s_dropped.select('rcis_id').collect()]

# COMMAND ----------

#display(shaw_consumer_wireless_account_df.filter(col('rcis_id').isin(v)))
print(shaw_consumer_wireline_account_df.filter(col('rcis_id').isin(v)).count())
print(shaw_consumer_contact_df.filter(col('rcis_id').isin(v)).count())

# COMMAND ----------

display(s_wln_c_joined.filter(col('s_rcis_id_cl') == '25913316'))

# COMMAND ----------

cl_shaw_consumer_contact_df                 = spark.read.format("parquet").load(cl_shaw_consumer_contact)

display(cl_shaw_consumer_contact_df.filter(col('rcis_id_cl') == '25913316'))

# COMMAND ----------

# Hence, Among 7186 dropped records in Rogers '11' dropped because of DQ and rest of the records were dropped because of it's only presents in contact (not in wls/wln).
# Hence, Among 1783 dropped records in Shaw '1783' dropped because of DQ.

# COMMAND ----------

# print(rogers_contact_df.dropDuplicates(['x_rcis_id']).count()) #10599077
# print(rogers_wireless_account_df.dropDuplicates(['x_rcis_id']).count()) #9902254
# print(rogers_wireline_account_df.dropDuplicates(['x_rcis_id']).count()) #3062252
# print(shaw_consumer_contact_df.dropDuplicates(['rcis_id']).count()) #2438553
# print(shaw_consumer_wireless_account_df.dropDuplicates(['rcis_id']).count()) #231570
# print(shaw_consumer_wireline_account_df.dropDuplicates(['rcis_id']).count()) #2387294


# unioned_contact_df      =  spark.read.format("parquet").load(unioned_contact)
# unioned_wireline_acc_df      =  spark.read.format("parquet").load(unioned_wireline_acc)
# unioned_wireless_acc_df     =  spark.read.format("parquet").load(unioned_wireless_acc)
#unique count
# print(unioned_contact_df.dropDuplicates(['rcis_id']).count()) 13030296
# print(unioned_wireless_acc_df.dropDuplicates(['rcis_id']).count()) 10133377
# print(unioned_wireline_acc_df.dropDuplicates(['rcis_id']).count()) 5447160

# #unique count
# print(r_wls_c_joined.dropDuplicates(['r_rcis_id_cl']).count()) 9902254
# print(r_wln_c_joined.dropDuplicates(['r_rcis_id_cl']).count()) 3062226
# print(s_wls_c_joined.dropDuplicates(['s_rcis_id_cl']).count()) 231570
# print(s_wln_c_joined.dropDuplicates(['s_rcis_id_cl']).count()) 2385510
