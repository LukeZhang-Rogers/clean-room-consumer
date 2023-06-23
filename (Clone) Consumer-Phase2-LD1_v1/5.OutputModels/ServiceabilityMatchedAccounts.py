# Databricks notebook source
# MAGIC %md ## Building Serviceability Matched Account Table
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC This notebook reads the foundation datasets that are generated from Rogers and Shaw cleansed datasets and cleansed version of serviceability reference tables to build the Serviceability Matched dataset.
# MAGIC
# MAGIC ### Requirements:
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC   <li>The foundation data and servicability reference data should be available in the Processed Zone</li>
# MAGIC
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configuration

# COMMAND ----------

# Imports
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../2.Common/ADLSDesignedPaths

# COMMAND ----------

# MAGIC %md
# MAGIC ##Source Datasets

# COMMAND ----------

r_wls_c_joined      =  spark.read.format("parquet").load(foundation_rogers_wls_c)
r_wln_c_joined      =  spark.read.format("parquet").load(foundation_rogers_wln_c)
s_wls_c_joined      =  spark.read.format("parquet").load(foundation_shaw_wls_c)
s_wln_c_joined      =  spark.read.format("parquet").load(foundation_shaw_wln_c)

s_wln_serviceability      =  spark.read.format("parquet").load(cl_shaw_wireline_serviceability)
r_wln_serviceability      =  spark.read.format("parquet").load(cl_rogers_wireline_serviceability)
r_wls_serviceability      =  spark.read.format("parquet").load(cl_rogers_wireless_serviceability)

# COMMAND ----------

s_wln_serviceability = s_wln_serviceability.withColumn('SERVICEABILITY_SOURCE', lit('Shaw')).withColumn('SERVICEABILITY_type', lit('Wireline'))
r_wln_serviceability = r_wln_serviceability.withColumn('SERVICEABILITY_SOURCE', lit('Rogers')).withColumn('SERVICEABILITY_type', lit('Wireline'))
r_wls_serviceability = r_wls_serviceability.withColumn('SERVICEABILITY_SOURCE', lit('Rogers')).withColumn('SERVICEABILITY_type', lit('Wireless'))

# COMMAND ----------

s_wln_serviceability = s_wln_serviceability.select('SERVICEABILITY_SOURCE', 'SERVICEABILITY_type', 'serviceability_address_full_cl', 'address_id')
r_wln_serviceability = r_wln_serviceability.select('SERVICEABILITY_SOURCE', 'SERVICEABILITY_type', 'serviceability_address_full_cl', col('sam_key').alias('sam_key_serviceability'))
r_wls_serviceability = r_wls_serviceability.select('SERVICEABILITY_SOURCE', 'SERVICEABILITY_type', 'serviceability_address_full_cl', 'postal_code_cl')


r_wls_c_joined       = r_wls_c_joined.select(col('r_rcis_id_cl').alias('rcis_id'),\
                                             col('r_fa_id_cl').alias('fa_id'),\
                                             col('r_snapshot_stamp').alias('snapshot_stamp'),\
                                             col('r_mailing_address_full_cl').alias('mailing_address_full'),\
                                             col('r_service_provider').alias('service_provider'),\
                                             col('r_sam_key').alias('sam_key'),\
                                             col('r_service_address_full_cl').alias('service_address_full'),\
                                             col('r_account_type').alias('account_type'),\
                                             col('r_service_postalcode_cl').alias('service_postalcode'),\
                                             col('r_mailing_postalcode_cl').alias('mailing_postalcode'))


r_wln_c_joined       = r_wln_c_joined.select(col('r_rcis_id_cl').alias('rcis_id'),\
                                             col('r_fa_id_cl').alias('fa_id'),\
                                             col('r_snapshot_stamp').alias('snapshot_stamp'),\
                                             col('r_mailing_address_full_cl').alias('mailing_address_full'),\
                                             col('r_service_provider').alias('service_provider'),\
                                             col('r_sam_key').alias('sam_key'),\
                                             col('r_service_address_full_cl').alias('service_address_full'),\
                                             col('r_account_type').alias('account_type'),\
                                             col('r_service_postalcode_cl').alias('service_postalcode'),\
                                             col('r_mailing_postalcode_cl').alias('mailing_postalcode'))


s_wls_c_joined       = s_wls_c_joined.select(col('s_rcis_id_cl').alias('rcis_id'),\
                                             col('s_fa_id_cl').alias('fa_id'),\
                                             col('s_snapshot_stamp').alias('snapshot_stamp'),\
                                             col('s_mailing_address_full_cl').alias('mailing_address_full'),\
                                             col('s_service_provider').alias('service_provider'),\
                                             col('s_sam_key').alias('sam_key'),\
                                             col('s_service_address_full_cl').alias('service_address_full'),\
                                             col('s_account_type').alias('account_type'),\
                                             col('s_service_postalcode_cl').alias('service_postalcode'),\
                                             col('s_mailing_postalcode_cl').alias('mailing_postalcode'))


s_wln_c_joined       = s_wln_c_joined.select(col('s_rcis_id_cl').alias('rcis_id'),\
                                             col('s_fa_id_cl').alias('fa_id'),\
                                             col('s_snapshot_stamp').alias('snapshot_stamp'),\
                                             col('s_mailing_address_full_cl').alias('mailing_address_full'),\
                                             col('s_service_provider').alias('service_provider'),\
                                             col('s_sam_key').alias('sam_key'),\
                                             col('s_service_address_full_cl').alias('service_address_full'),\
                                             col('s_account_type').alias('account_type'),\
                                             col('s_service_postalcode_cl').alias('service_postalcode'),\
                                             col('s_mailing_postalcode_cl').alias('mailing_postalcode'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Rules

# COMMAND ----------

# MAGIC %md
# MAGIC 1 . 100% Match on Rogers Mailing Address -> Shaw CH Serviceability Address

# COMMAND ----------

Rule1a = r_wls_c_joined.join(s_wln_serviceability , r_wls_c_joined.mailing_address_full == s_wln_serviceability.serviceability_address_full_cl ,how='inner')\
                       .withColumn("Ruleset_ID",lit("35"))

Rule1b = r_wln_c_joined.join(s_wln_serviceability , r_wln_c_joined.mailing_address_full == s_wln_serviceability.serviceability_address_full_cl ,how='inner')\
                       .withColumn("Ruleset_ID",lit("35"))

Rule1  = Rule1a.unionAll(Rule1b)

Rule1  = Rule1.withColumnRenamed('address_id', 'SERVICEABILITY_ID_MAILING_ADDRESS').dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC 2 . 100% Match on Rogers Service Address -> Shaw CH Serviceability Address

# COMMAND ----------

Rule2 = r_wln_c_joined.join(s_wln_serviceability , r_wln_c_joined.service_address_full == s_wln_serviceability.serviceability_address_full_cl ,how='inner')\
                      .withColumn("Ruleset_ID",lit("36"))

Rule2 = Rule2.withColumnRenamed('address_id', 'SERVICEABILITY_ID_SERVICE_ADDRESS').dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC 3 . 100% Match on Shaw Mailing Address -> Rogers CH Serviceability Address

# COMMAND ----------

Rule3a = s_wls_c_joined.join(r_wln_serviceability , s_wls_c_joined.mailing_address_full == r_wln_serviceability.serviceability_address_full_cl ,how='inner')\
                       .withColumn("Ruleset_ID",lit("37"))

Rule3b = s_wln_c_joined.join(r_wln_serviceability , s_wln_c_joined.mailing_address_full == r_wln_serviceability.serviceability_address_full_cl ,how='inner')\
                       .withColumn("Ruleset_ID",lit("37"))

Rule3 = Rule3a.unionAll(Rule3b)

Rule3 = Rule3.withColumnRenamed('sam_key_serviceability', 'SERVICEABILITY_ID_MAILING_ADDRESS').dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC 4 . 100% Match on Shaw Service Address -> Rogers CH Serviceability Address

# COMMAND ----------

Rule4 = s_wln_c_joined.join(r_wln_serviceability , s_wln_c_joined.service_address_full == r_wln_serviceability.serviceability_address_full_cl ,how='inner')\
                      .withColumn("Ruleset_ID",lit("38"))

Rule4 = Rule4.withColumnRenamed('sam_key_serviceability', 'SERVICEABILITY_ID_SERVICE_ADDRESS').dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC 5 . 100% Match on Shaw Mailing Address -> Rogers WLS Serviceability Address

# COMMAND ----------

Rule5a = s_wls_c_joined.join(r_wls_serviceability , s_wls_c_joined.mailing_postalcode == r_wls_serviceability.postal_code_cl ,how='inner')\
                       .withColumn("Ruleset_ID",lit("39"))

Rule5b = s_wln_c_joined.join(r_wls_serviceability , s_wln_c_joined.mailing_postalcode == r_wls_serviceability.postal_code_cl ,how='inner')\
                       .withColumn("Ruleset_ID",lit("39"))

Rule5 = Rule5a.unionAll(Rule5b)

Rule5 = Rule5.withColumnRenamed('postal_code_cl', 'SERVICEABILITY_ID_MAILING_ADDRESS').dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC 6 . 100% Match on Shaw Service Address -> Rogers WLS Serviceability Address

# COMMAND ----------

Rule6 = s_wln_c_joined.join(r_wls_serviceability , s_wln_c_joined.service_postalcode == r_wls_serviceability.postal_code_cl ,how='inner')\
                      .withColumn("Ruleset_ID",lit("40"))

Rule6 = Rule6.withColumnRenamed('postal_code_cl', 'SERVICEABILITY_ID_SERVICE_ADDRESS').dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC # Matched Dataset

# COMMAND ----------

union_1 = Rule1.unionAll(Rule3).unionAll(Rule5)
union_2 = Rule2.unionAll(Rule4).unionAll(Rule6)

# COMMAND ----------

matched = union_1.unionByName(union_2, allowMissingColumns=True)

# COMMAND ----------

matched_dataset_temp_1 = matched.groupBy('service_provider', 'rcis_id', 'fa_id', 'sam_key', 'account_type', 'mailing_address_full', 'service_address_full',\
                                         'SERVICEABILITY_SOURCE', 'SERVICEABILITY_type', 'snapshot_stamp')\
                                .agg(collect_set("SERVICEABILITY_ID_MAILING_ADDRESS"), collect_set("SERVICEABILITY_ID_SERVICE_ADDRESS"))

# COMMAND ----------

matched_dataset_temp_2 = matched_dataset_temp_1.withColumn("tmp", arrays_zip("collect_set(SERVICEABILITY_ID_MAILING_ADDRESS)", "collect_set(SERVICEABILITY_ID_SERVICE_ADDRESS)"))\
                                               .withColumn("tmp", explode("tmp"))\
                                               .select('service_provider', 'rcis_id', 'fa_id', 'sam_key', 'account_type', 'mailing_address_full', 'service_address_full', 'SERVICEABILITY_SOURCE',\
                                                       'SERVICEABILITY_type', 'snapshot_stamp', \
                                                       col("tmp.collect_set(SERVICEABILITY_ID_MAILING_ADDRESS)"),\
                                                       col("tmp.collect_set(SERVICEABILITY_ID_SERVICE_ADDRESS)"))

# COMMAND ----------

matched_dataset = matched_dataset_temp_2.withColumn('Source', when((col('service_provider')=='Wireless Rogers') |  (col('service_provider')=='Fido') | (col('service_provider')=='Wireless Fido') |\
                                                                   (col('service_provider')=='Rogers'), lit('Rogers'))\
                                                    .otherwise(lit('Shaw')))\
                                        .withColumn('account_type', initcap(col('account_type')))

# COMMAND ----------

matched_dataset = matched_dataset.withColumn("SERVICEABILITY_ID_MAILING_ADDRESS", concat_ws("",col("collect_set(SERVICEABILITY_ID_MAILING_ADDRESS)")))\
                                 .withColumn("SERVICEABILITY_ID_SERVICE_ADDRESS", concat_ws("",col("collect_set(SERVICEABILITY_ID_SERVICE_ADDRESS)")))

# COMMAND ----------

matched_final = matched_dataset.select('Source', 'rcis_id', 'fa_id', 'sam_key', 'account_type', 'mailing_address_full', 'service_address_full', 'SERVICEABILITY_SOURCE', 'SERVICEABILITY_type', 'SERVICEABILITY_ID_MAILING_ADDRESS', 'SERVICEABILITY_ID_SERVICE_ADDRESS', 'snapshot_stamp').dropDuplicates()

# COMMAND ----------

matched_final = matched_final.withColumn('MATCHED_TIMESTAMP', current_timestamp())\
                             .withColumn('SNAPSHOT_STAMP', lit(date).cast("date"))\
                             .withColumn('SERVICEABILITY_ID_MAILING_ADDRESS', when(col('SERVICEABILITY_ID_MAILING_ADDRESS')=='', lit(None)).otherwise(col('SERVICEABILITY_ID_MAILING_ADDRESS')))\
                             .withColumn('SERVICEABILITY_ID_SERVICE_ADDRESS', when(col('SERVICEABILITY_ID_SERVICE_ADDRESS')=='', lit(None)).otherwise(col('SERVICEABILITY_ID_SERVICE_ADDRESS')))      

# COMMAND ----------

serviceability_table = matched_final.select(col('Source').alias('SOURCE'),\
                                            col('rcis_id').alias('RCIS_ID'),\
                                            col('fa_id').alias('FA_ID'),\
                                            col('sam_key').alias('SAM_KEY'),\
                                            col('account_type').alias('ACCOUNT_TYPE'),\
                                            col('mailing_address_full').alias('MAILING_ADDRESS'),\
                                            col('service_address_full').alias('SERVICE_ADDRESS'),\
                                            'SERVICEABILITY_SOURCE',\
                                            'SERVICEABILITY_TYPE',\
                                            'SERVICEABILITY_ID_MAILING_ADDRESS',\
                                            'SERVICEABILITY_ID_SERVICE_ADDRESS',\
                                            'SNAPSHOT_STAMP',\
                                            'MATCHED_TIMESTAMP')

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to Storage

# COMMAND ----------

serviceability_table.write.parquet(serviceability_matched,mode='overwrite')
