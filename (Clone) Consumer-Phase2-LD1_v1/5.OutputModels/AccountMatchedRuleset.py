# Databricks notebook source
# MAGIC %md ## Building Account Matched Ruleset Table
# MAGIC
# MAGIC ### Description:
# MAGIC This notebook reads the fuzzy, probabilistic and deterministic matched datasets to build the AccountMatchedRuleset output model.
# MAGIC
# MAGIC ### Requirements:
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC   <li>The matched datasets are available in the Interim Consumption Zone</li>
# MAGIC
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../2.Common/ADLSDesignedPaths

# COMMAND ----------

# MAGIC %md
# MAGIC # Source Notebooks

# COMMAND ----------

fuzzy_customer_path = interimop_fuzzy_customer
fuzzy_address_path  = interimop_fuzzy_address

prob_customer_path  = interimop_prob_customer
prob_address_path   = interimop_prob_address

deter_path          = interimop_det_matched

# COMMAND ----------

fuzzy_customer_data = spark.read.format("parquet").load(fuzzy_customer_path)
fuzzy_address_data = spark.read.format("parquet").load(fuzzy_address_path)


prob_customer_data = spark.read.format("parquet").load(prob_customer_path)
prob_address_data = spark.read.format("parquet").load(prob_address_path)

deter_path_data = spark.read.format("parquet").load(deter_path)

# COMMAND ----------

fuzzy_path_data = fuzzy_customer_data.unionAll(fuzzy_address_data).dropDuplicates()
prob_path_data = prob_customer_data.unionAll(prob_address_data).dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC # Modify Deterministic 

# COMMAND ----------

deter_path_data = deter_path_data.withColumn('ruleset_type', lit('DETERMINISTIC'))\
                            .withColumn('ruleset_attr', when(col('Ruleset_ID') == '30', '43')\
							.when(col('Ruleset_ID') == '31', '44')\
							.when(col('Ruleset_ID') == '32', '45')\
							.when(col('Ruleset_ID') == '35', '16')\
							.when(col('Ruleset_ID') == '36', '17')\
							.when(col('Ruleset_ID') == '37', '18')\
							.when(col('Ruleset_ID') == '38', '19')\
							.when(col('Ruleset_ID') == '39', '20')\
							.when(col('Ruleset_ID') == '40', '21')\
							.when(col('Ruleset_ID') == '41', '22')\
							.when(col('Ruleset_ID') == '42', '23')\
							.when(col('Ruleset_ID') == '43', '24')\
							.when(col('Ruleset_ID') == '44', '25')\
							.when(col('Ruleset_ID') == '45', '26')\
							.when(col('Ruleset_ID') == '46', '27')\
							.when(col('Ruleset_ID') == '47', '28')\
							.when(col('Ruleset_ID') == '48', '29')\
							.when(col('Ruleset_ID') == '49', '30')\
							.when(col('Ruleset_ID') == '50', '31')\
							.when(col('Ruleset_ID') == '51', '32')\
							.when(col('Ruleset_ID') == '52', '33')\
							.when(col('Ruleset_ID') == '53', '34')\
							.when(col('Ruleset_ID') == '54', '35')\
							.when(col('Ruleset_ID') == '55', '36')\
							.when(col('Ruleset_ID') == '56', '37')\
							.when(col('Ruleset_ID') == '57', '38')\
							.when(col('Ruleset_ID') == '58', '39')\
							.when(col('Ruleset_ID') == '59', '40')\
							.when(col('Ruleset_ID') == '60', '41')\
							.when(col('Ruleset_ID') == '61', '42')\
							.otherwise(col('Ruleset_ID')))\
                            .withColumnRenamed('Ruleset_ID', 'ruleset_id')

# COMMAND ----------

deter_essential = deter_path_data.select('s_fa_id_cl', 'r_fa_id_cl', 's_rcis_id_cl', 'r_rcis_id_cl', "s_sam_key", "r_sam_key",  "s_account_type", "r_account_type", "s_account_status", "r_account_status",\
                                         's_snapshot_stamp', 'r_snapshot_stamp','ruleset_id', 'ruleset_attr', 'ruleset_type').dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC # Account Matched (Combine all three)

# COMMAND ----------

f_p = fuzzy_path_data.unionAll(prob_path_data)
account_matched = f_p.unionAll(deter_essential)

# COMMAND ----------

#Temporary ID added for joins
account_master_table = account_matched.withColumn('ACCOUNT_MATCHED_RULESET_GUID', monotonically_increasing_id())\
                                      .withColumn('MATCHED_TIMESTAMP', current_timestamp())\
                                      .withColumn('SNAPSHOT_STAMP', lit(date).cast("date"))\
                                       .drop('s_sam_key')

# lowercase for consistency
account_master_table_renamed = account_master_table.select('ACCOUNT_MATCHED_RULESET_GUID',\
                                                           lower(col('r_rcis_id_cl')).alias('ROGERS_ECID'),\
                                                           lower(col('r_fa_id_cl')).alias('ROGERS_ACCOUNT_ID'),\
                                                           lower(col('r_sam_key')).alias('ROGERS_SAM_KEY'),\
                                                           lower(col('r_account_type')).alias('ROGERS_ACCOUNT_TYPE'),\
                                                           lower(col('r_account_status')).alias('ROGERS_ACCOUNT_STATUS'),\
                                                           lower(col('s_rcis_id_cl')).alias('SHAW_MASTER_PARTY_ID'),\
                                                           lower(col('s_fa_id_cl')).alias('SHAW_ACCOUNT_ID'),\
                                                           lower(col('s_account_type')).alias('SHAW_ACCOUNT_TYPE'),\
                                                           lower(col('s_account_status')).alias('SHAW_ACCOUNT_STATUS'),\
                                                           col('ruleset_id').cast('int').alias('RULESET_ID'),\
                                                           upper(col('ruleset_type')).alias('RULESET_TYPE'),\
                                                           col('ruleset_attr').cast('double').alias('RULESET_ATTR'),\
                                                           to_timestamp(lower(col('r_snapshot_stamp'))).alias('ROGERS_SNAPSHOT_TIMESTAMP'),\
                                                           to_timestamp(lower(col('s_snapshot_stamp'))).alias('SHAW_SNAPSHOT_TIMESTAMP'),\
                                                           'SNAPSHOT_STAMP',\
                                                           'MATCHED_TIMESTAMP')

# COMMAND ----------

# MAGIC %md
# MAGIC # Store the Output

# COMMAND ----------

account_master_table_renamed.write.parquet(account_matched_ruleset,mode='overwrite')
