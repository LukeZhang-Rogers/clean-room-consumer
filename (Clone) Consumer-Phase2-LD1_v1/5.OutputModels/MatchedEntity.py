# Databricks notebook source
# MAGIC %md ## Building Matched Entity Table
# MAGIC
# MAGIC ### Description:
# MAGIC This notebook reads the AccountMatchedRuleset dataset to build the MatchedEntity table.
# MAGIC
# MAGIC ### Requirements:
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC   <li>The AccountMatchedRuleset dataset is available in Interim Consumption Layer</li>
# MAGIC
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC # Configurations

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../2.Common/ADLSDesignedPaths

# COMMAND ----------

# MAGIC %md
# MAGIC # Source Dataset

# COMMAND ----------

account_matched = spark.read.format("parquet").load(account_matched_ruleset)

# COMMAND ----------

# MAGIC %md
# MAGIC # Deterministic Results

# COMMAND ----------

deter_matched = account_matched.filter((col('RULESET_TYPE') == 'DETERMINISTIC') & (col('ROGERS_ECID').isNotNull()))\
                                       .select('ROGERS_ECID', 'SHAW_MASTER_PARTY_ID', 'RULESET_TYPE', 'RULESET_ATTR', 'RULESET_ID')

# COMMAND ----------

#Aggregation to customer level and selecting the minimum ruleset priority
deter_temp_1 = deter_matched.groupBy('ROGERS_ECID', 'SHAW_MASTER_PARTY_ID').agg(min('RULESET_ATTR').alias('RULESET_ATTR'))

# COMMAND ----------

deter_temp_2 = deter_temp_1.join(deter_matched, ['ROGERS_ECID', 'SHAW_MASTER_PARTY_ID', 'RULESET_ATTR'], 'left')

# COMMAND ----------

#Aggregation to customer level and selecting the minimum ruleset ID corresponding to the priority
deter_temp  = deter_temp_2.groupBy('ROGERS_ECID', 'SHAW_MASTER_PARTY_ID', 'RULESET_ATTR').agg(min('RULESET_ID').alias('RULESET_ID'))

deter_temp  = deter_temp.withColumn('DET_MATCH_FLAG', lit('Y'))

deter_final = deter_temp.select('ROGERS_ECID',\
                                'SHAW_MASTER_PARTY_ID',\
                                col('RULESET_ATTR').alias('CUSTOMER_DET_PRIORITY'),\
                                col('RULESET_ID').alias('BEST_DET_MATCH_RULESET_ID'),\
                                'DET_MATCH_FLAG')

# COMMAND ----------

# MAGIC %md
# MAGIC # Fuzzy Results

# COMMAND ----------

fuzzy_matched = account_matched.filter((col('RULESET_TYPE') == 'FUZZY') & (col('ROGERS_ECID').isNotNull()))\
                                       .select('ROGERS_ECID', 'SHAW_MASTER_PARTY_ID', 'RULESET_ATTR', 'RULESET_ID')

# COMMAND ----------

fuzzy_temp_1 = fuzzy_matched.groupBy('ROGERS_ECID', 'SHAW_MASTER_PARTY_ID').agg(max('RULESET_ATTR').alias('RULESET_ATTR'))

# COMMAND ----------

fuzzy_temp_2 = fuzzy_temp_1.join(fuzzy_matched, ['ROGERS_ECID', 'SHAW_MASTER_PARTY_ID', 'RULESET_ATTR'], 'left')

# COMMAND ----------

fuzzy_temp  = fuzzy_temp_2.groupBy('ROGERS_ECID', 'SHAW_MASTER_PARTY_ID', 'RULESET_ATTR').agg(min('RULESET_ID').alias('RULESET_ID'))

fuzzy_temp  = fuzzy_temp.withColumn('FUZZY_MATCH_FLAG', lit('Y'))

fuzzy_final = fuzzy_temp.select('ROGERS_ECID',\
                                'SHAW_MASTER_PARTY_ID',\
                                col('RULESET_ATTR').alias('CUSTOMER_FUZZY_SCORE'),\
                                col('RULESET_ID').alias('BEST_FUZZY_MATCH_RULESET_ID'),\
                                'FUZZY_MATCH_FLAG')

# COMMAND ----------

# MAGIC %md
# MAGIC # Probabilistic Results

# COMMAND ----------

prob_matched = account_matched.filter((col('RULESET_TYPE') == 'PROBABILISTIC') & (col('ROGERS_ECID').isNotNull()))\
                                      .select('ROGERS_ECID', 'SHAW_MASTER_PARTY_ID', 'RULESET_ATTR', 'RULESET_ID')

# COMMAND ----------

prob_temp_1 = prob_matched.groupBy('ROGERS_ECID', 'SHAW_MASTER_PARTY_ID').agg(max('RULESET_ATTR').alias('RULESET_ATTR'))

# COMMAND ----------

prob_temp_2 = prob_temp_1.join(prob_matched, ['ROGERS_ECID', 'SHAW_MASTER_PARTY_ID', 'RULESET_ATTR'], 'left')

# COMMAND ----------

prob_temp  = prob_temp_2.groupBy('ROGERS_ECID', 'SHAW_MASTER_PARTY_ID', 'RULESET_ATTR').agg(min('RULESET_ID').alias('RULESET_ID'))

prob_temp  = prob_temp.withColumn('PROB_MATCH_FLAG', lit('Y'))

prob_final = prob_temp.select('ROGERS_ECID', 'SHAW_MASTER_PARTY_ID',\
                              col('RULESET_ATTR').alias('CUSTOMER_PROB_CONFIDENCE_LVL'),\
                              col('RULESET_ID').alias('BEST_PROB_MATCH_RULESET_ID'),\
                              'PROB_MATCH_FLAG')

# COMMAND ----------

# MAGIC %md
# MAGIC # Combine All

# COMMAND ----------

joined_fp = fuzzy_final.join(prob_final, ['ROGERS_ECID', 'SHAW_MASTER_PARTY_ID'], 'full')

# COMMAND ----------

joined_deter_fp = joined_fp.join(deter_final, ['ROGERS_ECID', 'SHAW_MASTER_PARTY_ID'], 'full')

# COMMAND ----------

joined_deter_fp = joined_deter_fp.withColumn('DET_MATCH_IND', when(col('DET_MATCH_FLAG')== 'Y',lit('Y')).otherwise(lit('N')))\
                                 .withColumn('FUZZY_MATCH_IND', when(col('FUZZY_MATCH_FLAG')== 'Y', lit('Y')).otherwise(lit('N')))\
                                 .withColumn('PROB_MATCH_IND', when(col('PROB_MATCH_FLAG')== 'Y', lit('Y')).otherwise(lit('N')))\
                                 .drop('DET_MATCH_FLAG', 'FUZZY_MATCH_FLAG', 'PROB_MATCH_FLAG')

# COMMAND ----------

# MAGIC %md
# MAGIC # Select Attributes

# COMMAND ----------

remaining_attr = account_matched.select('ROGERS_ECID', 'SHAW_MASTER_PARTY_ID', 'ROGERS_SNAPSHOT_TIMESTAMP', 'SHAW_SNAPSHOT_TIMESTAMP').dropDuplicates()

matched = joined_deter_fp.join(remaining_attr, ['ROGERS_ECID','SHAW_MASTER_PARTY_ID'], how='left')

# COMMAND ----------

# MAGIC %md
# MAGIC # Appending Unmatched

# COMMAND ----------

r_wls_c_joined      =  spark.read.format("parquet").load(foundation_rogers_wls_c)
r_wln_c_joined      =  spark.read.format("parquet").load(foundation_rogers_wln_c)
s_wls_c_joined      =  spark.read.format("parquet").load(foundation_shaw_wls_c)
s_wln_c_joined      =  spark.read.format("parquet").load(foundation_shaw_wln_c)

r_joined = r_wln_c_joined.unionAll(r_wls_c_joined)
s_joined = s_wln_c_joined.unionAll(s_wls_c_joined)

# COMMAND ----------

r_joined_temp = r_joined.select(col('r_rcis_id_cl').alias('ROGERS_ECID'), col('r_snapshot_stamp').alias('ROGERS_SNAPSHOT_TIMESTAMP')).dropDuplicates()
s_joined_temp = s_joined.select(col('s_rcis_id_cl').alias('SHAW_MASTER_PARTY_ID'), col('s_snapshot_stamp').alias('SHAW_SNAPSHOT_TIMESTAMP')).dropDuplicates()

r_unmatched = r_joined_temp.join(matched, ['ROGERS_ECID'], how='left_anti')
s_unmatched = s_joined_temp.join(matched, ['SHAW_MASTER_PARTY_ID'], how='left_anti')

r_s_unmatched = r_unmatched.unionByName(s_unmatched, allowMissingColumns=True)

unmatched = r_s_unmatched.withColumn('DET_MATCH_IND', lit('N'))\
                         .withColumn('FUZZY_MATCH_IND', lit('N'))\
                         .withColumn('PROB_MATCH_IND', lit('N'))

# COMMAND ----------

full_matched_entity = matched.unionByName(unmatched, allowMissingColumns=True)

# COMMAND ----------

matched_entity_final = full_matched_entity.withColumn('MATCHED_ENTITY_GUID', monotonically_increasing_id())\
                                    .withColumn('MATCHED_TIMESTAMP', current_timestamp())\
                                    .withColumn('SNAPSHOT_STAMP', lit(date).cast("date"))

# COMMAND ----------

matched_entity_table = matched_entity_final.select('MATCHED_ENTITY_GUID', 'ROGERS_ECID','SHAW_MASTER_PARTY_ID','DET_MATCH_IND', 'FUZZY_MATCH_IND', 'PROB_MATCH_IND', 'BEST_DET_MATCH_RULESET_ID',\
                                                   'BEST_FUZZY_MATCH_RULESET_ID', 'BEST_PROB_MATCH_RULESET_ID',\
                                                   col('CUSTOMER_DET_PRIORITY').cast('int'),\
                                                   'CUSTOMER_FUZZY_SCORE', 'CUSTOMER_PROB_CONFIDENCE_LVL',\
                                                   to_timestamp(col('ROGERS_SNAPSHOT_TIMESTAMP')).alias('ROGERS_SNAPSHOT_TIMESTAMP'),\
                                                   to_timestamp(col('SHAW_SNAPSHOT_TIMESTAMP')).alias('SHAW_SNAPSHOT_TIMESTAMP'),\
                                                   'SNAPSHOT_STAMP',\
                                                   'MATCHED_TIMESTAMP')

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to Storage

# COMMAND ----------

matched_entity_table.write.parquet(matched_entity, mode='overwrite')
