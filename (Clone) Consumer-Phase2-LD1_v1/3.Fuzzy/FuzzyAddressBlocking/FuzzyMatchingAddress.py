# Databricks notebook source
# MAGIC %md
# MAGIC ## Fuzzy Matching Address : Rule 33 - Match on Mailing/Service Address
# MAGIC ## Description:
# MAGIC This notebook is called  to execute the address matching rule 33 : Match on Mailing/Service Address.
# MAGIC ## Requirements:
# MAGIC - 'ADLSDesignedPaths' notebook loads Foundational tables of Rogers and Shaw Data. Foundational tables are created by doing Data Standarization/Normlization on Raw Data. 
# MAGIC - Libraries : fuzzywuzzy 0.18.0 , ceja 0.3.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List
from pyspark.sql import Column

import ceja
import fuzzywuzzy
import numpy as np
from fuzzywuzzy import fuzz

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
# MAGIC ## Create Blocking Variables
# MAGIC ### Blocking/Searching
# MAGIC - Objective:    
# MAGIC Reduce the number of possible record pair comparisons by considering only potentially linkable record pairs together. 
# MAGIC - Method:    
# MAGIC   - Define a blocking variable to split datasets into blocks.   
# MAGIC   - Only records having the same value in the blocking variable are used to create record pairs.
# MAGIC   - Iterate the process with different blocking variables to finalize.
# MAGIC - Example:    
# MAGIC Define blocking variable as first letter of last name + first number of phone number
# MAGIC For someone named “Bill Stuart” with phone number 647123456, the blocking variables is s6.
# MAGIC
# MAGIC ### Applied Blocking Variable 
# MAGIC Mailing City + Mailing Zipcode + Mailing State

# COMMAND ----------

# add Blocking Variable/ID as a new coloumn
s = shaw_c_wln_wls.withColumn('s_blocking_id',concat(shaw_c_wln_wls.s_city_cl,lit('_'),shaw_c_wln_wls.s_zipcode_cl, lit('_'),shaw_c_wln_wls.s_state_cl))
r = rogers_c_wln_wls.withColumn('r_blocking_id',concat(rogers_c_wln_wls.r_city_cl,lit('_'),rogers_c_wln_wls.r_zipcode_cl, lit('_'),rogers_c_wln_wls.r_state_cl))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Pairs for Comparison
# MAGIC Pairs for comparison will be created only when the Blocking Variable/ID matches.

# COMMAND ----------

# create pairs
pairs_df = r.join(s,r.r_blocking_id == s.s_blocking_id,"cross")
print(pairs_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Similarity Measures
# MAGIC ### Token Sort Ratio 
# MAGIC Comparing two strings by looking at tokens (e.g. unit) of the strings.  
# MAGIC Reference: https://pypi.org/project/fuzzywuzzy/
# MAGIC
# MAGIC ### Levenshtein 
# MAGIC Levenshtein distance is a string metric for measuring the difference between two sequences. The Levenshtein distance between two words is the minimum number of single-character edits (insertions, deletions or substitutions) required to change one word into the other.   
# MAGIC Reference: https://en.wikipedia.org/wiki/Levenshtein_distance
# MAGIC
# MAGIC ### Jaro Winkler
# MAGIC The Jaro-Winkler similarity is a string metric measuring edit distance between two strings.The value of Jaro distance ranges from 0 to 1. where 1 means the strings are equal and 0 means no similarity between the two strings.   
# MAGIC Reference: https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance 

# COMMAND ----------

# UDF for token sort ratio using fuzzywuzzy
udf_token_sort_ratio = udf(lambda x,y: fuzz.token_sort_ratio(x, y)/100, FloatType())

# COMMAND ----------

# For each account pair for comparison, calculate 12 similarity scores (3 scores X 4 address combinations) and get the max similarity score:
# 3 scores: Levenshtein, Jaro Winkler Similarity, Token Sort Ratio
# 4 address combinations: Shaw Mailing Address vs Rogers Mailing Address, Shaw Service Address vs Rogers Service Address, Shaw Mailing Address vs Rogers Service Address, Shaw Service Address vs Rogers Mailing Address
sim_scores = pairs_df.withColumn('address_levenshtein_s_m_r_m',1-levenshtein(pairs_df.s_mailing_address_no_zipcode_dq_cl, pairs_df.r_mailing_address_no_zipcode_dq_cl) / greatest(length(pairs_df.r_mailing_address_no_zipcode_dq_cl),length(pairs_df.r_mailing_address_no_zipcode_dq_cl))) \
                     .withColumn('address_levenshtein_s_s_r_s',1-levenshtein(pairs_df.s_service_address_no_zipcode_dq_cl, pairs_df.r_service_address_no_zipcode_dq_cl) / greatest(length(pairs_df.s_service_address_no_zipcode_dq_cl),length(pairs_df.r_service_address_no_zipcode_dq_cl))) \
                     .withColumn('address_levenshtein_s_m_r_s',1-levenshtein(pairs_df.s_mailing_address_no_zipcode_dq_cl, pairs_df.r_service_address_no_zipcode_dq_cl) / greatest(length(pairs_df.s_mailing_address_no_zipcode_dq_cl),length(pairs_df.r_service_address_no_zipcode_dq_cl))) \
                     .withColumn('address_levenshtein_s_s_r_m',1-levenshtein(pairs_df.s_service_address_no_zipcode_dq_cl, pairs_df.r_mailing_address_no_zipcode_dq_cl) / greatest(length(pairs_df.s_service_address_no_zipcode_dq_cl),length(pairs_df.r_mailing_address_no_zipcode_dq_cl))) \
                     .withColumn('address_jaro_winkler_s_m_r_m', ceja.jaro_winkler_similarity(pairs_df.s_mailing_address_no_zipcode_dq_cl, pairs_df.r_mailing_address_no_zipcode_dq_cl)) \
                     .withColumn('address_jaro_winkler_s_s_r_s', ceja.jaro_winkler_similarity(pairs_df.s_service_address_no_zipcode_dq_cl, pairs_df.r_service_address_no_zipcode_dq_cl)) \
                     .withColumn('address_jaro_winkler_s_m_r_s', ceja.jaro_winkler_similarity(pairs_df.s_mailing_address_no_zipcode_dq_cl, pairs_df.r_service_address_no_zipcode_dq_cl)) \
                     .withColumn('address_jaro_winkler_s_s_r_m', ceja.jaro_winkler_similarity(pairs_df.s_service_address_no_zipcode_dq_cl, pairs_df.r_mailing_address_no_zipcode_dq_cl)) \
                     .withColumn('address_token_sort_s_m_r_m', udf_token_sort_ratio(pairs_df.s_mailing_address_no_zipcode_dq_cl, pairs_df.r_mailing_address_no_zipcode_dq_cl)) \
                     .withColumn('address_token_sort_s_s_r_s', udf_token_sort_ratio(pairs_df.s_service_address_no_zipcode_dq_cl, pairs_df.r_service_address_no_zipcode_dq_cl)) \
                     .withColumn('address_token_sort_s_m_r_s', udf_token_sort_ratio(pairs_df.s_mailing_address_no_zipcode_dq_cl, pairs_df.r_service_address_no_zipcode_dq_cl)) \
                     .withColumn('address_token_sort_s_s_r_m', udf_token_sort_ratio(pairs_df.s_service_address_no_zipcode_dq_cl, pairs_df.r_mailing_address_no_zipcode_dq_cl)) \
.withColumn('address_score',greatest('address_levenshtein_s_m_r_m','address_levenshtein_s_s_r_s','address_levenshtein_s_m_r_s','address_levenshtein_s_s_r_m','address_jaro_winkler_s_m_r_m','address_jaro_winkler_s_s_r_s', 'address_jaro_winkler_s_m_r_s', 'address_jaro_winkler_s_s_r_m', 'address_token_sort_s_m_r_m', 'address_token_sort_s_s_r_s', 'address_token_sort_s_m_r_s','address_token_sort_s_s_r_m'))

# COMMAND ----------

# create output
output = sim_scores.withColumn('ruleset_attr', sim_scores.address_score) \
                   .withColumn('ruleset_type', lit('FUZZY')) \
                   .withColumn('ruleset_id', lit(33)) \
             .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','s_sam_key','r_sam_key','s_account_type','r_account_type','s_account_status','r_account_status','s_snapshot_stamp','r_snapshot_stamp','ruleset_id','ruleset_attr','ruleset_type')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Threshold
# MAGIC Thresholds were identified based on PEI data. We didn't do any threshold adjustment/tuning for Entire Data.

# COMMAND ----------

# filter based on Threshold
output = output.filter(output.ruleset_attr >= 0.98)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Output

# COMMAND ----------

# write output
output.write.parquet(interimop_fuzzy_address, mode='overwrite')
