# Databricks notebook source
# MAGIC %md
# MAGIC ## Fuzzy Matching Address : Rule 33 - Match on Mailing/Service Address (Unit Testing)
# MAGIC ## Description:
# MAGIC This notebook is called to execute Unit Testing for address matching rule 33 : Match on Mailing/Service Address.
# MAGIC ## Requirements:
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
# MAGIC ## Create Unit Test Data

# COMMAND ----------

# create rogers data schema
schema_rogers_address = StructType([StructField("r_rcis_id_cl", IntegerType(), True)\
                       ,StructField("r_fa_id_cl", StringType(), True)\
                       ,StructField("r_mailing_address_no_zipcode_dq_cl", StringType(), True)\
                       ,StructField("r_service_address_no_zipcode_dq_cl", StringType(), True)\
                      ,StructField("r_account_type", StringType(), True)])

# COMMAND ----------

# create shaw data schema
schema_shaw_address = StructType([StructField("s_rcis_id_cl", IntegerType(), True)\
                       ,StructField("s_fa_id_cl", StringType(), True)\
                       ,StructField("s_mailing_address_no_zipcode_dq_cl", StringType(), True)\
                       ,StructField("s_service_address_no_zipcode_dq_cl", StringType(), True)\
                      ,StructField("s_account_type", StringType(), True)])

# COMMAND ----------

# populate rogers data
r_row =Row('r_rcis_id_cl',
            'r_fa_id_cl','r_mailing_address_no_zipcode_dq_cl','r_service_address_no_zipcode_dq_cl','account_type')
r1 = r_row(87207252,221356107,'25 parker street unit 1010 stouffville on l4a0s4','12 hodgson crescent k1g0b8 ottawa on','wireline')
r2 = r_row(28755030,524661377, None, '10-18 hurley street s4a1a1 estevan sk','wireless')
r3 = r_row(35443635,491156771,'49 wallis place dawson creek bc v1g1l1','113 cobby street v2a3w6 penticton bc','wireline')
r4 = r_row(80901366,614244346, None, '1 mccay place apt 59 mercier qc','wireless')

rogers_unit_test_data_address = spark.createDataFrame([r1,r2,r3,r4],schema=schema_rogers_address)
display(rogers_unit_test_data_address)

# COMMAND ----------

# populate shaw data
s_row =Row('s_rcis_id_cl',
            's_fa_id_cl','s_mailing_address_no_zipcode_dq_cl','s_service_address_no_zipcode_dq_cl','account_type')
s1 = s_row(98058197,'dbc40107462720','1010-25 parker smreet stouffville on l4a0s4','12 hodgson cre scent ottawa k1g0b8','wireline')
s2 = s_row(75614209,'dbc59890462929',None,'18 hurleystreet s4a1a1 estevan sk','wireless')
s3 = s_row(53560257,'dbc83800190912',None,'49 wallis place dawson creek v1g1l1 bc','wireless')
s4 = s_row(17229311,'dbc54168941500','59-1 mccay place mercier qc j6r0e3','1 mccay p lace j6r0e3 mercier qc','wireline')

shaw_unit_test_data_address = spark.createDataFrame([s1,s2,s3,s4],schema=schema_shaw_address)
display(shaw_unit_test_data_address)

# COMMAND ----------

# rename dataset
s = shaw_unit_test_data_address
r = rogers_unit_test_data_address

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Pairs for Comparison
# MAGIC Pairs for comparison will be created only when the Blocking Variable/ID matches.

# COMMAND ----------

# create all possible pairs by cross join
pairs_df = r.crossJoin(s)
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
             .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','s_account_type','r_account_type','ruleset_id','ruleset_attr','ruleset_type')

display(output)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Threshold
# MAGIC Thresholds were identified based on PEI data. We didn't do any threshold adjustment/tuning for Entire Data.

# COMMAND ----------

# filter based on threshold
output = output.filter(output.ruleset_attr >= 0.98)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Output

# COMMAND ----------

# write output
output.write.parquet("/mnt/development/Processed/InterimOutput/2022_05_04/unit_test_fuzzy_ourblocking_entire_table_address_with_threshold")
