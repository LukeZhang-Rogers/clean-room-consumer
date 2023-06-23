# Databricks notebook source
# MAGIC %md
# MAGIC ## Fuzzy Matching Customer : Executing All Matching Rules
# MAGIC ## Description:
# MAGIC This notebook is called  to execute the all customer matching rules.
# MAGIC ## Requirements:
# MAGIC - 'ADLSDesignedPaths' notebook loads Foundational tables of Rogers and Shaw Data. Foundational tables are created by doing Data Standarization/Normlization on Raw Data. 
# MAGIC - Libraries : fuzzywuzzy 0.18.0 , ceja 0.3.0, metaphone 0.6, strsimpy 0.2.1

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
import strsimpy
import metaphone
import fuzzywuzzy
import numpy as np
from fuzzywuzzy import fuzz
from strsimpy.cosine import Cosine
from metaphone import doublemetaphone

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# MAGIC %run ../../2.Common/ADLSDesignedPaths

# COMMAND ----------

foundation_rogers_wls_c_df      =  spark.read.format("parquet").load(foundation_rogers_wls_c)
foundation_rogers_wln_c_df      =  spark.read.format("parquet").load(foundation_rogers_wln_c)
foundation_shaw_wls_c_df        =  spark.read.format("parquet").load(foundation_shaw_wls_c)
foundation_shaw_wln_c_df        =  spark.read.format("parquet").load(foundation_shaw_wln_c)

# COMMAND ----------

rogers_c_wln_wls = foundation_rogers_wls_c_df.unionByName(foundation_rogers_wln_c_df)

# COMMAND ----------

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
# MAGIC First letter First Name + Last Name + Mailing City + Mailing State

# COMMAND ----------

# add Blocking Variable/ID as a new coloumn
s = shaw_c_wln_wls.withColumn('s_blocking_id',concat(shaw_c_wln_wls.s_dq_first_name_cl.substr(1,1),lit('_'),shaw_c_wln_wls.s_dq_last_name_cl,lit('_'),shaw_c_wln_wls.s_city_cl,lit('_'),shaw_c_wln_wls.s_state_cl))
r = rogers_c_wln_wls.withColumn('r_blocking_id',concat(rogers_c_wln_wls.r_dq_first_name_cl.substr(1,1),lit('_'),rogers_c_wln_wls.r_dq_last_name_cl,lit('_'),rogers_c_wln_wls.r_city_cl,lit('_'),rogers_c_wln_wls.r_state_cl))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Pairs for Comparison
# MAGIC Pairs for comparison will be created only when the Blocking Variable/ID matches.

# COMMAND ----------

# create pairs
pairs_df = r.join(s,r.r_blocking_id == s.s_blocking_id,"cross")
# print(pairs_df.count())

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
# MAGIC
# MAGIC ### Double Metaphone 
# MAGIC Compare two strings by comparing their pronunciation. It is called "Double" because it can return both a primary and a secondary code for a string.  
# MAGIC Reference : https://en.wikipedia.org/wiki/Metaphone
# MAGIC
# MAGIC ### Cosine similarity
# MAGIC Cosine similarity is defined as the cosine of the angle between two strings, that is, the dot product of the vectors divided by the product of their lengths.  
# MAGIC Reference : https://en.wikipedia.org/wiki/Cosine_similarity

# COMMAND ----------

# UDF for doublemetaphone
@udf(returnType=IntegerType())
def udf_doublemetaphone(x,y):
     return None if (x == None or y == None or doublemetaphone(x)!= doublemetaphone(y)) else 1
    
# UDF for cosine similarity
@udf(returnType=FloatType())
def udf_cosine(x,y):
     return None if (x == None or y == None) else Cosine(1).similarity(x, y) 

# UDF for token sort ratio using fuzzywuzzy
udf_token_sort_ratio = udf(lambda x,y: fuzz.token_sort_ratio(x, y)/100, FloatType())

# COMMAND ----------

# For each account pair for comparison, calculate different similarity scores and get the max similarity score per attribute:
# - Name Attribute, calculating Levenshtein, Jaro Winkler, Double Metaphone, Cosine Similarity, and Token Sort ratio
# - Email Attribute, calculating Levenshtein and Cosine Similarity
# - Phone Attribute, calculating Levenshtein
# - Address Attribute, calculating Levenshtein, Jaro Winkler and Token Sort ratio
sim_combined = pairs_df.withColumn('name_levenshtein', 1-levenshtein(pairs_df.s_dq_full_name_cl, pairs_df.r_dq_full_name_cl) / greatest(length(pairs_df.s_dq_full_name_cl),length(pairs_df.r_dq_full_name_cl))) \
               .withColumn('name_jaro_winkler', ceja.jaro_winkler_similarity(pairs_df.s_dq_full_name_cl, pairs_df.r_dq_full_name_cl)) \
               .withColumn('name_phonetic', udf_doublemetaphone(pairs_df.s_dq_full_name_cl, pairs_df.r_dq_full_name_cl)) \
               .withColumn('name_cosine', udf_cosine(pairs_df.s_dq_full_name_cl, pairs_df.r_dq_full_name_cl)) \
               .withColumn('name_token_sort_ratio', udf_token_sort_ratio(pairs_df.s_dq_full_name_cl, pairs_df.r_dq_full_name_cl)) \
               .withColumn('email_levenshtein', 1-levenshtein(pairs_df.s_dq_e_mail_cl, pairs_df.r_dq_e_mail_cl) / greatest(length(pairs_df.s_dq_e_mail_cl),length(pairs_df.r_dq_e_mail_cl)))  \
               .withColumn('email_cosine', udf_cosine(pairs_df.s_dq_e_mail_cl, pairs_df.r_dq_e_mail_cl)) \
               .withColumn('phone_levenshtein_s_p_r_p',1-levenshtein(pairs_df.s_dq_primary_phone_cl, pairs_df.r_dq_primary_phone_cl) / greatest(length(pairs_df.s_dq_primary_phone_cl),length(pairs_df.r_dq_primary_phone_cl)))  \
               .withColumn('phone_levenshtein_s_p_r_1',1-levenshtein(pairs_df.s_dq_primary_phone_cl, pairs_df.r_dq_alternate_phone_1_cl) / greatest(length(pairs_df.s_dq_primary_phone_cl),length(pairs_df.r_dq_alternate_phone_1_cl)))  \
               .withColumn('phone_levenshtein_s_p_r_2',1-levenshtein(pairs_df.s_dq_primary_phone_cl, pairs_df.r_dq_alternate_phone_2_cl) / greatest(length(pairs_df.s_dq_primary_phone_cl),length(pairs_df.r_dq_alternate_phone_2_cl)))  \
               .withColumn('phone_levenshtein_s_1_r_1',1-levenshtein(pairs_df.s_dq_alternate_phone_1_cl, pairs_df.r_dq_alternate_phone_1_cl) / greatest(length(pairs_df.s_dq_alternate_phone_1_cl),length(pairs_df.r_dq_alternate_phone_1_cl)))  \
               .withColumn('phone_levenshtein_s_1_r_2',1-levenshtein(pairs_df.s_dq_alternate_phone_1_cl, pairs_df.r_dq_alternate_phone_2_cl) / greatest(length(pairs_df.s_dq_alternate_phone_1_cl),length(pairs_df.r_dq_alternate_phone_2_cl)))  \
               .withColumn('phone_levenshtein_s_2_r_2',1-levenshtein(pairs_df.s_dq_alternate_phone_2_cl, pairs_df.r_dq_alternate_phone_2_cl) / greatest(length(pairs_df.s_dq_alternate_phone_2_cl),length(pairs_df.r_dq_alternate_phone_2_cl)))  \
               .withColumn('address_levenshtein_s_m_r_m',1-levenshtein(pairs_df.s_mailing_address_no_zipcode_dq_cl, pairs_df.r_mailing_address_no_zipcode_dq_cl) / greatest(length(pairs_df.r_mailing_address_no_zipcode_dq_cl),length(pairs_df.r_mailing_address_no_zipcode_dq_cl))) \
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
               .withColumn('name_score', greatest('name_levenshtein','name_jaro_winkler','name_phonetic','name_cosine','name_token_sort_ratio')) \
               .withColumn('email_score', greatest('email_levenshtein','email_cosine')) \
               .withColumn('phone_score', greatest('phone_levenshtein_s_p_r_p','phone_levenshtein_s_p_r_1','phone_levenshtein_s_p_r_2','phone_levenshtein_s_1_r_1','phone_levenshtein_s_1_r_2','phone_levenshtein_s_2_r_2')) \
           .withColumn('address_score',greatest('address_levenshtein_s_m_r_m','address_levenshtein_s_s_r_s','address_levenshtein_s_m_r_s','address_levenshtein_s_s_r_m','address_jaro_winkler_s_m_r_m','address_jaro_winkler_s_s_r_s', 'address_jaro_winkler_s_m_r_s', 'address_jaro_winkler_s_s_r_m', 'address_token_sort_s_m_r_m', 'address_token_sort_s_s_r_s', 'address_token_sort_s_m_r_s','address_token_sort_s_s_r_m'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Similarty Score by Ruleset

# COMMAND ----------

# replace null values with 0 
sim_final = sim_combined.fillna(value=0,subset=["name_score","email_score","phone_score","address_score"])

# COMMAND ----------

# define attribute weights for each ruleset. The weights can be adjusted in the future
rule16_weights = {'name_score':0.4,'email_score':0.2, 'phone_score':0.2, 'address_score': 0.2}
rule17_weights = {'name_score':0.5,'email_score':0.25, 'phone_score':0.25}
rule18_weights = {'name_score':0.5,'email_score':0.2, 'address_score': 0.3}
rule19_weights = {'name_score':0.5,'phone_score':0.2, 'address_score': 0.3}
rule20_weights = {'name_score':0.7,'email_score':0.3}
rule21_weights = {'name_score':0.7,'phone_score':0.3}
rule22_weights = {'name_score':0.7,'address_score':0.3}

# COMMAND ----------

# create final similarity score for each account pair using weighted average
sim_final = sim_final.withColumn('rule16',rule16_weights['name_score']*sim_final.name_score+
                                          rule16_weights['email_score']*sim_final.email_score+
                                          rule16_weights['phone_score']*sim_final.phone_score+
                                          rule16_weights['address_score']*sim_final.address_score) \
                     .withColumn('rule17',rule17_weights['name_score']*sim_final.name_score+
                                          rule17_weights['email_score']*sim_final.email_score+
                                          rule17_weights['phone_score']*sim_final.phone_score)  \
                     .withColumn('rule18',rule18_weights['name_score']*sim_final.name_score+
                                          rule18_weights['email_score']*sim_final.email_score+
                                          rule18_weights['address_score']*sim_final.address_score)  \
                     .withColumn('rule19',rule19_weights['name_score']*sim_final.name_score+
                                          rule19_weights['phone_score']*sim_final.phone_score+
                                          rule19_weights['address_score']*sim_final.address_score) \
                     .withColumn('rule20',rule20_weights['name_score']*sim_final.name_score+
                                          rule20_weights['email_score']*sim_final.email_score) \
                     .withColumn('rule21',rule21_weights['name_score']*sim_final.name_score+
                                          rule21_weights['phone_score']*sim_final.phone_score) \
                     .withColumn('rule22',rule22_weights['name_score']*sim_final.name_score+
                                          rule22_weights['address_score']*sim_final.address_score)

# COMMAND ----------

# create output
cols = ["rule16","rule17","rule18","rule19","rule20","rule21","rule22"]

output = sim_final.withColumn('array', explode(arrays_zip(array(*map(lambda x: lit(x), cols)), array(*cols), ))) \
                 .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','array.*') \
.toDF('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_sam_key','s_sam_key','r_account_type','s_account_type','r_account_status','s_account_status','r_snapshot_stamp','s_snapshot_stamp','ruleset_id','ruleset_attr') \
.withColumn('ruleset_type', lit('FUZZY')) \
.withColumn('ruleset_id', expr("substring(ruleset_id, 5, length(ruleset_id))"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Threshold
# MAGIC Thresholds were identified based on PEI data. We didn't do any threshold adjustment/tuning for Entire Data.

# COMMAND ----------

# filter based on each rule's individual threshold
output = output.filter(((output.ruleset_id == 16) & (output.ruleset_attr > 0.95)) | \
                        ((output.ruleset_id == 17) & (output.ruleset_attr > 0.92)) | \
                        ((output.ruleset_id == 18) & (output.ruleset_attr > 0.95)) | \
                        ((output.ruleset_id == 19) & (output.ruleset_attr > 0.925)) | \
                        ((output.ruleset_id == 20) & (output.ruleset_attr > 0.98)) | \
                        ((output.ruleset_id == 21) & (output.ruleset_attr > 0.92)) | \
                        ((output.ruleset_id == 22) & (output.ruleset_attr > 0.97))).dropDuplicates(["s_fa_id_cl","r_fa_id_cl"])\
.select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','s_sam_key','r_sam_key','s_account_type','r_account_type','s_account_status','r_account_status','s_snapshot_stamp','r_snapshot_stamp','ruleset_id','ruleset_attr','ruleset_type')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Output

# COMMAND ----------

# write output
output.write.parquet(interimop_fuzzy_customer, mode='overwrite')

# COMMAND ----------


