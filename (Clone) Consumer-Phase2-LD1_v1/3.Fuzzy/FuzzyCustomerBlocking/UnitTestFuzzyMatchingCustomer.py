# Databricks notebook source
# MAGIC %md
# MAGIC ## Fuzzy Matching Customer : Executing All Matching Rules (Unit Testing)
# MAGIC ## Description:
# MAGIC This notebook is called  to execute Unit Testing for all customer matching rules.
# MAGIC ## Requirements:
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
# MAGIC ## Create Unit Test Data

# COMMAND ----------

# create rogers data schema
rogers_schema = StructType([StructField("r_rcis_id_cl", IntegerType(), True)\
                       ,StructField("r_fa_id_cl", StringType(), True)\
                       ,StructField("r_dq_first_name_cl", StringType(), True)\
                        ,StructField("r_dq_last_name_cl", StringType(), True)\
                       ,StructField("r_dq_e_mail_cl", StringType(), True)\
                        ,StructField("r_dq_primary_phone_cl", StringType(), True)\
                     ,StructField("r_dq_alternate_phone_1_cl", StringType(), True)\
                       ,StructField("r_dq_alternate_phone_2_cl", StringType(), True)\
                       ,StructField("r_mailing_address_no_zipcode_dq_cl", StringType(), True)\
                       ,StructField("r_service_address_no_zipcode_dq_cl", StringType(), True)\
                        ,StructField("r_account_type", StringType(), True)])

# COMMAND ----------

# populate rogers data
r_row =Row('r_rcis_id_cl',
            'r_fa_id_cl','r_dq_first_name_cl',
            'r_dq_last_name_cl','r_dq_e_mail_cl','r_dq_primary_phone_cl',
            'r_dq_alternate_phone_1_cl',
            'r_dq_alternate_phone_2_cl','r_mailing_address_no_zipcode_dq_cl','r_service_address_no_zipcode_dq_cl','r_account_type',)

r1 = r_row(87207252,221356107,'kathy', 'white', 'kathywgmailcom', 3067984651, 2048153638, None, '4 green street armidale private hospital l3m1m1 grimsby on', '4 green street armidale private hospital l3m1m1 grimsby on', 'wireline')

r2 = r_row(28755030,524661377,'kathy','weller', 'kathywgmailcom', None, None, None, '35 florence taylor street flt 61 k6t3j1 elizabethtown on', None, 'wireless')

r3 = r_row(35443635,491156771, 'ryan', 'blake', 'blakeryahoocom',5198528318, None, None, '7 faithfull circuit tryphinia view b4p1j5 wolfville ns','7 faithfull circuit tryphinia view b4p1j5 wolfville ns', 'wireline')

r4 = r_row(57980231,609625697,'katelyn','miles','katemyahoocom',5063839144,	4034651207, None,'174 traeger street cabrini medical centre k6a0v4 hawkesbury on','11 monaro crescent oaklands village k4k3l6 rockland on', 'wireline')

r5 = r_row(84706391,251979557,	'blake','beelitz','blakebyahoocom', None , None , None,	'26 ranken place goodrick park e6e3a3 millville nb', None, 'wireless')	

r6 = r_row(86952694,275383511,'charli','alderson','charlieahotmailcom',	2507742732, None , None,'266 hawkesbury crescent deergarden caravn park e6a boiestown nb',	'266 hawkesbury crescent deergarden caravn park e6a boiestown nb', 'wireline')

r7 = r_row(80901366,614244346,'emiily','roche','emilyrgmailcom',4189091947, None ,None,'59 willoughby crescent donna valley m1b8a7 scarborough on'	, '59 willoughby crescent donna valley m1b8a7 scarborough on','wireline')

r8 = r_row(97467984,564647156,'benjamin','browne','benblivecom',4506084289, None , None,'392 parsons street north star resort n5v5x0 london on', '392 parsons street north star resort n5v5x0 london on','wireline')

r9 = r_row(46024489,979315273,'matthew','goldsworthy','mattglivecom',6478022978,None ,None,'5 karuah street wallaceburg on','335 shakespeare crescent marandyvale wellington on','wireline')

r10 = r_row(82033512,618808928,'samantha','dukic','samdgmailcom',4182793342, None , None, '20 ebeling court sec 1 campobello island nb','20 ebeling court sec 1 e5e6g9 campobello island nb', 'wireline')

r11 = r_row(17913016,	604323802,	'henry'	, 'leung',	'henrylaolcom',	6044025795, None, None,	'two tree hill 40 moorhouse street k1e4t5 orleans on','2 abrahams crescent carinya village pembroke on','wireline')

r12 = r_row(81293161,489928511,'malloney',None , 'mikaylamhotmailcom',5872628137, None , None, 	'37 randwick road avalind l2a9h4 fort erie on',	'37 randwick road avalind l2a9h4 fort erie on', 'wireline')

r13 = r_row(81293161,926201492,	'mikayla','malloney','mikaylamhotmailcom',4033563850, None , None, 	'37 randwick road avalsnd l2a fort erie on'	,'37 randwick road avalsnd l2a fort erie on', 'wireline')
rogers_unit_test_data = spark.createDataFrame([r1,r2,r3,r4,r5,r6,r7,r8,r9,r10,r11,r12,r13],schema=rogers_schema)
display(rogers_unit_test_data)

# COMMAND ----------

# create shaw data schema
shaw_schema = StructType([StructField("s_rcis_id_cl", IntegerType(), True)\
                       ,StructField("s_fa_id_cl", StringType(), True)\
                       ,StructField("s_dq_first_name_cl", StringType(), True)\
                        ,StructField("s_dq_last_name_cl", StringType(), True)\
                       ,StructField("s_dq_e_mail_cl", StringType(), True)\
                        ,StructField("s_dq_primary_phone_cl", StringType(), True)\
                     ,StructField("s_dq_alternate_phone_1_cl", StringType(), True)\
                       ,StructField("s_dq_alternate_phone_2_cl", StringType(), True)\
                       ,StructField("s_mailing_address_no_zipcode_dq_cl", StringType(), True)\
                       ,StructField("s_service_address_no_zipcode_dq_cl", StringType(), True)\
                           ,StructField("s_account_type", StringType(), True)])

# COMMAND ----------

# populate shaw data
s_row =Row('s_rcis_id_cl',
            's_fa_id_cl','s_dq_first_name_cl',
            's_dq_last_name_cl','s_dq_e_mail_cl','s_dq_primary_phone_cl',
            's_dq_alternate_phone_1_cl',
            's_dq_alternate_phone_2_cl','s_mailing_address_no_zipcode_dq_cl','s_service_address_no_zipcode_dq_cl','s_account_type',)

s1 = s_row(98058197,'dbc40107462720','cathy','whuet','kathywgmailcom',2048153638, None , None,'4 greene street armidale private hospital l3m1m1 grimsby on',	'4 greene street armidale private hospital l3m1m1 grimsby on', 'wireline')

s2 = s_row(75614209,'dbc59890462929','cathy','wellerv','kathywgmailcom',4185676584, None , None,'35 florence taylor street flt 61 k6t3j1 elizabethtown on',	'35 florence taylor street flt 61 k6t3j1 elizabethtown on', 'wireline')

s3 = s_row(53560257,'dbc83800190912','blake','ryan','blakeryahoocom',5198528318, None , None,'7 tryphinia view b4p1j5 wolfville ns'	,'7 tryphinia view b4p1j5 wolfville ns', 'wireline')

s4 = s_row(56642863,'dbc12165834108','kate','milesm','katemyahoocom',4034651207,5063839144, None,'11 monaro crescent oaklands village k4k3l6 rockland on', None, 'wireline'	)

s5 = s_row(36755378,'dbc15277283794','blake',None,'blakebyahoocom',	2049465942, None , None	,'26 ranken olace goodrick park e6e3a3 millville nb','26 ranken olace goodrick park e6e3a3 millville nb', 'wireline')	

s6 = s_row(93030943,'dbc69821065090','charlie', 'alderson',	'charlieagmailcom',	2507742732	, None, None,'266 hawkesbury crescent deergarden caravn park e6a1t8 boiestown nb',	'266 hawkesbury crescent deergarden caravn park e6a1t8 boiestown nb', 'wireline')

s7 = s_row(37824484,'dbc20540507974','emily','roche','emiilyrgmailcom', None , None , None, '59 willoughby crescent donna vblaley m1b8a7 scarborough on', None	, 'wireless')

s8 = s_row(79721648,'dbc90367505930','ben',	'brone','benblivecom', None , None , None,'392 parsons street north st ar resort n5v5x0 london on', None, 'wireless')

s9 = s_row(98755369,'dbc48822568375','goldsworthy','matthew','mattewglivecom',2043883322, None , None,'5 karuah s treet n8a0l7 wallaceburg on',	'5 karuah s treet n8a0l7 wallaceburg on', 'wireline')

s10 = s_row(98755369,'dbc37216673669','matt','goldsworthy',	'mattewghotmailcom'	,6478022978, None , None,'335 shakespeare crescent marandyvale n0b0h8 wellington on','335 shakespeare crescent marandyvale n0b0h8 wellington on', 'wireline')

s11 = s_row(17229311,'dbc54168941500'	, 'sammy',	'duikc'	, 'samdhotmailcom',	2049197357, None , None	,'20 ebelin c ourt sec 1 e5e6g9 campobello island nb',	'20 ebelin c ourt sec 1 e5e6g9 campobello island nb', 'wireline')

s12 = s_row(76053204,'dbc71889869278','harry','leuncg',	'henrylaolcom',	2263175308, None , None,'2 abrahams crescent carinya vlge pembroke on',	'40 moorhouse street two tree hill k1e4t5 orleans on', 'wireline')

s13 = s_row(12985872,'dbc30683282789','mikayla','malloney',	'mikaylamhotmailcom',4033563850, None , None, '37 randwic road avalsnd l2a9h4 fort erie on',	'37 randwic road avalsnd l2a9h4 fort erie on', 'wireline')

shaw_unit_test_data = spark.createDataFrame([s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13],schema=shaw_schema)
display(shaw_unit_test_data)

# COMMAND ----------

# create full_name column
rogers_unit_test_data = rogers_unit_test_data.withColumn('r_dq_full_name_cl',concat(rogers_unit_test_data.r_dq_first_name_cl,lit(' '),rogers_unit_test_data.r_dq_last_name_cl))
shaw_unit_test_data = shaw_unit_test_data.withColumn('s_dq_full_name_cl',concat(shaw_unit_test_data.s_dq_first_name_cl,lit(' '),shaw_unit_test_data.s_dq_last_name_cl))
display(shaw_unit_test_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Pairs for Comparison

# COMMAND ----------

# create all possible pairs by cross join 
pairs_df = rogers_unit_test_data.crossJoin(shaw_unit_test_data)
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
           .withColumn('address_score',greatest('address_levenshtein_s_m_r_m','address_levenshtein_s_s_r_s','address_levenshtein_s_m_r_s','address_levenshtein_s_s_r_m','address_jaro_winkler_s_m_r_m','address_jaro_winkler_s_s_r_s', 'address_jaro_winkler_s_m_r_s', 'address_jaro_winkler_s_s_r_m', 'address_token_sort_s_m_r_m', 'address_token_sort_s_s_r_s', 'address_token_sort_s_m_r_s','address_token_sort_s_s_r_m')) \

display(sim_combined)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Similarty Score by Ruleset

# COMMAND ----------

# Replace null values with 0 
sim_final = sim_combined.fillna(value=0,subset=["name_score","email_score","phone_score","address_score"])

# COMMAND ----------

# Define attribute weights for each ruleset. The weights can be adjusted in the future
rule16_weights = {'name_score':0.4,'email_score':0.2, 'phone_score':0.2, 'address_score': 0.2}
rule17_weights = {'name_score':0.5,'email_score':0.25, 'phone_score':0.25}
rule18_weights = {'name_score':0.5,'email_score':0.2, 'address_score': 0.3}
rule19_weights = {'name_score':0.5,'phone_score':0.2, 'address_score': 0.3}
rule20_weights = {'name_score':0.7,'email_score':0.3}
rule21_weights = {'name_score':0.7,'phone_score':0.3}
rule22_weights = {'name_score':0.7,'address_score':0.3}

# COMMAND ----------

# Create final similarity score for each account pair using weighted average
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
                 .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','array.*') \
.toDF('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','ruleset_id','ruleset_attr') \
.withColumn('ruleset_type', lit('FUZZY')) \
.withColumn('ruleset_id', expr("substring(ruleset_id, 5, length(ruleset_id))"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Threshold
# MAGIC Thresholds were identified based on PEI data. We didn't do any threshold adjustment/tuning for Entire Data.

# COMMAND ----------

# filter based on Threshold
output = output.filter(((output.ruleset_id == 16) & (output.ruleset_attr > 0.95)) | \
                        ((output.ruleset_id == 17) & (output.ruleset_attr > 0.92)) | \
                        ((output.ruleset_id == 18) & (output.ruleset_attr > 0.95)) | \
                        ((output.ruleset_id == 19) & (output.ruleset_attr > 0.925)) | \
                        ((output.ruleset_id == 20) & (output.ruleset_attr > 0.98)) | \
                        ((output.ruleset_id == 21) & (output.ruleset_attr > 0.92)) | \
                        ((output.ruleset_id == 22) & (output.ruleset_attr > 0.97))).dropDuplicates(["s_fa_id_cl","r_fa_id_cl"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Output

# COMMAND ----------

# write output
output.write.parquet("/mnt/development/Processed/InterimOutput/04_28/fuzzy_restrictive_entire_table_customer")
