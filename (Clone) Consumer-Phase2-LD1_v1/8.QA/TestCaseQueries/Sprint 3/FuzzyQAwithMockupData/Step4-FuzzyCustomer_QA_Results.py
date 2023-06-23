# Databricks notebook source
# MAGIC %md
# MAGIC ### Configure paths

# COMMAND ----------

fuzzy_output_result_path = "/mnt/development/InterimOutput/Fuzzy/QAMockData/Sprint3/Output"

# COMMAND ----------

rogers_interim_path =  "/mnt/development/InterimOutput/Fuzzy/QAMockData/Sprint3/Interim/Rogers"
foundation_rogers_wln_c  = rogers_interim_path + '/WirelineAndContact'
shaw_interim_path =  "/mnt/development/InterimOutput/Fuzzy/QAMockData/Sprint3/Interim/Shaw"
foundation_shaw_wln_c  = shaw_interim_path + '/WirelineAndContact'

# COMMAND ----------

from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List
from pyspark.sql import Column

import recordlinkage
from recordlinkage.index import Block, SortedNeighbourhood
import fuzzywuzzy
from fuzzywuzzy import fuzz
import metaphone
from metaphone import doublemetaphone
import strsimpy
from strsimpy.jaro_winkler import JaroWinkler
from strsimpy.normalized_levenshtein import NormalizedLevenshtein
from strsimpy.cosine import Cosine
import numpy as np

# COMMAND ----------

rogers_mockup_test_data = spark.read.format("parquet").load(foundation_rogers_wln_c)
shaw_mockup_test_data = spark.read.format("parquet").load(foundation_shaw_wln_c)

# COMMAND ----------

r = rogers_mockup_test_data
s = shaw_mockup_test_data

# COMMAND ----------

# cast type and rename
r = r.coalesce(1).withColumn('r_primary_phone_cl',r.r_dq_primary_phone_cl.cast(StringType())) \
                 .withColumn('r_alternate_phone_1_cl',r.r_dq_alternate_phone_1_cl.cast(StringType())) \
                 .withColumn('r_alternate_phone_2_cl',r.r_dq_alternate_phone_2_cl.cast(StringType())) \
                 .withColumnRenamed('r_dq_e_mail_cl','r_e_mail_cl') \
                 .withColumnRenamed('r_dq_full_name_cl','r_full_name_cl') \
                 .withColumnRenamed('r_dq_mailing_address_full_cl','r_mailing_address_full_cl') \
                 .withColumnRenamed('r_dq_service_address_rull_cl','r_service_address_full_cl')
s = s.coalesce(1).withColumn('s_primary_phone_cl',s.s_dq_primary_phone_cl.cast(StringType())) \
                 .withColumn('s_alternate_phone_1_cl',s.s_dq_alternate_phone_1_cl.cast(StringType())) \
                 .withColumn('s_alternate_phone_2_cl',s.s_dq_alternate_phone_2_cl.cast(StringType())) \
                 .withColumnRenamed('s_dq_full_name_cl','s_full_name_cl') \
                 .withColumnRenamed('s_dq_e_mail_cl','s_e_mail_cl') \
                 .withColumnRenamed('s_dq_mailing_address_full_cl','s_mailing_address_full_cl') \
                 .withColumnRenamed('s_dq_service_address_rull_cl','s_service_address_full_cl')# display(sample_shaw_contact_df)

# COMMAND ----------

# create a vector for phone
s_columns = [col('s_primary_phone_cl'), col('s_alternate_phone_1_cl'), col('s_alternate_phone_2_cl')]
r_columns = [col('r_primary_phone_cl'), col('r_alternate_phone_1_cl'), col('r_alternate_phone_2_cl')]
s = s.withColumn('temp', array(s_columns))\
     .withColumn('s_phone_vector', expr("FILTER(temp, x -> x is not null)")) \
     .drop('temp')
r = r.withColumn('temp', array(r_columns)) \
     .withColumn('r_phone_vector', expr("FILTER(temp, x -> x is not null)")) \
     .drop('temp')

# create a vector for address
s_columns = [col('s_service_address_full_cl'), col('s_mailing_address_full_cl')]
r_columns = [col('r_service_address_full_cl'), col('r_mailing_address_full_cl')]
s = s.withColumn('temp', array(s_columns))\
     .withColumn('s_address_vector', expr("FILTER(temp, x -> x is not null)")) \
     .drop('temp')
r = r.withColumn('temp', array(r_columns)) \
     .withColumn('r_address_vector', expr("FILTER(temp, x -> x is not null)")) \
     .drop('temp')

# COMMAND ----------

# create all pairs
indexer = recordlinkage.Index().full()
all_pairs = indexer.index(s.toPandas().set_index("s_fa_id_cl"),r.toPandas().set_index("r_fa_id_cl"))

# join with attributes
pairs_df = spark.createDataFrame(all_pairs.to_frame(name=["s_fa_id_cl","r_fa_id_cl"]))
pairs_df = pairs_df.join(r,["r_fa_id_cl"],"left")
pairs_df = pairs_df.join(s,["s_fa_id_cl"],"left")
pairs_df = pairs_df.select("s_fa_id_cl","r_fa_id_cl","s_rcis_id_cl","r_rcis_id_cl","r_full_name_cl","r_phone_vector","r_e_mail_cl","r_address_vector","s_full_name_cl","s_phone_vector","s_e_mail_cl","s_address_vector")

display(pairs_df)

# COMMAND ----------

# levenshtein   
# udf_levenshtein = udf(lambda x,y: NormalizedLevenshtein().similarity(x, y))
@udf(returnType=FloatType())
def udf_levenshtein(x,y):
     return None if (x == None or y == None) else NormalizedLevenshtein().similarity(x, y)
    
# jaro winkler
# udf_jaro_winkler = udf(lambda x,y: JaroWinkler().similarity(x, y)) 
@udf(returnType=FloatType())
def udf_jaro_winkler(x,y):
     return None if (x == None or y == None) else JaroWinkler().similarity(x, y)
    
# phonetic
@udf(returnType=IntegerType())
def udf_doublemetaphone(x,y):
     return None if (x == None or y == None or doublemetaphone(x)!= doublemetaphone(y)) else 1
    
# cosine
# udf_cosine = udf(lambda x,y: Cosine(1).similarity(x, y)) 
@udf(returnType=FloatType())
def udf_cosine(x,y):
     return None if (x == None or y == None) else Cosine(1).similarity(x, y) 

# token set ratio
udf_token_set_ratio = udf(lambda x,y: fuzz.token_set_ratio(x, y)/100, FloatType()) 

# multiple levenshtein
def multiple_levenshtein(x,y):
    scores = []
    for i in x:
        for j in y:
            if x is not None and j is not None:
                sim = NormalizedLevenshtein().similarity(i,j)
                scores.append(sim)
    return scores
udf_multiple_levenshtein = udf(multiple_levenshtein, ArrayType(FloatType()))

# multiple jaro winkler
def multiple_jaro_winkler(x,y):
    scores = []
    for i in x:
        for j in y:
            if x is not None and j is not None:
                sim = JaroWinkler().similarity(x, y)
                scores.append(sim)
    return scores
udf_multiple_jaro_winkler = udf(multiple_jaro_winkler, ArrayType(FloatType()))

# COMMAND ----------

sim_scores = pairs_df.withColumn('name_levenshtein', udf_levenshtein(pairs_df.s_full_name_cl, pairs_df.r_full_name_cl)) \
                        .withColumn('name_jaro_winkler', udf_jaro_winkler(pairs_df.s_full_name_cl, pairs_df.r_full_name_cl)) \
                        .withColumn('name_phonetic', udf_doublemetaphone(pairs_df.s_full_name_cl, pairs_df.r_full_name_cl)) \
                        .withColumn('name_cosine', udf_cosine(pairs_df.s_full_name_cl, pairs_df.r_full_name_cl)) \
                        .withColumn('name_token_set_ratio', udf_token_set_ratio(pairs_df.s_full_name_cl, pairs_df.r_full_name_cl)) \
                        .withColumn('email_levenshtein', udf_levenshtein(pairs_df.s_e_mail_cl, pairs_df.r_e_mail_cl)) \
                        .withColumn('email_cosine', udf_cosine(pairs_df.s_e_mail_cl, pairs_df.r_e_mail_cl)) \
                        .withColumn('phone_multiple_levenshtein',udf_multiple_levenshtein(pairs_df.s_phone_vector,pairs_df.r_phone_vector)) \
                        .withColumn('address_multiple_levenshtein',udf_multiple_levenshtein(pairs_df.s_address_vector,pairs_df.r_address_vector)) \
                        .withColumn('address_multiple_jaro_winkler',udf_multiple_jaro_winkler(pairs_df.s_address_vector,pairs_df.r_address_vector)) \
                        .withColumn('address_multiple', concat(col("address_multiple_levenshtein"),col("address_multiple_jaro_winkler")))

# COMMAND ----------

sim_scores = sim_scores.select("s_fa_id_cl","r_fa_id_cl","s_rcis_id_cl","r_rcis_id_cl","name_levenshtein","name_jaro_winkler","name_phonetic","name_cosine","name_token_set_ratio","email_levenshtein","email_cosine","phone_multiple_levenshtein","address_multiple_levenshtein","address_multiple_jaro_winkler","address_multiple")

# COMMAND ----------

# 0 or null ?
@udf(returnType=FloatType())
def udf_max_in_vector(x):
     return None if (x == []) else float(np.max(np.abs(x)))

# COMMAND ----------

sim_combined = sim_scores.withColumn('name_score',greatest("name_levenshtein","name_jaro_winkler","name_phonetic","name_cosine","name_token_set_ratio")) \
                            .withColumn('email_score', greatest("email_levenshtein","email_cosine")) \
                            .withColumn('phone_score',udf_max_in_vector(sim_scores.phone_multiple_levenshtein)) \
                            .withColumn('address_score',udf_max_in_vector(sim_scores.address_multiple)) 

# COMMAND ----------

rule3_weights = {'name_score':0.3,'email_score':0.2, 'phone_score':0.2, 'address_score': 0.3}
rule5_weights = {'name_score':0.5,'email_score':0.25, 'phone_score':0.25}
rule7_weights = {'name_score':0.4,'email_score':0.2, 'address_score': 0.4}
rule10_weights = {'name_score':0.4,'phone_score':0.2, 'address_score': 0.4}
rule11_weights = {'name_score':0.7,'email_score':0.3}
rule12_weights = {'name_score':0.7,'phone_score':0.3}
rule15_weights = {'name_score':0.7,'address_score': 0.3}

# COMMAND ----------

wf = "1 / ("
val = ""
for col in rule3_weights:
    wf += f"if({col} is null,0 ,{rule3_weights[col]}) + "
    val += f"if( {col} is null, 0, {col} * {rule3_weights[col]} * rule3_weight_factor) + "
wf += "0 )"
val += "0"

sim_final = sim_combined.withColumn("rule3_weight_factor", F.expr(wf)) \
                        .withColumn("rule3", F.expr(val))

wf = "1 / ("
val = ""
for col in rule5_weights:
    wf += f"if({col} is null,0 ,{rule5_weights[col]}) + "
    val += f"if( {col} is null, 0, {col} * {rule5_weights[col]} *  rule5_weight_factor) + "
wf += "0 )"
val += "0"

sim_final = sim_final.withColumn("rule5_weight_factor", F.expr(wf)) \
                     .withColumn("rule5", F.expr(val))

wf = "1 / ("
val = ""
for col in rule7_weights:
    wf += f"if({col} is null,0 ,{rule7_weights[col]}) + "
    val += f"if( {col} is null, 0, {col} * {rule7_weights[col]} *  rule7_weight_factor) + "
wf += "0 )"
val += "0"

sim_final = sim_final.withColumn("rule7_weight_factor", F.expr(wf)) \
                     .withColumn("rule7", F.expr(val))

wf = "1 / ("
val = ""
for col in rule10_weights:
    wf += f"if({col} is null,0 ,{rule10_weights[col]}) + "
    val += f"if( {col} is null, 0, {col} * {rule10_weights[col]} *  rule10_weight_factor) + "
wf += "0 )"
val += "0"

sim_final = sim_final.withColumn("rule10_weight_factor", F.expr(wf)) \
                     .withColumn("rule10", F.expr(val))

wf = "1 / ("
val = ""
for col in rule11_weights:
    wf += f"if({col} is null,0 ,{rule11_weights[col]}) + "
    val += f"if( {col} is null, 0, {col} * {rule11_weights[col]} *  rule11_weight_factor) + "
wf += "0 )"
val += "0"

sim_final = sim_final.withColumn("rule11_weight_factor", F.expr(wf)) \
                     .withColumn("rule11", F.expr(val))

wf = "1 / ("
val = ""
for col in rule12_weights:
    wf += f"if({col} is null,0 ,{rule12_weights[col]}) + "
    val += f"if( {col} is null, 0, {col} * {rule12_weights[col]} *  rule12_weight_factor) + "
wf += "0 )"
val += "0"

sim_final = sim_final.withColumn("rule12_weight_factor", F.expr(wf)) \
                     .withColumn("rule12", F.expr(val))

wf = "1 / ("
val = ""
for col in rule15_weights:
    wf += f"if({col} is null,0 ,{rule15_weights[col]}) + "
    val += f"if( {col} is null, 0, {col} * {rule15_weights[col]} *  rule15_weight_factor) + "
wf += "0 )"
val += "0"

sim_final = sim_final.withColumn("rule15_weight_factor", F.expr(wf)) \
                     .withColumn("rule15", F.expr(val))

sim_final = sim_final.select("s_fa_id_cl","r_fa_id_cl","s_rcis_id_cl","r_rcis_id_cl","rule3","rule5","rule7","rule10","rule11","rule12","rule15")

# COMMAND ----------

display(sim_final)

# COMMAND ----------

sim_final.write.parquet(fuzzy_output_result_path+"/DetailedFuzzy", mode='overwrite')

# COMMAND ----------

def find_max_rule_per_row(cols: List[str]) -> Column:
    max_val = F.greatest(*[F.col(c) for c in cols])
    max_col_expr = F
    for c in cols:
        max_col_expr = max_col_expr.when(F.col(c) == max_val, c)
    return max_col_expr

df = sim_final.withColumn("fuzzy_score", greatest("rule3","rule5","rule7","rule10","rule11","rule12","rule15")) \
.withColumn("fuzzy_ruleset_id", find_max_rule_per_row(["rule3","rule5","rule7","rule10","rule11","rule12","rule15"])) \
.select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','fuzzy_ruleset_id','fuzzy_score') \
.withColumn('fuzzy_ruleset_id', expr("substring(fuzzy_ruleset_id, 5, length(fuzzy_ruleset_id))"))

display(df)

# COMMAND ----------

df.write.parquet(fuzzy_output_result_path+"/AggregatendFuzzy", mode='overwrite')
