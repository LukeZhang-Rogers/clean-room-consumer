# Databricks notebook source
# MAGIC %md ## Generating Summary Stats 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC This notebook provides the total record count (unique customer pairs) that are matched for each ruleset
# MAGIC
# MAGIC
# MAGIC ### Requirements:
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC   <li>The matched entity and ruleset table should be available at consumption layer
# MAGIC
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurations

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the Datasets

# COMMAND ----------

# MAGIC %run ../2.Common/ADLSDesignedPaths

# COMMAND ----------

matched_entity_final_df = spark.read.format("parquet").load(matched_entity_final)
ruleset_final_df = spark.read.format("parquet").load(ruleset_final).drop('RULESET_CREATED_TIMESTAMP', 'RULESET_UPDATED_TIMESTAMP', 'RULESET_NAME', 'IS_CUSTOMER_MATCH', 'IS_ADDRESS_MATCH')

# COMMAND ----------

# Adding extra columns to ruleset table
ruleset_temp_df = ruleset_final_df.withColumn('MATCHED_TIMESTAMP', current_timestamp())\
                                  .withColumn('SNAPSHOT_STAMP', lit(date))

# COMMAND ----------

# MAGIC %md
# MAGIC # Generating counts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deterministic Rules (1-15, 30-32, 35-61)

# COMMAND ----------

list_of_rules = []
#DET rules 30-32
for rule_no in range(1,16):
    total_pairs = matched_entity_final_df.filter((col('DET_MATCH_IND')=='Y') & (col('BEST_DET_MATCH_RULESET_ID')==rule_no)).count()
    list_of_rules.append((rule_no,total_pairs))

for rule_no in range(30,33):
    total_pairs = matched_entity_final_df.filter((col('DET_MATCH_IND')=='Y') & (col('BEST_DET_MATCH_RULESET_ID')==rule_no)).count()
    list_of_rules.append((rule_no,total_pairs))

for rule_no in range(35,62):
    total_pairs = matched_entity_final_df.filter((col('DET_MATCH_IND')=='Y') & (col('BEST_DET_MATCH_RULESET_ID')==rule_no)).count()
    list_of_rules.append((rule_no,total_pairs))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Fuzzy Rules (16-23, 33)

# COMMAND ----------

for rule_no in range(16,23):
    total_pairs = matched_entity_final_df.filter((col('DET_MATCH_IND')=='N') & (col('FUZZY_MATCH_IND')=='Y') & (col('BEST_FUZZY_MATCH_RULESET_ID')==rule_no)).count()
    list_of_rules.append((rule_no,total_pairs))

total_pairs = matched_entity_final_df.filter((col('DET_MATCH_IND')=='N') & (col('FUZZY_MATCH_IND')=='Y') & (col('BEST_FUZZY_MATCH_RULESET_ID')==33)).count()
list_of_rules.append((33,total_pairs))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probabilistic Rules (23-30, 34)

# COMMAND ----------

for rule_no in range(23,30):
    total_pairs = matched_entity_final_df.filter((col('DET_MATCH_IND')=='N')& (col('FUZZY_MATCH_IND')=='N') & (col('PROB_MATCH_IND')=='Y') & (col('BEST_PROB_MATCH_RULESET_ID')==rule_no)).count()
    list_of_rules.append((rule_no,total_pairs))
total_pairs = matched_entity_final_df.filter((col('DET_MATCH_IND')=='N')& (col('FUZZY_MATCH_IND')=='N') & (col('PROB_MATCH_IND')=='Y') & (col('BEST_PROB_MATCH_RULESET_ID')==34)).count()
list_of_rules.append((34,total_pairs))

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating a new dataframe from the counts

# COMMAND ----------

new_cols = ["RULESET_ID","TOTAL_COUNT"]
new_df = spark.createDataFrame(data=list_of_rules, schema = new_cols)
new_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Joining ruleset table to fetch the counts for each rule

# COMMAND ----------

df = ruleset_temp_df.join(new_df, on = 'RULESET_ID', how='left').orderBy('RULESET_ID')

# COMMAND ----------

display(df)

# COMMAND ----------

## Uncomment if want to drop the table
# %sql
# DROP DATABASE IF EXISTS SUMMARY_STATS CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC # Storing it in Database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS SUMMARY_STATS;
# MAGIC SHOW DATABASES

# COMMAND ----------

df.createOrReplaceTempView('df')

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SUMMARY_STATS;
# MAGIC SELECT CURRENT_DATABASE();

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC CREATE TABLE IF NOT EXISTS SUMMARY_STATS (
# MAGIC RULESET_ID INT,
# MAGIC RULESET_PRIORITY STRING, 
# MAGIC RULESET_TYPE STRING,
# MAGIC RULESET_DESCRIPTION STRING,
# MAGIC MATCHED_TIMESTAMP TIMESTAMP,
# MAGIC SNAPSHOT_STAMP DATE,
# MAGIC TOTAL_COUNT INT
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO SUMMARY_STATS
# MAGIC SELECT * FROM df

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from SUMMARY_STATS
