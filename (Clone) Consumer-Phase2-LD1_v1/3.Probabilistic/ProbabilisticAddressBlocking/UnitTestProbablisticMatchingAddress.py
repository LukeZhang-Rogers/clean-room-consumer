# Databricks notebook source
# MAGIC %md
# MAGIC ## Probabilistic Matching Address : Rule 34 - Match on Mailing/Service Address (Unit Testing)
# MAGIC ## Description:
# MAGIC This Notebook is called to execute the address matching rule 34 : Match on Mailing/Service Address.
# MAGIC
# MAGIC ## Requirements:
# MAGIC - Notebook "UnitTestProbabilisticAddressUtils" is an input to this notebook. It has to be placed in the same folder as this notebook.
# MAGIC - Libraries : splink 2.1.12 and altair 4.2.0
# MAGIC - Please refer to Splink Documentation for more information around Splink and it's usage https://github.com/moj-analytical-services/splink

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Unit Test Utils

# COMMAND ----------

# MAGIC %run ./UnitTestProbabilisticAddressUtils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Unit Test Data

# COMMAND ----------

from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List
from pyspark.sql import Column

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

# MAGIC %md 
# MAGIC ## Pre Processing
# MAGIC Two Main tasks :
# MAGIC 1. Selecting the coloumns/attributes from DE processed data that need to be used and needed in Output Table. 
# MAGIC 2. Renaming columns as part of Splink requirement.

# COMMAND ----------

# pre-processing for shaw
shaw_splink_ready = shaw_select_rename(shaw_unit_test_data_address)

# COMMAND ----------

# pre-processing for rogers
rogers_splink_ready = rogers_select_rename(rogers_unit_test_data_address)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 34
# MAGIC Mailing/Service Address

# COMMAND ----------

# apply splink for rule 34 - Mailing/Service Address
linker_rule34 = Splink(settings_rule34, [rogers_splink_ready, shaw_splink_ready], spark)
linker_rule34_df = linker_rule34.get_scored_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Processing
# MAGIC Splink is not able to generate accurate probability scores when there is null in the attributes. Post-processing is required to filter out record pairs with null attributes to improve matching outcome

# COMMAND ----------

# filter out pairs with null attribtes for rule 34 - Mailing/Service Address
rule34_interim = linker_rule34_df.filter(~(linker_rule34_df.service_address_no_zipcode_dq_cl_l.isNull() & linker_rule34_df.mailing_address_no_zipcode_dq_cl_l.isNull()))\
                         .filter(~(linker_rule34_df.service_address_no_zipcode_dq_cl_r.isNull() & linker_rule34_df.mailing_address_no_zipcode_dq_cl_r.isNull()))

# COMMAND ----------

# create output
rule34_results = rule34_interim.withColumnRenamed('match_probability','ruleset_attr') \
                                  .withColumnRenamed('rcis_id_cl_l','r_rcis_id_cl')\
                                  .withColumnRenamed('rcis_id_cl_r','s_rcis_id_cl')\
                                  .withColumnRenamed('fa_id_cl_l','r_fa_id_cl')\
                                  .withColumnRenamed('fa_id_cl_r','s_fa_id_cl')\
                                  .withColumnRenamed('account_type_l','r_account_type')\
                                  .withColumnRenamed('account_type_r','s_account_type')\
                                  .withColumn('ruleset_type', lit('PROBABILISTIC'))\
                                  .withColumn('ruleset_id', lit(34))\
                                  .select(['s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','s_account_type','r_account_type','ruleset_id','ruleset_attr','ruleset_type'])

display(rule34_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Threshold
# MAGIC Thresholds were identified based on PEI data. We didn't do any threshold adjustment/tuning for Entire Data.

# COMMAND ----------

#filter based on Threshold
output = rule34_results.filter(rule34_results.ruleset_attr >= 0.95)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Output

# COMMAND ----------

#write output
output.write.parquet("/mnt/development/Processed/InterimOutput/2022_05_04/unittest_probablistic_ourblocking_entire_table_address_with_threshold",mode='overwrite')
