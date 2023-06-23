# Databricks notebook source
# MAGIC %md
# MAGIC ## Probabilistic Matching Customer : Executing All Matching Rules (Unit Testing)
# MAGIC ## Description:
# MAGIC This Notebook is called to execute the Unit Testing on all customer matching rules.
# MAGIC
# MAGIC ## Requirements:
# MAGIC - Notebook "UnitTestProbabilisticCustomerUtils" is an input to this notebook. It has to be placed in the same folder as this notebook.
# MAGIC - Libraries : splink 2.1.12 and altair 4.2.0
# MAGIC - Please refer to Splink Documentation for more information around Splink and it's usage https://github.com/moj-analytical-services/splink

# COMMAND ----------

from pyspark.sql import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List
from pyspark.sql import Column

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Unit Test Utils

# COMMAND ----------

# MAGIC %run ./UnitTestProbabilisticCustomerUtils

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

# create shaw schema
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

# MAGIC %md 
# MAGIC ## Pre Processing
# MAGIC Two Main tasks :
# MAGIC 1. Selecting the coloumns/attributes from DE processed data that need to be used and needed in Output Table. 
# MAGIC 2. Renaming columns as part of Splink requirement.

# COMMAND ----------

# pre-processing for shaw
shaw_splink_ready = shaw_select_rename(shaw_unit_test_data)

# COMMAND ----------

# pre-processing for rogers
rogers_splink_ready = rogers_select_rename(rogers_unit_test_data)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rule 23
# MAGIC Name, Email, Phone Number and Mailing/Service Address

# COMMAND ----------

# apply splink for rule 23 - Name, Email, Phone Number and Mailing/Service Address
linker_rule23 = Splink(settings_rule23, [rogers_splink_ready, shaw_splink_ready], spark)
linker_rule23_df = linker_rule23.get_scored_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Processing 
# MAGIC Splink is not able to generate accurate probability scores when there is null in the attributes. Post-processing is required to filter out record pairs with null attributes to improve matching outcome

# COMMAND ----------

# filter out pairs with null attribtes for rule 23 - Name, Email, Phone Number and Mailing/Service Address
rule23_interim = linker_rule23_df.filter(linker_rule23_df.first_name_cl_l.isNotNull() & linker_rule23_df.first_name_cl_r.isNotNull()) \
                         .filter(linker_rule23_df.e_mail_cl_l.isNotNull() & linker_rule23_df.e_mail_cl_r.isNotNull()) \
                         .filter(~(linker_rule23_df.primary_phone_cl_l.isNull() & linker_rule23_df.alternate_phone_1_cl_l.isNull() & linker_rule23_df.alternate_phone_2_cl_l.isNull()))\
                         .filter(~(linker_rule23_df.primary_phone_cl_r.isNull() & linker_rule23_df.alternate_phone_1_cl_r.isNull() & linker_rule23_df.alternate_phone_2_cl_r.isNull()))\
                         .filter(~(linker_rule23_df.service_address_no_zipcode_dq_cl_l.isNull() & linker_rule23_df.mailing_address_no_zipcode_dq_cl_l.isNull()))\
                         .filter(~(linker_rule23_df.service_address_no_zipcode_dq_cl_r.isNull() & linker_rule23_df.mailing_address_no_zipcode_dq_cl_r.isNull()))

# COMMAND ----------

# renaming coloumns back to original names as raw data to create output
rule23_results = rule23_interim.withColumnRenamed('match_probability','23')\
                                 .withColumnRenamed('rcis_id_cl_l','r_rcis_id_cl')\
                                 .withColumnRenamed('rcis_id_cl_r','s_rcis_id_cl')\
                                 .withColumnRenamed('fa_id_cl_l','r_fa_id_cl')\
                                 .withColumnRenamed('fa_id_cl_r','s_fa_id_cl')\
                                 .withColumnRenamed('account_type_l','r_account_type')\
                                 .withColumnRenamed('account_type_r','s_account_type')\
                                 .select(['r_rcis_id_cl','s_rcis_id_cl','r_fa_id_cl','s_fa_id_cl','r_account_type','s_account_type','23'])

# COMMAND ----------

#create output
cols = ['23']
rule_23_longtable  = rule23_results.withColumn('array', f.explode(f.arrays_zip(f.array(*map(lambda x: f.lit(x), cols)), f.array(*cols), ))) \
  .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_account_type','s_account_type','array.*') \
  .toDF('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_account_type','s_account_type','ruleset_id','ruleset_attr')

# COMMAND ----------

# MAGIC %md
# MAGIC # Rule 24
# MAGIC Name, Email and Phone Number

# COMMAND ----------

# apply splink for rule 24 - Name, Email and Phone Number
linker_rule24 = Splink(settings_rule24, [rogers_splink_ready, shaw_splink_ready], spark)
linker_rule24_df = linker_rule24.get_scored_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Processing 
# MAGIC Splink is not able to generate accurate probability scores when there is null in the attributes. Post-processing is required to filter out record pairs with null attributes to improve matching outcome

# COMMAND ----------

# filter out pairs with null attribtes for rule 24 - Name, Email and Phone Number
rule24_interim = linker_rule24_df.filter(linker_rule24_df.first_name_cl_l.isNotNull() & linker_rule24_df.first_name_cl_r.isNotNull()) \
                         .filter(linker_rule24_df.e_mail_cl_l.isNotNull() & linker_rule24_df.e_mail_cl_r.isNotNull()) \
                         .filter(~(linker_rule24_df.primary_phone_cl_l.isNull() & linker_rule24_df.alternate_phone_1_cl_l.isNull() & linker_rule24_df.alternate_phone_2_cl_l.isNull()))\
                         .filter(~(linker_rule24_df.primary_phone_cl_r.isNull() & linker_rule24_df.alternate_phone_1_cl_r.isNull() & linker_rule24_df.alternate_phone_2_cl_r.isNull()))

# COMMAND ----------

# renaming coloumns back to original names as raw data to create output
rule24_results = rule24_interim.withColumnRenamed('match_probability','24')\
                                 .withColumnRenamed('rcis_id_cl_l','r_rcis_id_cl')\
                                 .withColumnRenamed('rcis_id_cl_r','s_rcis_id_cl')\
                                 .withColumnRenamed('fa_id_cl_l','r_fa_id_cl')\
                                 .withColumnRenamed('fa_id_cl_r','s_fa_id_cl')\
                                 .withColumnRenamed('account_type_l','r_account_type')\
                                 .withColumnRenamed('account_type_r','s_account_type')\
                               .select(['r_rcis_id_cl','s_rcis_id_cl','r_fa_id_cl','s_fa_id_cl','r_account_type','s_account_type','24'])

# COMMAND ----------

#create output
cols = ['24']
rule_24_longtable  = rule24_results.withColumn('array', f.explode(f.arrays_zip(f.array(*map(lambda x: f.lit(x), cols)), f.array(*cols), ))) \
  .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_account_type','s_account_type','array.*') \
  .toDF('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_account_type','s_account_type','ruleset_id','ruleset_attr')

# COMMAND ----------

# MAGIC %md 
# MAGIC # Rule 25
# MAGIC Name, Email and Mailing/Service Address

# COMMAND ----------

# apply splink for rule 25 - Name, Email and Mailing/Service Address
linker_rule25 = Splink(settings_rule25, [rogers_splink_ready, shaw_splink_ready], spark)
linker_rule25_df = linker_rule25.get_scored_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Processing

# COMMAND ----------

# filter out pairs with null attribtes for rule 25 - Name, Email and Mailing/Service Address
rule25_interim = linker_rule25_df.filter(linker_rule25_df.first_name_cl_l.isNotNull() & linker_rule25_df.first_name_cl_r.isNotNull()) \
                         .filter(linker_rule25_df.e_mail_cl_l.isNotNull() & linker_rule25_df.e_mail_cl_r.isNotNull()) \
                         .filter(~(linker_rule25_df.service_address_no_zipcode_dq_cl_l.isNull() & linker_rule25_df.mailing_address_no_zipcode_dq_cl_l.isNull()))\
                         .filter(~(linker_rule25_df.service_address_no_zipcode_dq_cl_r.isNull() & linker_rule25_df.mailing_address_no_zipcode_dq_cl_r.isNull()))

# COMMAND ----------

# renaming coloumns back to original names as raw data to create output
rule25_results = rule25_interim.withColumnRenamed('match_probability','25')\
                                 .withColumnRenamed('rcis_id_cl_l','r_rcis_id_cl')\
                                 .withColumnRenamed('rcis_id_cl_r','s_rcis_id_cl')\
                                 .withColumnRenamed('fa_id_cl_l','r_fa_id_cl')\
                                 .withColumnRenamed('fa_id_cl_r','s_fa_id_cl')\
                                 .withColumnRenamed('account_type_l','r_account_type')\
                                 .withColumnRenamed('account_type_r','s_account_type')\
                               .select(['r_rcis_id_cl','s_rcis_id_cl','r_fa_id_cl','s_fa_id_cl','r_account_type','s_account_type','25'])

# COMMAND ----------

#create output
cols = ['25']
rule_25_longtable  = rule25_results.withColumn('array', f.explode(f.arrays_zip(f.array(*map(lambda x: f.lit(x), cols)), f.array(*cols), ))) \
  .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_account_type','s_account_type','array.*') \
  .toDF('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_account_type','s_account_type','ruleset_id','ruleset_attr')

# COMMAND ----------

# MAGIC %md
# MAGIC # Rule 26
# MAGIC Name, Phone and Mailing/Service Address

# COMMAND ----------

# apply splink for rule 26 - Name, Phone and Mailing/Service Address
linker_rule26 = Splink(settings_rule26, [rogers_splink_ready, shaw_splink_ready], spark)
linker_rule26_df = linker_rule26.get_scored_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Processing 

# COMMAND ----------

# filter out pairs with null attribtes for rule 26 - Name, Phone and Mailing/Service Address
rule26_interim = linker_rule26_df.filter(linker_rule26_df.first_name_cl_l.isNotNull() & linker_rule26_df.first_name_cl_r.isNotNull()) \
                         .filter(~(linker_rule26_df.primary_phone_cl_l.isNull() & linker_rule26_df.alternate_phone_1_cl_l.isNull() & linker_rule26_df.alternate_phone_2_cl_l.isNull()))\
                         .filter(~(linker_rule26_df.primary_phone_cl_r.isNull() & linker_rule26_df.alternate_phone_1_cl_r.isNull() & linker_rule26_df.alternate_phone_2_cl_r.isNull()))\
                         .filter(~(linker_rule26_df.service_address_no_zipcode_dq_cl_l.isNull() & linker_rule26_df.mailing_address_no_zipcode_dq_cl_l.isNull()))\
                         .filter(~(linker_rule26_df.service_address_no_zipcode_dq_cl_r.isNull() & linker_rule26_df.mailing_address_no_zipcode_dq_cl_r.isNull()))

# COMMAND ----------

# renaming coloumns back to original names as raw data to create output
rule26_results = rule26_interim.withColumnRenamed('match_probability','26')\
                                 .withColumnRenamed('rcis_id_cl_l','r_rcis_id_cl')\
                                 .withColumnRenamed('rcis_id_cl_r','s_rcis_id_cl')\
                                 .withColumnRenamed('fa_id_cl_l','r_fa_id_cl')\
                                 .withColumnRenamed('fa_id_cl_r','s_fa_id_cl')\
                                 .withColumnRenamed('account_type_l','r_account_type')\
                                 .withColumnRenamed('account_type_r','s_account_type')\
                                 .select(['r_rcis_id_cl','s_rcis_id_cl','r_fa_id_cl','s_fa_id_cl','r_account_type','s_account_type','26'])

# COMMAND ----------

#create output
cols = ['26']
rule_26_longtable  = rule26_results.withColumn('array', f.explode(f.arrays_zip(f.array(*map(lambda x: f.lit(x), cols)), f.array(*cols), ))) \
  .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_account_type','s_account_type','array.*') \
  .toDF('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_account_type','s_account_type','ruleset_id','ruleset_attr')

# COMMAND ----------

# MAGIC %md 
# MAGIC # Rule 27 
# MAGIC Name and Email

# COMMAND ----------

# apply splink for rule 27 - Name and Email
linker_rule27 = Splink(settings_rule27, [rogers_splink_ready, shaw_splink_ready], spark)
linker_rule27_df = linker_rule27.get_scored_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Processing 

# COMMAND ----------

# filter out pairs with null attribtes for rule 27 - Name and Email
rule27_interim = linker_rule27_df.filter(linker_rule27_df.first_name_cl_l.isNotNull() & linker_rule27_df.first_name_cl_r.isNotNull()) \
                         .filter(linker_rule27_df.e_mail_cl_l.isNotNull() & linker_rule27_df.e_mail_cl_r.isNotNull())

# COMMAND ----------

# renaming coloumns back to original names as raw data to create output
rule27_results = rule27_interim.withColumnRenamed('match_probability','27')\
                                 .withColumnRenamed('rcis_id_cl_l','r_rcis_id_cl')\
                                 .withColumnRenamed('rcis_id_cl_r','s_rcis_id_cl')\
                                 .withColumnRenamed('fa_id_cl_l','r_fa_id_cl')\
                                 .withColumnRenamed('fa_id_cl_r','s_fa_id_cl')\
                                 .withColumnRenamed('account_type_l','r_account_type')\
                                 .withColumnRenamed('account_type_r','s_account_type')\
                                 .select(['r_rcis_id_cl','s_rcis_id_cl','r_fa_id_cl','s_fa_id_cl','r_account_type','s_account_type','27'])

# COMMAND ----------

#create output
cols = ['27']
rule_27_longtable  = rule27_results.withColumn('array', f.explode(f.arrays_zip(f.array(*map(lambda x: f.lit(x), cols)), f.array(*cols), ))) \
  .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_account_type','s_account_type','array.*') \
  .toDF('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_account_type','s_account_type','ruleset_id','ruleset_attr')

# COMMAND ----------

# MAGIC %md
# MAGIC # Rule 28
# MAGIC Name and Phone

# COMMAND ----------

# apply splink for rule 28 - Name and Phone
linker_rule28 = Splink(settings_rule28, [rogers_splink_ready, shaw_splink_ready], spark)
linker_rule28_df = linker_rule28.get_scored_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Processing 

# COMMAND ----------

# filter out pairs with null attribtes for rule 28 - Name and Phone
rule28_interim = linker_rule28_df.filter(linker_rule28_df.first_name_cl_l.isNotNull() & linker_rule28_df.first_name_cl_r.isNotNull()) \
                         .filter(~(linker_rule28_df.primary_phone_cl_l.isNull() & linker_rule28_df.alternate_phone_1_cl_l.isNull() & linker_rule28_df.alternate_phone_2_cl_l.isNull()))\
                         .filter(~(linker_rule28_df.primary_phone_cl_r.isNull() & linker_rule28_df.alternate_phone_1_cl_r.isNull() & linker_rule28_df.alternate_phone_2_cl_r.isNull()))

# COMMAND ----------

# renaming coloumns back to original names as raw data to create output
rule28_results = rule28_interim.withColumnRenamed('match_probability','28')\
                                 .withColumnRenamed('rcis_id_cl_l','r_rcis_id_cl')\
                                 .withColumnRenamed('rcis_id_cl_r','s_rcis_id_cl')\
                                 .withColumnRenamed('fa_id_cl_l','r_fa_id_cl')\
                                 .withColumnRenamed('fa_id_cl_r','s_fa_id_cl')\
                                 .withColumnRenamed('account_type_l','r_account_type')\
                                 .withColumnRenamed('account_type_r','s_account_type')\
                                 .select(['r_rcis_id_cl','s_rcis_id_cl','r_fa_id_cl','s_fa_id_cl','r_account_type','s_account_type','28'])

# COMMAND ----------

#create output
cols = ['28']
rule_28_longtable  = rule28_results.withColumn('array', f.explode(f.arrays_zip(f.array(*map(lambda x: f.lit(x), cols)), f.array(*cols), ))) \
  .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_account_type','s_account_type','array.*') \
  .toDF('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_account_type','s_account_type','ruleset_id','ruleset_attr') 

# COMMAND ----------

# MAGIC %md
# MAGIC # Rule 29
# MAGIC Name and Mailing/Service

# COMMAND ----------

# apply splink for rule 29 - Name and Mailing/Service
linker_rule29 = Splink(settings_rule29, [rogers_splink_ready, shaw_splink_ready], spark)
linker_rule29_df = linker_rule29.get_scored_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Processing 

# COMMAND ----------

# filter out pairs with null attribtes for rule 29 - Name and Mailing/Service
rule29_interim = linker_rule29_df.filter(linker_rule29_df.first_name_cl_l.isNotNull() & linker_rule29_df.first_name_cl_r.isNotNull()) \
                         .filter(~(linker_rule29_df.service_address_no_zipcode_dq_cl_l.isNull() & linker_rule29_df.mailing_address_no_zipcode_dq_cl_l.isNull()))\
                         .filter(~(linker_rule29_df.service_address_no_zipcode_dq_cl_r.isNull() & linker_rule29_df.mailing_address_no_zipcode_dq_cl_r.isNull()))

# COMMAND ----------

# renaming coloumns back to original names as raw data to create output
rule29_results = rule29_interim.withColumnRenamed('match_probability','29')\
                                 .withColumnRenamed('rcis_id_cl_l','r_rcis_id_cl')\
                                 .withColumnRenamed('rcis_id_cl_r','s_rcis_id_cl')\
                                 .withColumnRenamed('fa_id_cl_l','r_fa_id_cl')\
                                 .withColumnRenamed('fa_id_cl_r','s_fa_id_cl')\
                                 .withColumnRenamed('account_type_l','r_account_type')\
                                 .withColumnRenamed('account_type_r','s_account_type')\
                                 .select(['r_rcis_id_cl','s_rcis_id_cl','r_fa_id_cl','s_fa_id_cl','r_account_type','s_account_type','29'])

# COMMAND ----------

#create output
cols = ['29']
rule_29_longtable  = rule29_results.withColumn('array', f.explode(f.arrays_zip(f.array(*map(lambda x: f.lit(x), cols)), f.array(*cols), ))) \
  .select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_account_type','s_account_type','array.*') \
  .toDF('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','r_account_type','s_account_type','ruleset_id','ruleset_attr') 

# COMMAND ----------

# MAGIC %md
# MAGIC # Account Level Probablistic Output

# COMMAND ----------

#union of each rule's individual table
output = rule_23_longtable.union(rule_24_longtable).union(rule_25_longtable).union(rule_26_longtable).union(rule_27_longtable).union(rule_28_longtable).union(rule_29_longtable).withColumn('ruleset_type', lit('PROBABILISTIC'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Apply Threshold
# MAGIC Thresholds were identified based on PEI data. We didn't do any threshold adjustment/tuning for Entire Data.

# COMMAND ----------

#filter based on each rule's individual Threshold
output = output.filter(((output.ruleset_id == 23) & (output.ruleset_attr > 0.99981)) | \
                        ((output.ruleset_id == 24) & (output.ruleset_attr > 0.95)) | \
                        ((output.ruleset_id == 25) & (output.ruleset_attr > 0.95)) | \
                        ((output.ruleset_id == 26) & (output.ruleset_attr > 0.94)) | \
                        ((output.ruleset_id == 27) & (output.ruleset_attr > 0.96)) | \
                        ((output.ruleset_id == 28) & (output.ruleset_attr > 0.99)) | \
                        ((output.ruleset_id == 29) & (output.ruleset_attr > 0.99))).dropDuplicates(["s_fa_id_cl","r_fa_id_cl"]) \
.select('s_fa_id_cl','r_fa_id_cl','s_rcis_id_cl','r_rcis_id_cl','s_account_type','r_account_type','s_account_status','ruleset_id','ruleset_attr','ruleset_type')

# COMMAND ----------

# MAGIC %md
# MAGIC # Store Output

# COMMAND ----------

#write output
output.write.parquet("/mnt/development/Processed/InterimOutput/2022_05_04/probablistic_ourblocking_entire_table_customer_with_threshold")
