# Databricks notebook source
# MAGIC %md ## Executing DeterministicMatchingRules(1-15,30-32)
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC
# MAGIC This notebook is called from the DeterministicMatchingRules(1-15,30-32) notebook to execute the initial set of 18 deterministic matching rules for the supplied foundation tables. The outputs generated in this notebook are then inherited in the DeterministicMatchingFlow notebook to build the downstream models.
# MAGIC
# MAGIC
# MAGIC ### Requirements:
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC   <li>This notebook will be executed from within the DeterministicMatchingFlow notebook</li>
# MAGIC
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configs

# COMMAND ----------

# Imports
import pandas as pd
from pyspark.sql.functions import col,desc, concat, lower
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col,desc,count
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 1
# MAGIC 100% match on Customer Name, Email, Phone Number and Service Address

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN  :: Service Address

Rule1_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_full_name_cl","swlnc.s_dq_e_mail_cl","swlnc.s_service_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_dq_e_mail_cl","rwlnc.r_service_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 2
# MAGIC 100% match on Customer Name, Email, Phone Number and Mailing Address

# COMMAND ----------


#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS  :: Mailing address
Rule2_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_full_name_cl" ,"rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("2"))

# COMMAND ----------

#R1 S2
# R1: Rogers C & WLS   S2: Shaw C & WLN :: MA only
Rule2_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_dq_e_mail_cl","swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_full_name_cl" ,"rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("2"))

# COMMAND ----------

#R2 S1
# R2: Rogers C & WLN   S1: Shaw C & WLS  :: MA only

Rule2_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_dq_e_mail_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("2"))

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN

Rule2_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_dq_e_mail_cl","swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_dq_e_mail_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("2"))


# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 3
# MAGIC 100% match on Customer Name, Email, Phone Number and Mailing Address/Service Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule3_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_full_name_cl","rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("3"))


# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule3_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_full_name_cl","rwlsc.r_dq_e_mail_cl"))&\
                          (((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))))\
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("3"))

# COMMAND ----------

# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule3_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_dq_e_mail_cl"))&\
                          (((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))))\
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                          (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                          (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("3"))


# COMMAND ----------

# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule3_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_dq_e_mail_cl"))&\
                          ((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                          (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                          (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("3"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 4
# MAGIC 100% match on Customer Name, Email and Phone Number

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule4_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_full_name_cl","rwlsc.r_dq_e_mail_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("4"))

# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule4_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_full_name_cl","rwlsc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("4"))

# COMMAND ----------


# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule4_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("4"))

# COMMAND ----------

# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule4_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("4"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 5
# MAGIC 100% match on Customer Name, Email and Service Address

# COMMAND ----------

# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule5_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .filter((col("s_service_address_no_zipcode_dq_cl").isin([col("r_service_address_no_zipcode_dq_cl")])))\
                          .withColumn("Ruleset_ID",lit("5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 6
# MAGIC 100% match on Customer Name, Email and Mailing Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS  :: Mailing address
Rule6_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_full_name_cl" ,"rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("6"))

# COMMAND ----------

#R1 S2
# R1: Rogers C & WLS   S2: Shaw C & WLN :: MA only
Rule6_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_dq_e_mail_cl","swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_full_name_cl" ,"rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("6"))

# COMMAND ----------

#R2 S1
# R2: Rogers C & WLN   S1: Shaw C & WLS  :: MA only

Rule6_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_dq_e_mail_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("6"))

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN

Rule6_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_dq_e_mail_cl","swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_dq_e_mail_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("6"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 7
# MAGIC 100% match on Customer Name, Email and Mailing Address/Service Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule7_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_full_name_cl" ,"rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("7"))

# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule7_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_full_name_cl" ,"rwlsc.r_dq_e_mail_cl"))&\
                          ((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("7"))


# COMMAND ----------

# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule7_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_dq_e_mail_cl"))&\
                          (((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("7"))

# COMMAND ----------

Rule7_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_dq_e_mail_cl"))&\
                          ((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("7"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 8
# MAGIC 100% match on Customer Name, Phone and Service Address

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN  :: Service Address

Rule8_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_service_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_service_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("8"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 9
# MAGIC 100% match on Customer Name, Phone and Mailing Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS  :: Mailing address
Rule9_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_full_name_cl" ,"rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("9"))


# COMMAND ----------

#R1 S2
# R1: Rogers C & WLS   S2: Shaw C & WLN :: MA only
Rule9_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_full_name_cl" ,"rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("9"))


# COMMAND ----------

#R2 S1
# R2: Rogers C & WLN   S1: Shaw C & WLS  :: MA only

Rule9_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("9"))

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN

Rule9_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("9"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 10
# MAGIC 100% match on Customer Name, Phone and Mailing Address/Service Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule10_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_full_name_cl" ,"rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("10"))


# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule10_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(concat("swlnc.s_dq_full_name_cl" )==\
                           concat("rwlsc.r_dq_full_name_cl" ))&\
                          ((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("10"))

# COMMAND ----------


# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule10_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlsc.s_dq_full_name_cl" )==\
                           concat("rwlnc.r_dq_full_name_cl"))&\
                          (((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))))\
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("10"))


# COMMAND ----------

# R2: Rogers C & WLN  S2: Shaw C & WLN
Rule10_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlnc.s_dq_full_name_cl" )==\
                           concat("rwlnc.r_dq_full_name_cl"))&\
                          (((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))|\
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))|\
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 11
# MAGIC 100% match on Customer Name and Email

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule11_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_full_name_cl" ,"rwlsc.r_dq_e_mail_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("11"))

# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule11_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_full_name_cl" ,"rwlsc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .withColumn("Ruleset_ID",lit("11"))

# COMMAND ----------

# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule11_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .withColumn("Ruleset_ID",lit("11"))

# COMMAND ----------


# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule11_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_dq_e_mail_cl") \
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("11"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 12
# MAGIC 100% match on Customer Name and Phone

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule12_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_full_name_cl" )==\
                           concat("rwlsc.r_dq_full_name_cl" )
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("12"))


# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN


Rule12_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_full_name_cl" )==\
                           concat("rwlsc.r_dq_full_name_cl" ) \
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("12"))


# COMMAND ----------

# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule12_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_full_name_cl" )==\
                           concat("rwlnc.r_dq_full_name_cl") \
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("12"))

# COMMAND ----------


# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule12_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_full_name_cl" )==\
                           concat("rwlnc.r_dq_full_name_cl") \
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("12"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 13
# MAGIC 100% match on Customer Name and Service Address

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN  :: Service Address

Rule13_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_service_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_service_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("13"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 14
# MAGIC 100% match on Customer Name and Mailing Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS  :: Mailing address
Rule14_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_full_name_cl" ,"rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("14"))

# COMMAND ----------

#R1 S2
# R1: Rogers C & WLS   S2: Shaw C & WLN :: MA only
Rule14_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_full_name_cl" ,"rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("14"))


# COMMAND ----------

#R2 S1
# R2: Rogers C & WLN   S1: Shaw C & WLS  :: MA only

Rule14_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("14"))

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN

Rule14_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_full_name_cl" ,"swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_full_name_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("14"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 15
# MAGIC 100% match on Customer Name and Mailing Address/Service Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule15_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_full_name_cl" ,"swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_full_name_cl" ,"rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("15"))

# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule15_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(concat("swlnc.s_dq_full_name_cl" )==\
                           concat("rwlsc.r_dq_full_name_cl" ))&\
                          ((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("15"))

# COMMAND ----------


# R2: Rogers C & WLN  S1: Shaw C & WLS


Rule15_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlsc.s_dq_full_name_cl" )==\
                           concat("rwlnc.r_dq_full_name_cl"))&\
                          ((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("15"))


# COMMAND ----------

# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule15_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlnc.s_dq_full_name_cl" )==\
                           concat("rwlnc.r_dq_full_name_cl"))&\
                          (((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("15"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 30
# MAGIC 100% match on Service Address

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN  :: Service Address

Rule30_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,col("swlnc.s_service_address_no_zipcode_dq_cl")==col("rwlnc.r_service_address_no_zipcode_dq_cl")\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("30"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 31
# MAGIC 100% match on Mailing Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS  :: Mailing address
Rule31_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,col("swlsc.s_mailing_address_no_zipcode_dq_cl")==col("rwlsc.r_mailing_address_no_zipcode_dq_cl")\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("31"))

# COMMAND ----------

#R1 S2
# R1: Rogers C & WLS   S2: Shaw C & WLN :: MA only
Rule31_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,col("swlnc.s_mailing_address_no_zipcode_dq_cl")==col("rwlsc.r_mailing_address_no_zipcode_dq_cl")\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("31"))

# COMMAND ----------

#R2 S1
# R2: Rogers C & WLN   S1: Shaw C & WLS  :: MA only

Rule31_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,col("swlsc.s_mailing_address_no_zipcode_dq_cl")==col("rwlnc.r_mailing_address_no_zipcode_dq_cl")\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("31"))

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN

Rule31_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,col("swlnc.s_mailing_address_no_zipcode_dq_cl")==col("rwlnc.r_mailing_address_no_zipcode_dq_cl")\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("31"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 32
# MAGIC 100% match on Mailing Address / Service Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule32_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,col("swlsc.s_mailing_address_no_zipcode_dq_cl")==col("rwlsc.r_mailing_address_no_zipcode_dq_cl")\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("32"))

# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule32_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("32"))

# COMMAND ----------

# R2: Rogers C & WLN  S1: Shaw C & WLS


Rule32_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("32"))

# COMMAND ----------

# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule32_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("32"))
