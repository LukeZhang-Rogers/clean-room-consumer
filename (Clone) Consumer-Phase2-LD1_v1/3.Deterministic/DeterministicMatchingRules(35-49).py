# Databricks notebook source
# MAGIC %md ## Executing All Deterministic Matching Rules
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC
# MAGIC This notebook is called from the DeterministicMatchingFlow notebook to execute 35-49 deterministic matching rules for the supplied foundation tables. The outputs generated in this notebook are then consumed in the DeterministicMatchingFlow notebook to build the downstream models.
# MAGIC
# MAGIC
# MAGIC ### Requirements:
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC   <li>The foundation datasets are available at: '/mnt/development/InterimOutput/'</li>
# MAGIC
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configs

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col,desc, concat, lower
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col,desc,count
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 35
# MAGIC 100% match on Customer Last Name, Email, Phone Number and Service Address

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN  :: Service Address

Rule35_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_last_name_cl","swlnc.s_dq_e_mail_cl","swlnc.s_service_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_dq_e_mail_cl","rwlnc.r_service_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("35"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 36
# MAGIC 100% match on Customer Last Name, Email, Phone Number and Mailing Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS  :: Mailing address
Rule36_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_last_name_cl" ,"rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("36"))

# COMMAND ----------

#R1 S2
# R1: Rogers C & WLS   S2: Shaw C & WLN :: MA only
Rule36_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_dq_e_mail_cl","swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_last_name_cl" ,"rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("36"))

# COMMAND ----------

#R2 S1
# R2: Rogers C & WLN   S1: Shaw C & WLS  :: MA only

Rule36_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_dq_e_mail_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("36"))

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN

Rule36_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_dq_e_mail_cl","swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_dq_e_mail_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("36"))


# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 37
# MAGIC 100% match on Customer Name, Email, Phone Number and Mailing Address/Service Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule37_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_last_name_cl","rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("37"))


# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule37_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_last_name_cl","rwlsc.r_dq_e_mail_cl"))&\
                          (((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))))\
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("37"))

# COMMAND ----------

# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule37_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_dq_e_mail_cl"))&\
                          (((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))))\
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                          (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                          (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("37"))


# COMMAND ----------

# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule37_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_dq_e_mail_cl"))&\
                          ((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                          (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                          (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("37"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 38
# MAGIC 100% match on Customer Last Name, Email and Phone Number

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule38_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_last_name_cl","rwlsc.r_dq_e_mail_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("38"))

# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule38_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_last_name_cl","rwlsc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("38"))

# COMMAND ----------


# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule38_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("38"))

# COMMAND ----------

# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule38_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("38"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 39
# MAGIC 100% match on Customer Last Name, Email and Service Address

# COMMAND ----------

# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule39_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .filter((col("s_service_address_no_zipcode_dq_cl").isin([col("r_service_address_no_zipcode_dq_cl")])))\
                          .withColumn("Ruleset_ID",lit("39"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 40
# MAGIC 100% match on Customer Last Name, Email and Mailing Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS  :: Mailing address
Rule40_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_last_name_cl" ,"rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("40"))

# COMMAND ----------

#R1 S2
# R1: Rogers C & WLS   S2: Shaw C & WLN :: MA only
Rule40_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_dq_e_mail_cl","swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_last_name_cl" ,"rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("40"))

# COMMAND ----------

#R2 S1
# R2: Rogers C & WLN   S1: Shaw C & WLS  :: MA only

Rule40_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_dq_e_mail_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("40"))

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN

Rule40_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_dq_e_mail_cl","swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_dq_e_mail_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("40"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 41
# MAGIC 100% match on Customer Last Name, Email and Mailing Address/Service Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule41_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_last_name_cl" ,"rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("41"))

# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule41_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_last_name_cl" ,"rwlsc.r_dq_e_mail_cl"))&\
                          ((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("41"))


# COMMAND ----------

# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule41_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_dq_e_mail_cl"))&\
                          (((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("41"))

# COMMAND ----------

Rule41_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_dq_e_mail_cl"))&\
                          ((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("41"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 42
# MAGIC 100% match on Customer Last Name, Phone and Service Address

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN  :: Service Address

Rule42_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_service_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_service_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("42"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 43
# MAGIC 100% match on Customer Name, Phone and Mailing Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS  :: Mailing address
Rule43_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_last_name_cl" ,"rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("43"))


# COMMAND ----------

#R1 S2
# R1: Rogers C & WLS   S2: Shaw C & WLN :: MA only
Rule43_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_last_name_cl" ,"rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("43"))


# COMMAND ----------

#R2 S1
# R2: Rogers C & WLN   S1: Shaw C & WLS  :: MA only

Rule43_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("43"))

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN

Rule43_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("43"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 44
# MAGIC 100% match on Customer Last Name, Phone and Mailing Address/Service Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule44_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_last_name_cl" ,"rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("44"))


# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule44_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(concat("swlnc.s_dq_last_name_cl" )==\
                           concat("rwlsc.r_dq_last_name_cl" ))&\
                          ((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("44"))

# COMMAND ----------


# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule44_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlsc.s_dq_last_name_cl" )==\
                           concat("rwlnc.r_dq_last_name_cl"))&\
                          (((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))))\
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("44"))


# COMMAND ----------

# R2: Rogers C & WLN  S2: Shaw C & WLN
Rule44_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlnc.s_dq_last_name_cl" )==\
                           concat("rwlnc.r_dq_last_name_cl"))&\
                          (((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))|\
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))|\
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("44"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 45
# MAGIC 100% match on Customer Last Name and Email

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule45_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_last_name_cl" ,"rwlsc.r_dq_e_mail_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("45"))

# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule45_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_last_name_cl" ,"rwlsc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .withColumn("Ruleset_ID",lit("45"))

# COMMAND ----------

# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule45_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .withColumn("Ruleset_ID",lit("45"))

# COMMAND ----------


# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule45_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_dq_e_mail_cl") \
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("45"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 46
# MAGIC 100% match on Customer Last Name and Phone

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule46_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_last_name_cl" )==\
                           concat("rwlsc.r_dq_last_name_cl" )
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("46"))


# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN


Rule46_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_last_name_cl" )==\
                           concat("rwlsc.r_dq_last_name_cl" ) \
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("46"))


# COMMAND ----------

# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule46_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_last_name_cl" )==\
                           concat("rwlnc.r_dq_last_name_cl") \
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("46"))

# COMMAND ----------


# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule46_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_last_name_cl" )==\
                           concat("rwlnc.r_dq_last_name_cl") \
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("46"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 47
# MAGIC 100% match on Customer Name and Service Address

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN  :: Service Address

Rule47_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_service_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_service_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("47"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 48
# MAGIC 100% match on Customer Name and Mailing Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS  :: Mailing address
Rule48_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_last_name_cl" ,"rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("48"))

# COMMAND ----------

#R1 S2
# R1: Rogers C & WLS   S2: Shaw C & WLN :: MA only
Rule48_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_last_name_cl" ,"rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("48"))


# COMMAND ----------

#R2 S1
# R2: Rogers C & WLN   S1: Shaw C & WLS  :: MA only

Rule48_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("48"))

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN

Rule48_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_last_name_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("48"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 49
# MAGIC 100% match on Customer Last Name and Mailing Address/Service Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule49_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_last_name_cl" ,"swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_last_name_cl" ,"rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("49"))

# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule49_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(concat("swlnc.s_dq_last_name_cl" )==\
                           concat("rwlsc.r_dq_last_name_cl" ))&\
                          ((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("49"))

# COMMAND ----------


# R2: Rogers C & WLN  S1: Shaw C & WLS


Rule49_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlsc.s_dq_last_name_cl" )==\
                           concat("rwlnc.r_dq_last_name_cl"))&\
                          ((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("49"))


# COMMAND ----------

# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule49_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlnc.s_dq_last_name_cl" )==\
                           concat("rwlnc.r_dq_last_name_cl"))&\
                          (((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("49"))
