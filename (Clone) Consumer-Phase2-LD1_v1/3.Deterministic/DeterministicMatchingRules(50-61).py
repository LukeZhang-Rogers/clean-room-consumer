# Databricks notebook source
# MAGIC %md ## Executing All Deterministic Matching Rules
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC
# MAGIC This notebook is called from the DeterministicMatchingFlow notebook to execute the 50-61 deterministic matching rules for the supplied foundation tables. The outputs generated in this notebook are then consumed in the DeterministicMatchingFlow notebook to build the downstream models.
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
# MAGIC ##Rule 50
# MAGIC 100% match on Email, Phone Number and Service Address

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN  :: Service Address

Rule50_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_e_mail_cl","swlnc.s_service_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_e_mail_cl","rwlnc.r_service_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("50"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 51
# MAGIC 100% match on Email, Phone Number and Mailing Address

# COMMAND ----------


#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS  :: Mailing address
Rule51_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("51"))

# COMMAND ----------

#R1 S2
# R1: Rogers C & WLS   S2: Shaw C & WLN :: MA only
Rule51_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_e_mail_cl","swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("51"))

# COMMAND ----------

#R2 S1
# R2: Rogers C & WLN   S1: Shaw C & WLS  :: MA only

Rule51_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_e_mail_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("51"))

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN

Rule51_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_e_mail_cl","swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_e_mail_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("51"))


# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 52
# MAGIC 100% match on Email, Phone Number and Mailing Address/Service Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule52_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("52"))


# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule52_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(concat("swlnc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_e_mail_cl"))&\
                          (((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))))\
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("52"))

# COMMAND ----------

# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule52_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlsc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_e_mail_cl"))&\
                          (((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))))\
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                          (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                          (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("52"))


# COMMAND ----------

# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule52_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlnc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_e_mail_cl"))&\
                          ((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                          (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                          (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("52"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 53
# MAGIC 100% match on Email and Phone Number

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule53_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_e_mail_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("53"))

# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule53_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("53"))

# COMMAND ----------


# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule53_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("53"))

# COMMAND ----------

# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule53_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("53"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 54
# MAGIC 100% match on Email and Service Address

# COMMAND ----------

# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule54_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .filter((col("s_service_address_no_zipcode_dq_cl").isin([col("r_service_address_no_zipcode_dq_cl")])))\
                          .withColumn("Ruleset_ID",lit("54"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 55
# MAGIC 100% match on Customer Last Name, Email and Mailing Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS  :: Mailing address
Rule55_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("55"))

# COMMAND ----------

#R1 S2
# R1: Rogers C & WLS   S2: Shaw C & WLN :: MA only
Rule55_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_last_name_cl" ,"swlnc.s_dq_e_mail_cl","swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_last_name_cl" ,"rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("55"))

# COMMAND ----------

#R2 S1
# R2: Rogers C & WLN   S1: Shaw C & WLS  :: MA only

Rule55_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_e_mail_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("55"))

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN

Rule55_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_e_mail_cl","swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_dq_e_mail_cl","rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("55"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 56
# MAGIC 100% match on Email and Mailing Address/Service Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule56_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_e_mail_cl","swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_dq_e_mail_cl","rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("56"))

# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule56_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(concat("swlnc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_e_mail_cl"))&\
                          ((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("56"))


# COMMAND ----------

# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule56_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlsc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_e_mail_cl"))&\
                          (((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("56"))

# COMMAND ----------

Rule56_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(concat("swlnc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_e_mail_cl"))&\
                          ((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("56"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 57
# MAGIC 100% match on Phone and Service Address

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN  :: Service Address

Rule57_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_service_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_service_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("57"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 58
# MAGIC 100% match on Phone and Mailing Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS  :: Mailing address
Rule58_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("58"))


# COMMAND ----------

#R1 S2
# R1: Rogers C & WLS   S2: Shaw C & WLN :: MA only
Rule58_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("58"))


# COMMAND ----------

#R2 S1
# R2: Rogers C & WLN   S1: Shaw C & WLS  :: MA only

Rule58_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("58"))

# COMMAND ----------

#R2 S2
# R2: Rogers C & WLN   S2: Shaw C & WLN

Rule58_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlnc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("58"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 59
# MAGIC 100% match on Phone and Mailing Address/Service Address

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule59_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_mailing_address_no_zipcode_dq_cl")==\
                           concat("rwlsc.r_mailing_address_no_zipcode_dq_cl")
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("59"))


# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule59_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("59"))

# COMMAND ----------


# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule59_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))))\
                          ,how='inner') \
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))| \
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("59"))


# COMMAND ----------

# R2: Rogers C & WLN  S2: Shaw C & WLN
Rule59_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(((col("s_mailing_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_mailing_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_service_address_no_zipcode_dq_cl")))|\
                          (col("s_service_address_no_zipcode_dq_cl")==(col("r_mailing_address_no_zipcode_dq_cl"))))\
                          ,how='inner')\
                          .filter((col("s_dq_primary_phone_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))|\
                           (col("s_dq_alternate_phone_1_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")]))|\
                           (col("s_dq_alternate_phone_2_cl").isin([col("r_dq_primary_phone_cl"),col("r_dq_alternate_phone_1_cl"),col("r_dq_alternate_phone_2_cl")])))\
                          .withColumn("Ruleset_ID",lit("59"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 60
# MAGIC 100% match on Email

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

Rule60_R1S1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlsc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_e_mail_cl")
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("60"))

# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

Rule60_R1S2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,concat("swlnc.s_dq_e_mail_cl")==\
                           concat("rwlsc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .withColumn("Ruleset_ID",lit("60"))

# COMMAND ----------

# R2: Rogers C & WLN  S1: Shaw C & WLS

Rule60_R2S1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlsc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_e_mail_cl") \
                          ,how='inner') \
                          .withColumn("Ruleset_ID",lit("60"))

# COMMAND ----------


# R2: Rogers C & WLN  S2: Shaw C & WLN

Rule60_R2S2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,concat("swlnc.s_dq_e_mail_cl")==\
                           concat("rwlnc.r_dq_e_mail_cl") \
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("60"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rule 61
# MAGIC 100% match on Phone

# COMMAND ----------

#R1 S1
# R1: Rogers C & WLS   S1: Shaw C & WLS

#Rule61 R1S1

Rule61_R1S1_1 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_primary_phone_cl")==col("r_dq_primary_phone_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R1S1_2 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_primary_phone_cl")==col("r_dq_alternate_phone_1_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R1S1_3 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_primary_phone_cl")==col("r_dq_alternate_phone_2_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R1S1_4 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_alternate_phone_1_cl")==col("r_dq_primary_phone_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R1S1_5 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_alternate_phone_1_cl")==col("r_dq_alternate_phone_1_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R1S1_6 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_alternate_phone_1_cl")==col("r_dq_alternate_phone_2_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R1S1_7 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_alternate_phone_2_cl")==col("r_dq_primary_phone_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R1S1_8 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_alternate_phone_2_cl")==col("r_dq_alternate_phone_1_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R1S1_9 = s_wls_c_joined.alias('swlsc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_alternate_phone_2_cl")==col("r_dq_alternate_phone_2_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))


Rule61_R1S1 = Rule61_R1S1_1.unionAll(Rule61_R1S1_2).unionAll(Rule61_R1S1_3).unionAll(Rule61_R1S1_4).unionAll(Rule61_R1S1_5).unionAll(Rule61_R1S1_6)\
                        .unionAll(Rule61_R1S1_7).unionAll(Rule61_R1S1_8).unionAll(Rule61_R1S1_9).distinct()

# COMMAND ----------

# R1: Rogers C & WLS  S2: Shaw C & WLN

#Rule61 R1S2

Rule61_R1S2_1 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_primary_phone_cl")==col("r_dq_primary_phone_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R1S2_2 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_primary_phone_cl")==col("r_dq_alternate_phone_1_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R1S2_3 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_primary_phone_cl")==col("r_dq_alternate_phone_2_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R1S2_4 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_alternate_phone_1_cl")==col("r_dq_primary_phone_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R1S2_5 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_alternate_phone_1_cl")==col("r_dq_alternate_phone_1_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R1S2_6 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_alternate_phone_1_cl")==col("r_dq_alternate_phone_2_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R1S2_7 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_alternate_phone_2_cl")==col("r_dq_primary_phone_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R1S2_8 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_alternate_phone_2_cl")==col("r_dq_alternate_phone_1_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R1S2_9 = s_wln_c_joined.alias('swlnc').join(r_wls_c_joined.alias('rwlsc')\
                          ,(col("s_dq_alternate_phone_2_cl")==col("r_dq_alternate_phone_2_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))


Rule61_R1S2 = Rule61_R1S2_1.unionAll(Rule61_R1S2_2).unionAll(Rule61_R1S2_3).unionAll(Rule61_R1S2_4).unionAll(Rule61_R1S2_5).unionAll(Rule61_R1S2_6)\
                        .unionAll(Rule61_R1S2_7).unionAll(Rule61_R1S2_8).unionAll(Rule61_R1S2_9).distinct()

# COMMAND ----------

# R2: Rogers C & WLN  S1: Shaw C & WLS

#Rule 61: R2S1
Rule61_R2S1_1 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_primary_phone_cl")==col("r_dq_primary_phone_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R2S1_2 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_primary_phone_cl")==col("r_dq_alternate_phone_1_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R2S1_3 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_primary_phone_cl")==col("r_dq_alternate_phone_2_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R2S1_4 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_alternate_phone_1_cl")==col("r_dq_primary_phone_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R2S1_5 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_alternate_phone_1_cl")==col("r_dq_alternate_phone_1_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R2S1_6 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_alternate_phone_1_cl")==col("r_dq_alternate_phone_2_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R2S1_7 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_alternate_phone_2_cl")==col("r_dq_primary_phone_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R2S1_8 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_alternate_phone_2_cl")==col("r_dq_alternate_phone_1_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R2S1_9 = s_wls_c_joined.alias('swlsc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_alternate_phone_2_cl")==col("r_dq_alternate_phone_2_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))


Rule61_R2S1 = Rule61_R2S1_1.unionAll(Rule61_R2S1_2).unionAll(Rule61_R2S1_3).unionAll(Rule61_R2S1_4).unionAll(Rule61_R2S1_5).unionAll(Rule61_R2S1_6)\
                        .unionAll(Rule61_R2S1_7).unionAll(Rule61_R2S1_8).unionAll(Rule61_R2S1_9).distinct()

# COMMAND ----------


# R2: Rogers C & WLN  S2: Shaw C & WLN

#Rule 61: R2S2
Rule61_R2S2_1 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_primary_phone_cl")==col("r_dq_primary_phone_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R2S2_2 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_primary_phone_cl")==col("r_dq_alternate_phone_1_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R2S2_3 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_primary_phone_cl")==col("r_dq_alternate_phone_2_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R2S2_4 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_alternate_phone_1_cl")==col("r_dq_primary_phone_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R2S2_5 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_alternate_phone_1_cl")==col("r_dq_alternate_phone_1_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R2S2_6 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_alternate_phone_1_cl")==col("r_dq_alternate_phone_2_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R2S2_7 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_alternate_phone_2_cl")==col("r_dq_primary_phone_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R2S2_8 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_alternate_phone_2_cl")==col("r_dq_alternate_phone_1_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))

Rule61_R2S2_9 = s_wln_c_joined.alias('swlnc').join(r_wln_c_joined.alias('rwlnc')\
                          ,(col("s_dq_alternate_phone_2_cl")==col("r_dq_alternate_phone_2_cl"))\
                          ,how='inner')\
                          .withColumn("Ruleset_ID",lit("61"))


Rule61_R2S2 = Rule61_R2S2_1.unionAll(Rule61_R2S2_2).unionAll(Rule61_R2S2_3).unionAll(Rule61_R2S2_4).unionAll(Rule61_R2S2_5).unionAll(Rule61_R2S2_6)\
                        .unionAll(Rule61_R2S2_7).unionAll(Rule61_R2S2_8).unionAll(Rule61_R2S2_9).distinct()
