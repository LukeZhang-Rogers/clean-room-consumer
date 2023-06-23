# Databricks notebook source
# MAGIC %md
# MAGIC ## Probabilistic Matching Customer : Utility Functions
# MAGIC ## Description:
# MAGIC This notebook is called from the ProbabilisticMatchingCustomer notebook to execute customer matching rules.
# MAGIC ## Requirements:
# MAGIC - Libraries : splink 2.1.12 and altair 4.2.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import altair as alt
from splink import Splink
from operator import itemgetter
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from splink.profile import column_value_frequencies_chart
from splink.intuition import bayes_factor_intuition_chart
from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

# MAGIC %md
# MAGIC # Utility Functions

# COMMAND ----------

# This Function as a part of Splink's requirement:
# Adds two additional columns 
# 1. unique_id adds an Integer Unique ID 
# 2. source_dataset adds a string suggesting dataset source e.g rogers and shaw here. 

# Note : These coloumns do not affect learning or model parameters , it is just a way of Splink to get two data.
def splink_ready_data(input_df,source_dataset_name):
    splink_ready_df = input_df.withColumn("unique_id",monotonically_increasing_id())\
    .withColumn("source_dataset",lit(source_dataset_name))
    return splink_ready_df

# COMMAND ----------

# This Function adds a coloumn of our specified Blocking variable/ID. 
# Note : Blocking variable/ID will be used later inside Splink to generate pair only when Blocking variable/ID matches.
# Note : Here for Address Matching our Blocking Variable/ID : First letter of First Name + Last Name + Mailing Address City + Mailing Address State 
def shaw_add_blocking_id(s_df_without_blocking):
    s_blocking_scheme = concat(s_df_without_blocking.s_dq_first_name_cl.substr(1,1),lit('_'),s_df_without_blocking.s_dq_last_name_cl,lit('_'),s_df_without_blocking.s_city_cl,lit('_'),s_df_without_blocking.s_state_cl)
    s_df_with_blocking  = s_df_without_blocking.withColumn("blocking_id", s_blocking_scheme)
    return s_df_with_blocking

# COMMAND ----------

# This Function as a part of Splink's requirement rename each coloumn/attribute of interest to make it similar in both data source.
s_coloumns_of_interest = ['s_rcis_id_cl','s_fa_id_cl','s_dq_first_name_cl','s_dq_last_name_cl','s_dq_e_mail_cl','s_dq_primary_phone_cl','s_dq_alternate_phone_1_cl','s_dq_alternate_phone_2_cl','s_mailing_address_no_zipcode_dq_cl','s_service_address_no_zipcode_dq_cl','s_sam_key','s_account_type','s_account_status','s_snapshot_stamp','unique_id','source_dataset','blocking_id']

def shaw_select_rename(shaw_df):
    s_temp_splink_ready = splink_ready_data(input_df=shaw_df,source_dataset_name="shaw")
    s_temp_splink_ready = shaw_add_blocking_id(s_df_without_blocking=s_temp_splink_ready)
    shaw_splink_ready = s_temp_splink_ready.select(s_coloumns_of_interest)\
    .withColumnRenamed('s_rcis_id_cl','rcis_id_cl')\
    .withColumnRenamed('s_fa_id_cl','fa_id_cl')\
    .withColumnRenamed('s_dq_first_name_cl','first_name_cl')\
    .withColumnRenamed('s_dq_last_name_cl','last_name_cl')\
    .withColumnRenamed('s_dq_e_mail_cl','e_mail_cl')\
    .withColumnRenamed('s_dq_primary_phone_cl','primary_phone_cl')\
    .withColumnRenamed('s_dq_alternate_phone_1_cl','alternate_phone_1_cl')\
    .withColumnRenamed('s_dq_alternate_phone_2_cl','alternate_phone_2_cl')\
    .withColumnRenamed('s_mailing_address_no_zipcode_dq_cl','mailing_address_no_zipcode_dq_cl')\
    .withColumnRenamed('s_service_address_no_zipcode_dq_cl','service_address_no_zipcode_dq_cl')\
    .withColumnRenamed('s_sam_key','sam_key')\
    .withColumnRenamed('s_account_type','account_type')\
    .withColumnRenamed('s_account_status','account_status')\
    .withColumnRenamed('s_snapshot_stamp','snapshot_stamp')
    return shaw_splink_ready

# COMMAND ----------

# This Function adds a coloumn of our specified Blocking variable/ID. 
# Note : Blocking variable/ID will be used later inside Splink to generate pair only when Blocking variable/ID matches.
# Note : Here for Address Matching our Blocking Variable/ID : First letter of First Name + Last Name + Mailing Address City + Mailing Address State 
def rogers_add_blocking_id(r_df_without_blocking):
    r_blocking_scheme = concat(r_df_without_blocking.r_dq_first_name_cl.substr(1,1),lit('_'),r_df_without_blocking.r_dq_last_name_cl,lit('_'),r_df_without_blocking.r_city_cl,lit('_'),r_df_without_blocking.r_state_cl)
    r_df_with_blocking  = r_df_without_blocking.withColumn("blocking_id", r_blocking_scheme)
    return r_df_with_blocking

# COMMAND ----------

# This Function as a part of Splink's requirement rename each coloumn/attribute of interest to make it similar in both data source.
r_coloumns_of_interest = ['r_rcis_id_cl','r_fa_id_cl','r_dq_first_name_cl','r_dq_last_name_cl','r_dq_e_mail_cl','r_dq_primary_phone_cl','r_dq_alternate_phone_1_cl','r_dq_alternate_phone_2_cl','r_mailing_address_no_zipcode_dq_cl','r_service_address_no_zipcode_dq_cl','r_sam_key','r_account_type','r_account_status','r_snapshot_stamp','unique_id','source_dataset','blocking_id']

def rogers_select_rename(rogers_df):
    r_temp_splink_ready = splink_ready_data(input_df=rogers_df,source_dataset_name="rogers")
    r_temp_splink_ready = rogers_add_blocking_id(r_df_without_blocking=r_temp_splink_ready)
    rogers_splink_ready = r_temp_splink_ready.select(r_coloumns_of_interest)\
    .withColumnRenamed('r_rcis_id_cl','rcis_id_cl')\
    .withColumnRenamed('r_fa_id_cl','fa_id_cl')\
    .withColumnRenamed('r_dq_first_name_cl','first_name_cl')\
    .withColumnRenamed('r_dq_last_name_cl','last_name_cl')\
    .withColumnRenamed('r_dq_e_mail_cl','e_mail_cl')\
    .withColumnRenamed('r_dq_primary_phone_cl','primary_phone_cl')\
    .withColumnRenamed('r_dq_alternate_phone_1_cl','alternate_phone_1_cl')\
    .withColumnRenamed('r_dq_alternate_phone_2_cl','alternate_phone_2_cl')\
    .withColumnRenamed('r_mailing_address_no_zipcode_dq_cl','mailing_address_no_zipcode_dq_cl')\
    .withColumnRenamed('r_service_address_no_zipcode_dq_cl','service_address_no_zipcode_dq_cl')\
    .withColumnRenamed('r_sam_key','sam_key')\
    .withColumnRenamed('r_account_type','account_type')\
    .withColumnRenamed('r_account_status','account_status')\
    .withColumnRenamed('r_snapshot_stamp','snapshot_stamp')
    return rogers_splink_ready

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration Rule 23
# MAGIC Name, Email, Phone Number and Mailing/Service Address

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hyperparameters            
# MAGIC ### em_convergence_rule23 : Convergence tolerance for the EM algorithm.
# MAGIC - The algorithm will stop converging when the maximum of the change in model parameters between iterations is below this value.
# MAGIC
# MAGIC ### max_iterations_rule23 : The maximum number of iterations to run even if convergence has not been reached.
# MAGIC - The algorithm will stop converging when maximum number of iterations has been reached.

# COMMAND ----------

# Mulitple iterations were performed to dervie these two parameters.

# COMMAND ----------

em_convergence_rule23 = 0.0001
max_iterations_rule23 = 5

# COMMAND ----------

# Each Parameter of Setting JSON is explained here : https://moj-analytical-services.github.io/splink_settings_editor/

# COMMAND ----------

# Setting JSON for Rule 23 : Name, Email, Phone Number and Mailing/Service Address
phone_expression = """
CASE 
WHEN primary_phone_cl_l is null AND alternate_phone_1_cl_l is null AND alternate_phone_2_cl_l is null then -1
WHEN primary_phone_cl_r is null AND alternate_phone_1_cl_r is null AND alternate_phone_2_cl_r is null then -1
WHEN primary_phone_cl_l = primary_phone_cl_r OR primary_phone_cl_l = alternate_phone_1_cl_r OR primary_phone_cl_l = alternate_phone_2_cl_r THEN 1
WHEN alternate_phone_1_cl_l = primary_phone_cl_r OR alternate_phone_1_cl_l = alternate_phone_1_cl_r OR alternate_phone_1_cl_l = alternate_phone_2_cl_r THEN 1
WHEN alternate_phone_2_cl_l = primary_phone_cl_r OR alternate_phone_2_cl_l = alternate_phone_1_cl_r OR alternate_phone_2_cl_l = alternate_phone_2_cl_r THEN 1
ELSE 0
END
"""
address_expression = """
CASE 
WHEN mailing_address_no_zipcode_dq_cl_l is null AND service_address_no_zipcode_dq_cl_l is null then -1
WHEN mailing_address_no_zipcode_dq_cl_r is null AND service_address_no_zipcode_dq_cl_r is null then -1
WHEN mailing_address_no_zipcode_dq_cl_l = service_address_no_zipcode_dq_cl_r OR service_address_no_zipcode_dq_cl_l = mailing_address_no_zipcode_dq_cl_r THEN 1
ELSE 0
END
"""
settings_rule23 = {
    "link_type": "link_only",
    "blocking_rules": ["l.blocking_id = r.blocking_id"],
    "comparison_columns": [
        {
            "col_name": "first_name_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "col_name": "mailing_address_no_zipcode_dq_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "col_name": "service_address_no_zipcode_dq_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "custom_name": "mailing_or_service_inversion",
            "custom_columns_used": ["mailing_address_no_zipcode_dq_cl", "service_address_no_zipcode_dq_cl"],
            "case_expression": address_expression,
            "num_levels": 2
        },
        {
            "col_name": "e_mail_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "custom_name": "phone_inversion",
            "custom_columns_used": ["primary_phone_cl", "alternate_phone_1_cl", "alternate_phone_2_cl"],
            "case_expression": phone_expression,
            "num_levels": 2
        }
    ],
    "em_convergence": em_convergence_rule23,
    "max_iterations": max_iterations_rule23,
    "additional_columns_to_retain":['last_name_cl','rcis_id_cl','fa_id_cl','sam_key','account_type','account_status','snapshot_stamp']
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration Rule 24
# MAGIC Name, Email and Phone Number

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hyperparameters            
# MAGIC ### em_convergence_rule24 : Convergence tolerance for the EM algorithm.
# MAGIC - The algorithm will stop converging when the maximum of the change in model parameters between iterations is below this value.
# MAGIC ### max_iterations_rule24 : The maximum number of iterations to run even if convergence has not been reached.
# MAGIC - The algorithm will stop converging when maximum number of iterations has been reached.

# COMMAND ----------

#Mulitple iterations were performed to dervie these two parameters.

# COMMAND ----------

em_convergence_rule24 = 0.0001
max_iterations_rule24 = 5

# COMMAND ----------

# Each Parameter of Setting JSON is explained here : https://moj-analytical-services.github.io/splink_settings_editor/

# COMMAND ----------

#Setting JSON for Rule 24 : Name, Email and Phone Number
phone_expression = """
CASE 
WHEN primary_phone_cl_l is null AND alternate_phone_1_cl_l is null AND alternate_phone_2_cl_l is null then -1
WHEN primary_phone_cl_r is null AND alternate_phone_1_cl_r is null AND alternate_phone_2_cl_r is null then -1
WHEN primary_phone_cl_l = primary_phone_cl_r OR primary_phone_cl_l = alternate_phone_1_cl_r OR primary_phone_cl_l = alternate_phone_2_cl_r THEN 1
WHEN alternate_phone_1_cl_l = primary_phone_cl_r OR alternate_phone_1_cl_l = alternate_phone_1_cl_r OR alternate_phone_1_cl_l = alternate_phone_2_cl_r THEN 1
WHEN alternate_phone_2_cl_l = primary_phone_cl_r OR alternate_phone_2_cl_l = alternate_phone_1_cl_r OR alternate_phone_2_cl_l = alternate_phone_2_cl_r THEN 1
ELSE 0
END
"""
settings_rule24 = {
    "link_type": "link_only",
    "blocking_rules": ["l.blocking_id = r.blocking_id"],
    "comparison_columns": [
        {
            "col_name": "first_name_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "col_name": "e_mail_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "custom_name": "phone_inversion",
            "custom_columns_used": ["primary_phone_cl", "alternate_phone_1_cl", "alternate_phone_2_cl"],
            "case_expression": phone_expression,
            "num_levels": 2
        }
    ],
    "em_convergence": em_convergence_rule24,
    "max_iterations": max_iterations_rule24,
    "additional_columns_to_retain":['last_name_cl','rcis_id_cl','fa_id_cl','sam_key','account_type','account_status','snapshot_stamp']
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration Rule 25
# MAGIC Name, Email and Mailing/Service Address

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hyperparameters            
# MAGIC ### em_convergence_rule25 : Convergence tolerance for the EM algorithm.
# MAGIC - The algorithm will stop converging when the maximum of the change in model parameters between iterations is below this value.
# MAGIC
# MAGIC ### max_iterations_rule25 : The maximum number of iterations to run even if convergence has not been reached.
# MAGIC - The algorithm will stop converging when maximum number of iterations has been reached.

# COMMAND ----------

#Mulitple iterations were performed to dervie these two parameters.

# COMMAND ----------

em_convergence_rule25 = 0.0001
max_iterations_rule25 = 5

# COMMAND ----------

# Each Parameter of Setting JSON is explained here : https://moj-analytical-services.github.io/splink_settings_editor/

# COMMAND ----------

#Setting JSON for Rule 25 : Name, Email and Mailing/Service Address
address_expression = """
CASE 
WHEN mailing_address_no_zipcode_dq_cl_l is null AND service_address_no_zipcode_dq_cl_l is null then -1
WHEN mailing_address_no_zipcode_dq_cl_r is null AND service_address_no_zipcode_dq_cl_r is null then -1
WHEN mailing_address_no_zipcode_dq_cl_l = service_address_no_zipcode_dq_cl_r OR service_address_no_zipcode_dq_cl_l = mailing_address_no_zipcode_dq_cl_r THEN 1
ELSE 0
END
"""
settings_rule25 = {
    "link_type": "link_only",
    "blocking_rules": ["l.blocking_id = r.blocking_id"],
    "comparison_columns": [
        {
            "col_name": "first_name_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "col_name": "e_mail_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "col_name": "mailing_address_no_zipcode_dq_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "col_name": "service_address_no_zipcode_dq_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "custom_name": "mailing_or_service_inversion",
            "custom_columns_used": ["mailing_address_no_zipcode_dq_cl", "service_address_no_zipcode_dq_cl"],
            "case_expression": address_expression,
            "num_levels": 2
        }
    ],
    "em_convergence": em_convergence_rule25,
    "max_iterations": max_iterations_rule25,
    "additional_columns_to_retain":['last_name_cl','rcis_id_cl','fa_id_cl','sam_key','account_type','account_status','snapshot_stamp']
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration Rule 26
# MAGIC Name, Phone and Mailing/Service Address

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hyperparameters            
# MAGIC ### em_convergence_rule26 : Convergence tolerance for the EM algorithm.
# MAGIC - The algorithm will stop converging when the maximum of the change in model parameters between iterations is below this value.
# MAGIC
# MAGIC ### max_iterations_rule26 : The maximum number of iterations to run even if convergence has not been reached.
# MAGIC - The algorithm will stop converging when maximum number of iterations has been reached.

# COMMAND ----------

#Mulitple iterations were performed to dervie these two parameters.

# COMMAND ----------

em_convergence_rule26 = 0.0001
max_iterations_rule26 = 5

# COMMAND ----------

# Each Parameter of Setting JSON is explained here : https://moj-analytical-services.github.io/splink_settings_editor/

# COMMAND ----------

#Setting JSON for Rule 26 : Name, Phone and Mailing/Service Address
phone_expression = """
CASE 
WHEN primary_phone_cl_l is null AND alternate_phone_1_cl_l is null AND alternate_phone_2_cl_l is null then -1
WHEN primary_phone_cl_r is null AND alternate_phone_1_cl_r is null AND alternate_phone_2_cl_r is null then -1
WHEN primary_phone_cl_l = primary_phone_cl_r OR primary_phone_cl_l = alternate_phone_1_cl_r OR primary_phone_cl_l = alternate_phone_2_cl_r THEN 1
WHEN alternate_phone_1_cl_l = primary_phone_cl_r OR alternate_phone_1_cl_l = alternate_phone_1_cl_r OR alternate_phone_1_cl_l = alternate_phone_2_cl_r THEN 1
WHEN alternate_phone_2_cl_l = primary_phone_cl_r OR alternate_phone_2_cl_l = alternate_phone_1_cl_r OR alternate_phone_2_cl_l = alternate_phone_2_cl_r THEN 1
ELSE 0
END
"""
address_expression = """
CASE 
WHEN mailing_address_no_zipcode_dq_cl_l is null AND service_address_no_zipcode_dq_cl_l is null then -1
WHEN mailing_address_no_zipcode_dq_cl_r is null AND service_address_no_zipcode_dq_cl_r is null then -1
WHEN mailing_address_no_zipcode_dq_cl_l = service_address_no_zipcode_dq_cl_r OR service_address_no_zipcode_dq_cl_l = mailing_address_no_zipcode_dq_cl_r THEN 1
ELSE 0
END
"""
settings_rule26 = {
    "link_type": "link_only",
    "blocking_rules": ["l.blocking_id = r.blocking_id"],
    "comparison_columns": [
        {
            "col_name": "first_name_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "col_name": "mailing_address_no_zipcode_dq_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "col_name": "service_address_no_zipcode_dq_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "custom_name": "mailing_or_service_inversion",
            "custom_columns_used": ["mailing_address_no_zipcode_dq_cl", "service_address_no_zipcode_dq_cl"],
            "case_expression": address_expression,
            "num_levels": 2
        },
        {
            "custom_name": "phone_inversion",
            "custom_columns_used": ["primary_phone_cl", "alternate_phone_1_cl", "alternate_phone_2_cl"],
            "case_expression": phone_expression,
            "num_levels": 2
        }
    ],
    "em_convergence": em_convergence_rule26,
    "max_iterations": max_iterations_rule26,
    "additional_columns_to_retain":['last_name_cl','rcis_id_cl','fa_id_cl','sam_key','account_type','account_status','snapshot_stamp']
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration Rule 27
# MAGIC Name and Email

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hyperparameters            
# MAGIC ### em_convergence_rule27 : Convergence tolerance for the EM algorithm.
# MAGIC - The algorithm will stop converging when the maximum of the change in model parameters between iterations is below this value.
# MAGIC
# MAGIC ### max_iterations_rule27 : The maximum number of iterations to run even if convergence has not been reached.
# MAGIC - The algorithm will stop converging when maximum number of iterations has been reached.

# COMMAND ----------

#Mulitple iterations were performed to dervie these two parameters.

# COMMAND ----------

em_convergence_rule27 = 0.0001
max_iterations_rule27 = 5

# COMMAND ----------

# Each Parameter of Setting JSON is explained here : https://moj-analytical-services.github.io/splink_settings_editor/

# COMMAND ----------

#Setting JSON for Rule 27 : Name and Email
settings_rule27 = {
    "link_type": "link_only",
    "blocking_rules": ["l.blocking_id = r.blocking_id"],
    "comparison_columns": [
        {
            "col_name": "first_name_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "col_name": "e_mail_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        }
    ],
    "em_convergence": em_convergence_rule27,
    "max_iterations": max_iterations_rule27,
    "additional_columns_to_retain":['last_name_cl','rcis_id_cl','fa_id_cl','sam_key','account_type','account_status','snapshot_stamp']
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration Rule 28
# MAGIC Name and Phone

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hyperparameters            
# MAGIC ### em_convergence_rule28 : Convergence tolerance for the EM algorithm.
# MAGIC - The algorithm will stop converging when the maximum of the change in model parameters between iterations is below this value.
# MAGIC
# MAGIC ### max_iterations_rule28 : The maximum number of iterations to run even if convergence has not been reached.
# MAGIC - The algorithm will stop converging when maximum number of iterations has been reached.

# COMMAND ----------

#Mulitple iterations were performed to dervie these two parameters.

# COMMAND ----------

em_convergence_rule28 = 0.0001
max_iterations_rule28 = 5

# COMMAND ----------

# Each Parameter of Setting JSON is explained here : https://moj-analytical-services.github.io/splink_settings_editor/

# COMMAND ----------

#Setting JSON for Rule 28 : Name and Phone
phone_expression = """
CASE 
WHEN primary_phone_cl_l is null AND alternate_phone_1_cl_l is null AND alternate_phone_2_cl_l is null then -1
WHEN primary_phone_cl_r is null AND alternate_phone_1_cl_r is null AND alternate_phone_2_cl_r is null then -1
WHEN primary_phone_cl_l = primary_phone_cl_r OR primary_phone_cl_l = alternate_phone_1_cl_r OR primary_phone_cl_l = alternate_phone_2_cl_r THEN 1
WHEN alternate_phone_1_cl_l = primary_phone_cl_r OR alternate_phone_1_cl_l = alternate_phone_1_cl_r OR alternate_phone_1_cl_l = alternate_phone_2_cl_r THEN 1
WHEN alternate_phone_2_cl_l = primary_phone_cl_r OR alternate_phone_2_cl_l = alternate_phone_1_cl_r OR alternate_phone_2_cl_l = alternate_phone_2_cl_r THEN 1
ELSE 0
END
"""
settings_rule28 = {
    "link_type": "link_only",
    "blocking_rules": ["l.blocking_id = r.blocking_id"],
    "comparison_columns": [
        {
            "col_name": "first_name_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "custom_name": "phone_inversion",
            "custom_columns_used": ["primary_phone_cl", "alternate_phone_1_cl", "alternate_phone_2_cl"],
            "case_expression": phone_expression,
            "num_levels": 2
        }
    ],
    "em_convergence": em_convergence_rule28,
    "max_iterations": max_iterations_rule28,
    "additional_columns_to_retain":['last_name_cl','rcis_id_cl','fa_id_cl','sam_key','account_type','account_status','snapshot_stamp']
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration Rule 29
# MAGIC Name and Mailing/Service Address

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hyperparameters            
# MAGIC ### em_convergence_rule29 : Convergence tolerance for the EM algorithm.
# MAGIC - The algorithm will stop converging when the maximum of the change in model parameters between iterations is below this value.
# MAGIC
# MAGIC ### max_iterations_rule29 : The maximum number of iterations to run even if convergence has not been reached.
# MAGIC - The algorithm will stop converging when maximum number of iterations has been reached.

# COMMAND ----------

#Mulitple iterations were performed to dervie these two parameters.

# COMMAND ----------

em_convergence_rule29 = 0.0001
max_iterations_rule29 = 5

# COMMAND ----------

# Each Parameter of Setting JSON is explained here : https://moj-analytical-services.github.io/splink_settings_editor/

# COMMAND ----------

#Setting JSON for Rule 29 : Name and Mailing/Service Address
address_expression = """
CASE 
WHEN mailing_address_no_zipcode_dq_cl_l is null AND service_address_no_zipcode_dq_cl_l is null then -1
WHEN mailing_address_no_zipcode_dq_cl_r is null AND service_address_no_zipcode_dq_cl_r is null then -1
WHEN mailing_address_no_zipcode_dq_cl_l = service_address_no_zipcode_dq_cl_r OR service_address_no_zipcode_dq_cl_l = mailing_address_no_zipcode_dq_cl_r THEN 1
ELSE 0
END
"""
settings_rule29 = {
    "link_type": "link_only",
    "blocking_rules": ["l.blocking_id = r.blocking_id"],
    "comparison_columns": [
        {
            "col_name": "first_name_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "col_name": "mailing_address_no_zipcode_dq_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "col_name": "service_address_no_zipcode_dq_cl",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "custom_name": "mailing_or_service_inversion",
            "custom_columns_used": ["mailing_address_no_zipcode_dq_cl", "service_address_no_zipcode_dq_cl"],
            "case_expression": address_expression,
            "num_levels": 2
        },
    ],
    "em_convergence": em_convergence_rule29,
    "max_iterations": max_iterations_rule29,
    "additional_columns_to_retain":['last_name_cl','rcis_id_cl','fa_id_cl','sam_key','account_type','account_status','snapshot_stamp']
}
