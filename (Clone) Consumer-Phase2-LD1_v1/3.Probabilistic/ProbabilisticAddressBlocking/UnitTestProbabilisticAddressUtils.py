# Databricks notebook source
# MAGIC %md
# MAGIC ## Probabilistic Matching Address : Utility Functions (Unit Testing)
# MAGIC ## Description:
# MAGIC This notebook is called from the UnitTestProbabilisticMatchingAddress notebook to execute the address matching rule 34 : Match on Mailing/Service Address.
# MAGIC ## Requirements:
# MAGIC - Libraries : splink 2.1.12 and altair 4.2.0

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports

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

# Note : These columns do not affect learning or model parameters , it is just a way of Splink to get two data.
def splink_ready_data(input_df,source_dataset_name):
    splink_ready_df = input_df.coalesce(1).withColumn("unique_id",monotonically_increasing_id()).withColumn("source_dataset",lit(source_dataset_name))
    return splink_ready_df

# COMMAND ----------

# This Function as a part of Splink's requirement rename each coloumn/attribute of interest to make it similar in both data source.
s_coloumns_of_interest = ['s_rcis_id_cl','s_fa_id_cl','s_mailing_address_no_zipcode_dq_cl','s_service_address_no_zipcode_dq_cl','s_account_type','unique_id','source_dataset']
def shaw_select_rename(shaw_df):
    s_temp_splink_ready = splink_ready_data(input_df=shaw_df,source_dataset_name="shaw")
    shaw_splink_ready = s_temp_splink_ready.select(s_coloumns_of_interest)\
    .withColumnRenamed('s_rcis_id_cl','rcis_id_cl')\
    .withColumnRenamed('s_fa_id_cl','fa_id_cl')\
    .withColumnRenamed('s_mailing_address_no_zipcode_dq_cl','mailing_address_no_zipcode_dq_cl')\
    .withColumnRenamed('s_service_address_no_zipcode_dq_cl','service_address_no_zipcode_dq_cl')\
    .withColumnRenamed('s_account_type','account_type')
    return shaw_splink_ready

# COMMAND ----------

# This Function as a part of Splink's requirement rename each coloumn/attribute of interest to make it similar in both data source.
r_coloumns_of_interest = ['r_rcis_id_cl','r_fa_id_cl','r_mailing_address_no_zipcode_dq_cl','r_service_address_no_zipcode_dq_cl','r_account_type','unique_id','source_dataset']

def rogers_select_rename(rogers_df):
    r_temp_splink_ready = splink_ready_data(input_df=rogers_df,source_dataset_name="rogers")
    rogers_splink_ready = r_temp_splink_ready.select(r_coloumns_of_interest)\
    .withColumnRenamed('r_rcis_id_cl','rcis_id_cl')\
    .withColumnRenamed('r_fa_id_cl','fa_id_cl')\
    .withColumnRenamed('r_mailing_address_no_zipcode_dq_cl','mailing_address_no_zipcode_dq_cl')\
    .withColumnRenamed('r_service_address_no_zipcode_dq_cl','service_address_no_zipcode_dq_cl')\
    .withColumnRenamed('r_account_type','account_type')
    return rogers_splink_ready

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration Rule 34
# MAGIC Mailing/Service Address 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hyperparameters
# MAGIC ### em_convergence_rule34 : Convergence tolerance for the EM algorithm.
# MAGIC - The algorithm will stop converging when the maximum of the change in model parameters between iterations is below this value. 
# MAGIC
# MAGIC ### max_iterations_rule34 : The maximum number of iterations to run even if convergence has not been reached.
# MAGIC - The algorithm will stop converging when maximum number of iterations has been reached.

# COMMAND ----------

em_convergence_rule34 = 0.000001
max_iterations_rule34 = 10

# COMMAND ----------

# Each Parameter of Setting JSON is explained here : https://moj-analytical-services.github.io/splink_settings_editor/

# COMMAND ----------

address_expression = """
CASE 
WHEN mailing_address_no_zipcode_dq_cl_l is null AND service_address_no_zipcode_dq_cl_l is null then -1
WHEN mailing_address_no_zipcode_dq_cl_r is null AND service_address_no_zipcode_dq_cl_r is null then -1
WHEN mailing_address_no_zipcode_dq_cl_l = service_address_no_zipcode_dq_cl_r OR service_address_no_zipcode_dq_cl_l = mailing_address_no_zipcode_dq_cl_r THEN 1
ELSE 0
END
"""
settings_rule34 = {
    "link_type": "link_only",
    "blocking_rules": [],
    "comparison_columns": [
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
    "em_convergence": em_convergence_rule34,
    "max_iterations": max_iterations_rule34,
    "additional_columns_to_retain":['rcis_id_cl','fa_id_cl','account_type']
}
