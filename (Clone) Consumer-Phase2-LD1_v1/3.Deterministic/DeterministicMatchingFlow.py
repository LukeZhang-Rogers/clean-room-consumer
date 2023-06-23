# Databricks notebook source
# MAGIC %md ## Executing Deterministic Matching Flow
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC
# MAGIC This is the main notebook that executes the logic end-to-end to generate the interim output datasets that will be modelled downstream based on the output model design. Starts with executing the (Shaw/Rogers)FoundationDataset notebooks to build foundation datasets. In the next step, deteministic matching rules are executed in 3 successive steps (DeterministicMatchingRules(1-15,30-32), DeterministicMatchingRules(35-49), DeterministicMatchingRules(50-61)) to execute the core matching logic. The matched datasets are then built in preparation for final output modelling.
# MAGIC
# MAGIC
# MAGIC ### Requirements:
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC   <li>(Shaw/Rogers)FoundationDataset notebooks are configured to ingest cleansed datasets and generate foundation tables</li>
# MAGIC   <li>DeterministicMatchingRules(1-15,30-32) notebook is configured to execute the matching rules and generate results for each ruleset</li>
# MAGIC   <li>DeterministicMatchingRules(35-49) notebook is configured to execute the matching rules and generate results for each ruleset</li>
# MAGIC   <li>DeterministicMatchingRules(50-61) notebook is configured to execute the matching rules and generate results for each ruleset</li>
# MAGIC
# MAGIC </ul>
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Parameters:
# MAGIC
# MAGIC <b>execution_mode:</b> Set to 1 (default) to execute the flow. Set to 0 for unit testing.</br>
# MAGIC <b>overwrite_output:</b> Set to 1 to overwrite the outputs. Set to 0 (default) to not overwrite the outputs .</br>
# MAGIC <b>include_shawmobile:</b> Set to 1 (default) if you want to include Shaw Mobile customers. Set to 0 if you do not want to include Shaw Mobile customers .</br>

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configuration

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col,desc, concat, lower
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col,desc,count
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../2.Common/ADLSDesignedPaths

# COMMAND ----------

# DBTITLE 1,Uncomment and execute if widgets are not imported
# dbutils.widgets.text(name = "overwrite_output", defaultValue = "0")
# dbutils.widgets.text(name = "execution_mode", defaultValue = "1")
# dbutils.widgets.text(name = "include_shawmobile", defaultValue = "1")

# COMMAND ----------

var_overwrite_output = dbutils.widgets.get("overwrite_output")
var_execution_mode   = dbutils.widgets.get("execution_mode")
var_include_shawmobile = dbutils.widgets.get("include_shawmobile")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Source Notebooks

# COMMAND ----------

if (var_execution_mode == '1'):
    r_wls_c_joined      =  spark.read.format("parquet").load(foundation_rogers_wls_c)
    r_wln_c_joined      =  spark.read.format("parquet").load(foundation_rogers_wln_c)
    s_wls_c_joined      =  spark.read.format("parquet").load(foundation_shaw_wls_c)
    s_wln_c_joined      =  spark.read.format("parquet").load(foundation_shaw_wln_c)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Matching Rules

# COMMAND ----------

# MAGIC %run ./DeterministicMatchingRules(1-15,30-32)

# COMMAND ----------

# MAGIC %run ./DeterministicMatchingRules(35-49)

# COMMAND ----------

# MAGIC %run ./DeterministicMatchingRules(50-61)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Union Checks

# COMMAND ----------

# rule 1
Rule1 = Rule1_R2S2

# COMMAND ----------

# rule2
df1_temp = Rule2_R2S1.unionByName(Rule2_R2S2, allowMissingColumns=True)
df2_temp = Rule2_R1S2.unionByName(Rule2_R1S1, allowMissingColumns=True)

Rule2    = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# COMMAND ----------

# rule3
df1_temp = Rule3_R2S1.unionByName(Rule3_R2S2, allowMissingColumns=True)
df2_temp = Rule3_R1S2.unionByName(Rule3_R1S1, allowMissingColumns=True)
Rule3 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# COMMAND ----------

# rule4
df1_temp = Rule4_R2S1.unionByName(Rule4_R2S2, allowMissingColumns=True)
df2_temp = Rule4_R1S2.unionByName(Rule4_R1S1, allowMissingColumns=True)
Rule4 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# COMMAND ----------

# rule5
Rule5 = Rule5_R2S2

# COMMAND ----------

# rule6
df1_temp = Rule6_R2S1.unionByName(Rule6_R2S2, allowMissingColumns=True)
df2_temp = Rule6_R1S2.unionByName(Rule6_R1S1, allowMissingColumns=True)
Rule6 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# COMMAND ----------

# rule7
df1_temp = Rule7_R2S1.unionByName(Rule7_R2S2, allowMissingColumns=True)
df2_temp = Rule7_R1S2.unionByName(Rule7_R1S1, allowMissingColumns=True)
Rule7 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# COMMAND ----------

# rule8
Rule8 = Rule8_R2S2

# COMMAND ----------

# rule9
df1_temp = Rule9_R2S1.unionByName(Rule9_R2S2, allowMissingColumns=True)
df2_temp = Rule9_R1S2.unionByName(Rule9_R1S1, allowMissingColumns=True)
Rule9 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# COMMAND ----------

# rule10
df1_temp = Rule10_R2S1.unionByName(Rule10_R2S2, allowMissingColumns=True)
df2_temp = Rule10_R1S2.unionByName(Rule10_R1S1, allowMissingColumns=True)
Rule10 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# COMMAND ----------

# rule11
df1_temp = Rule11_R2S1.unionByName(Rule11_R2S2, allowMissingColumns=True)
df2_temp = Rule11_R1S2.unionByName(Rule11_R1S1, allowMissingColumns=True)
Rule11 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# COMMAND ----------

# rule12
df1_temp = Rule12_R2S1.unionByName(Rule12_R2S2, allowMissingColumns=True)
df2_temp = Rule12_R1S2.unionByName(Rule12_R1S1, allowMissingColumns=True)
Rule12 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# COMMAND ----------

# rule13
Rule13 = Rule13_R2S2

# COMMAND ----------

# rule14
df1_temp = Rule14_R2S1.unionByName(Rule14_R2S2, allowMissingColumns=True)
df2_temp = Rule14_R1S2.unionByName(Rule14_R1S1, allowMissingColumns=True)
Rule14 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# COMMAND ----------

# rule15
df1_temp = Rule15_R2S1.unionByName(Rule15_R2S2, allowMissingColumns=True)
df2_temp = Rule15_R1S2.unionByName(Rule15_R1S1, allowMissingColumns=True)
Rule15 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# COMMAND ----------

# rule30
Rule30 = Rule30_R2S2

# COMMAND ----------

# rule31
df1_temp = Rule31_R2S1.unionByName(Rule31_R2S2, allowMissingColumns=True)
df2_temp = Rule31_R1S2.unionByName(Rule31_R1S1, allowMissingColumns=True)
Rule31 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# COMMAND ----------

# rule32
df1_temp = Rule32_R2S1.unionByName(Rule32_R2S2, allowMissingColumns=True)
df2_temp = Rule32_R1S2.unionByName(Rule32_R1S1, allowMissingColumns=True)
Rule32 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# COMMAND ----------

#New Determinisitic Rules 35 - 49

# rule 35
Rule35   = Rule35_R2S2

# rule36
df1_temp = Rule36_R2S1.unionByName(Rule36_R2S2, allowMissingColumns=True)
df2_temp = Rule36_R1S2.unionByName(Rule36_R1S1, allowMissingColumns=True)

Rule36    = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule37
df1_temp = Rule37_R2S1.unionByName(Rule37_R2S2, allowMissingColumns=True)
df2_temp = Rule37_R1S2.unionByName(Rule37_R1S1, allowMissingColumns=True)
Rule37 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule38
df1_temp = Rule38_R2S1.unionByName(Rule38_R2S2, allowMissingColumns=True)
df2_temp = Rule38_R1S2.unionByName(Rule38_R1S1, allowMissingColumns=True)
Rule38 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)


# rule39
Rule39 = Rule39_R2S2

# rule40
df1_temp = Rule40_R2S1.unionByName(Rule40_R2S2, allowMissingColumns=True)
df2_temp = Rule40_R1S2.unionByName(Rule40_R1S1, allowMissingColumns=True)
Rule40 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule41
df1_temp = Rule41_R2S1.unionByName(Rule41_R2S2, allowMissingColumns=True)
df2_temp = Rule41_R1S2.unionByName(Rule41_R1S1, allowMissingColumns=True)
Rule41 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule42
Rule42 = Rule42_R2S2

# rule43
df1_temp = Rule43_R2S1.unionByName(Rule43_R2S2, allowMissingColumns=True)
df2_temp = Rule43_R1S2.unionByName(Rule43_R1S1, allowMissingColumns=True)
Rule43 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule44
df1_temp = Rule44_R2S1.unionByName(Rule44_R2S2, allowMissingColumns=True)
df2_temp = Rule44_R1S2.unionByName(Rule44_R1S1, allowMissingColumns=True)
Rule44 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule45
df1_temp = Rule45_R2S1.unionByName(Rule45_R2S2, allowMissingColumns=True)
df2_temp = Rule45_R1S2.unionByName(Rule45_R1S1, allowMissingColumns=True)
Rule45 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule46
df1_temp = Rule46_R2S1.unionByName(Rule46_R2S2, allowMissingColumns=True)
df2_temp = Rule46_R1S2.unionByName(Rule46_R1S1, allowMissingColumns=True)
Rule46 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule47
Rule47 = Rule47_R2S2

# rule48
df1_temp = Rule48_R2S1.unionByName(Rule48_R2S2, allowMissingColumns=True)
df2_temp = Rule48_R1S2.unionByName(Rule48_R1S1, allowMissingColumns=True)
Rule48 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule49
df1_temp = Rule49_R2S1.unionByName(Rule49_R2S2, allowMissingColumns=True)
df2_temp = Rule49_R1S2.unionByName(Rule49_R1S1, allowMissingColumns=True)
Rule49 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# COMMAND ----------

#New Determinisitic Rules 50 - 61

# rule 50
Rule50   = Rule50_R2S2

# rule51
df1_temp = Rule51_R2S1.unionByName(Rule51_R2S2, allowMissingColumns=True)
df2_temp = Rule51_R1S2.unionByName(Rule51_R1S1, allowMissingColumns=True)

Rule51    = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule52
df1_temp = Rule52_R2S1.unionByName(Rule52_R2S2, allowMissingColumns=True)
df2_temp = Rule52_R1S2.unionByName(Rule52_R1S1, allowMissingColumns=True)
Rule52 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule53
df1_temp = Rule53_R2S1.unionByName(Rule53_R2S2, allowMissingColumns=True)
df2_temp = Rule53_R1S2.unionByName(Rule53_R1S1, allowMissingColumns=True)
Rule53 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule54
Rule54 = Rule54_R2S2

# rule55
df1_temp = Rule55_R2S1.unionByName(Rule55_R2S2, allowMissingColumns=True)
df2_temp = Rule55_R1S2.unionByName(Rule55_R1S1, allowMissingColumns=True)
Rule55 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule56
df1_temp = Rule56_R2S1.unionByName(Rule56_R2S2, allowMissingColumns=True)
df2_temp = Rule56_R1S2.unionByName(Rule56_R1S1, allowMissingColumns=True)
Rule56 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule57
Rule57 = Rule57_R2S2

# rule58
df1_temp = Rule58_R2S1.unionByName(Rule58_R2S2, allowMissingColumns=True)
df2_temp = Rule58_R1S2.unionByName(Rule58_R1S1, allowMissingColumns=True)
Rule58 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule59
df1_temp = Rule59_R2S1.unionByName(Rule59_R2S2, allowMissingColumns=True)
df2_temp = Rule59_R1S2.unionByName(Rule59_R1S1, allowMissingColumns=True)
Rule59 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule60
df1_temp = Rule60_R2S1.unionByName(Rule60_R2S2, allowMissingColumns=True)
df2_temp = Rule60_R1S2.unionByName(Rule60_R1S1, allowMissingColumns=True)
Rule60 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# rule61
df1_temp = Rule61_R2S1.unionByName(Rule61_R2S2, allowMissingColumns=True)
df2_temp = Rule61_R1S2.unionByName(Rule61_R1S1, allowMissingColumns=True)
Rule61 = df1_temp.unionByName(df2_temp, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Matched Dataset

# COMMAND ----------

#Bringing all the matched rulesets together before persisting to storage
matched_dataset_1 = Rule1.unionByName(Rule2).unionByName(Rule3).unionByName(Rule4).unionByName(Rule5).unionByName(Rule6).unionByName(Rule7).unionByName(Rule8).unionByName(Rule9).unionByName(Rule10)\
                    .unionByName(Rule11).unionByName(Rule12).unionByName(Rule13).unionByName(Rule14).unionByName(Rule15).cache()

matched_dataset_2 = Rule35.unionByName(Rule36).unionByName(Rule37).unionByName(Rule38).unionByName(Rule39).unionByName(Rule40).unionByName(Rule41).unionByName(Rule42).unionByName(Rule43)\
                    .unionByName(Rule44).unionByName(Rule45).unionByName(Rule46).unionByName(Rule47).unionByName(Rule48).unionByName(Rule49).cache()

matched_dataset_3 = Rule50.unionByName(Rule51).unionByName(Rule52).unionByName(Rule53).unionByName(Rule54).unionByName(Rule55).cache()

matched_dataset_4 = Rule56.unionByName(Rule57).cache()

matched_dataset_5 = Rule58.unionByName(Rule59).cache()

matched_dataset_6 = Rule60.cache()

matched_dataset_7 = Rule61.cache()

matched_dataset_8 = Rule30.unionByName(Rule32).cache()

matched_dataset_9 = Rule31.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC # Aligning the columns before appending and storing

# COMMAND ----------

l = ["s_contact_id", "s_rcis_id_cl", "s_fa_id_cl", "s_dq_first_name_cl", "s_dq_last_name_cl", "s_dq_full_name_cl", "s_dq_primary_phone_cl", "s_dq_alternate_phone_1_cl", "s_dq_alternate_phone_2_cl", "s_dq_e_mail_cl", "s_address_cl", "s_city_cl", "s_state_cl", "s_zipcode_cl", "s_snapshot_stamp", "s_execution_date", "s_mailing_address_no_zipcode_dq_cl", "s_mailing_address_full_cl", "s_mailing_postalcode_cl", "s_source_system", "s_account_status", "s_service_provider", "s_service_address_cl", "s_sam_key", "s_service_address_no_zipcode_dq_cl", "s_service_address_full_cl", "s_service_postalcode_cl", "s_account_type", "r_contact_id", "r_rcis_id_cl", "r_fa_id_cl", "r_dq_first_name_cl", "r_dq_last_name_cl", "r_dq_full_name_cl", "r_dq_primary_phone_cl", "r_dq_alternate_phone_1_cl", "r_dq_alternate_phone_2_cl", "r_dq_e_mail_cl", "r_address_cl", "r_city_cl", "r_state_cl", "r_zipcode_cl", "r_snapshot_stamp", "r_execution_date", "r_mailing_address_no_zipcode_dq_cl", "r_mailing_address_full_cl", "r_mailing_postalcode_cl", "r_source_system", "r_account_status", "r_service_provider", "r_service_address_cl", "r_sam_key", "r_service_address_no_zipcode_dq_cl", "r_service_address_full_cl", "r_service_postalcode_cl", "r_account_type", "Ruleset_ID"]

# COMMAND ----------

#Bringing all the matched rulesets together before persisting to storage
matched_dataset_1 = matched_dataset_1.select(l)

matched_dataset_2 = matched_dataset_2.select(l)

matched_dataset_3 = matched_dataset_3.select(l)

matched_dataset_4 = matched_dataset_4.select(l)

matched_dataset_5 = matched_dataset_5.select(l)

matched_dataset_6 = matched_dataset_6.select(l)

matched_dataset_7 = matched_dataset_7.select(l)

matched_dataset_8 = matched_dataset_8.select(l)

matched_dataset_9 = matched_dataset_9.select(l)

# COMMAND ----------

#output 
if var_overwrite_output=='1':
    matched_dataset_1.write.parquet(interimop_det_matched,mode='append')
    matched_dataset_2.write.parquet(interimop_det_matched,mode='append')
    matched_dataset_3.write.parquet(interimop_det_matched,mode='append')
    matched_dataset_4.write.parquet(interimop_det_matched,mode='append')
    matched_dataset_5.write.parquet(interimop_det_matched,mode='append')
    matched_dataset_6.write.parquet(interimop_det_matched,mode='append')
    matched_dataset_7.write.parquet(interimop_det_matched,mode='append')
    matched_dataset_8.write.parquet(interimop_det_matched,mode='append')
    matched_dataset_9.write.parquet(interimop_det_matched,mode='append')

# COMMAND ----------

if var_execution_mode=='1':
    matched_dataset_1.unpersist()
    matched_dataset_2.unpersist()
    matched_dataset_3.unpersist()
    matched_dataset_4.unpersist()
    matched_dataset_5.unpersist()
    matched_dataset_6.unpersist()
    matched_dataset_7.unpersist()
    matched_dataset_8.unpersist()
    matched_dataset_9.unpersist()

# COMMAND ----------


