# Databricks notebook source
# MAGIC %md
# MAGIC #EDA

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

#Cleaned Data

shaw_path = "/mnt/development/Processed/Iteration8/Shaw" 
rogers_path = "/mnt/development/Processed/Iteration8/Rogers" 

shaw_consumer_contact = shaw_path + '/Contact' 
shaw_consumer_wireless_account = shaw_path + '/WirelessAccount' 
shaw_consumer_wireline_account = shaw_path + '/WirelineAccount' 

rogers_contact = rogers_path + '/Contact' 
rogers_wireless_account = rogers_path + '/WirelessAccount' 
rogers_wireline_account = rogers_path + '/WirelineAccount'

#Load dataframes
cl_shaw_consumer_contact_df                = spark.read.format("parquet").load(shaw_consumer_contact)
cl_shaw_consumer_wireless_account_df       = spark.read.format("parquet").load(shaw_consumer_wireless_account)
cl_shaw_consumer_wireline_account_df       = spark.read.format("parquet").load(shaw_consumer_wireline_account)
#cl_shaw_consumer_wireless_service_df       = spark.read.format("parquet").load(shaw_consumer_wireless_service)

cl_rogers_contact_df                 = spark.read.format("parquet").load(rogers_contact)
cl_rogers_wireless_account_df        = spark.read.format("parquet").load(rogers_wireless_account)
cl_rogers_wireline_account_df        = spark.read.format("parquet").load(rogers_wireline_account)

# COMMAND ----------

#Load dataframes
AccountMatchedRuleset = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration3/AccountMatchedRuleset")
MatchedEntity = spark.read.format("parquet").load("/mnt/development/Processed/QA/Iteration3/MatchedEntity")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Counts

# COMMAND ----------

#Rogers

#Number of rows 
cl_Rogers_number_of_customers = cl_rogers_contact_df.distinct()
print('cl_Rogers_number_of_customers_count = ', cl_Rogers_number_of_customers.count())

#Distinct # of customers (rcis_id)
cl_Rogers_number_of_customers_distinct = cl_rogers_contact_df.select('rcis_id').distinct()
print('cl_Rogers_number_of_customers_count_distinct = ', cl_Rogers_number_of_customers_distinct.count())

#Distinct # of customers (rcis_id_cl)
cl_Rogers_number_of_customers_distinct_cl = cl_rogers_contact_df.select('rcis_id_cl').distinct()
print('cl_Rogers_number_of_customers_count_distinct_cl = ', cl_Rogers_number_of_customers_distinct_cl.count())

#Distinct # of accounts (fa_id)
cl_Rogers_number_of_accounts_distinct = cl_rogers_contact_df.select('fa_id').distinct()
print('cl_Rogers_number_of_accounts_count_distinct = ', cl_Rogers_number_of_accounts_distinct.count())

#Distinct # of accounts (fa_id_cl)
cl_Rogers_number_of_accounts_distinct_cl = cl_rogers_contact_df.select('fa_id_cl').distinct()
print('cl_Rogers_number_of_accounts_count_distinct_cl = ', cl_Rogers_number_of_accounts_distinct_cl.count())

#Distinct # of customers & account  (rcis_id & fa_id)
cl_Rogers_number_of_customers_and_account_distinct = cl_rogers_contact_df.select('rcis_id','fa_id').distinct()
print('cl_Rogers_number_of_customers_and_account_count_distinct = ', cl_Rogers_number_of_customers_and_account_distinct.count())


# COMMAND ----------

#Shaw 

#Number of rows 
cl_Shaw_number_of_customers = cl_shaw_consumer_contact_df.distinct()
print('cl_Shaw_number_of_customers_count = ', cl_Shaw_number_of_customers.count())

#Distinct # of customers (rcis_id)
cl_Shaw_number_of_customers_distinct = cl_shaw_consumer_contact_df.select('rcis_id').distinct()
print('cl_Shaw_number_of_customers_count_distinct = ', cl_Shaw_number_of_customers_distinct.count())

#Distinct # of customers (rcis_id_cl)
cl_Shaw_number_of_customers_distinct_cl = cl_shaw_consumer_contact_df.select('rcis_id_cl').distinct()
print('cl_Shaw_number_of_customers_count_distinct_cl = ', cl_Shaw_number_of_customers_distinct_cl.count())

#Distinct # of accounts (fa_id)
cl_Shaw_number_of_accounts_distinct = cl_shaw_consumer_contact_df.select('fa_id').distinct()
print('cl_Shaw_number_of_accounts_count_distinct = ', cl_Shaw_number_of_accounts_distinct.count())

#Distinct # of accounts (fa_id)
cl_Shaw_number_of_accounts_distinct_cl = cl_shaw_consumer_contact_df.select('fa_id_cl').distinct()
print('cl_Shaw_number_of_accounts_count_distinct_cl = ', cl_Shaw_number_of_accounts_distinct_cl.count())

#Distinct # of customers & account  (rcis_id & fa_id)
cl_Shaw_number_of_customers_and_account_distinct = cl_shaw_consumer_contact_df.select('rcis_id_cl','fa_id_cl').distinct()
print('cl_Shaw_number_of_customers_and_account_count_distinct = ', cl_Shaw_number_of_customers_and_account_distinct.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Matched_Entity

# COMMAND ----------

# MAGIC %md
# MAGIC ####Counts

# COMMAND ----------

#Total # of rows
MatchedEntity_total_rows = MatchedEntity.distinct()
print('MatchedEntity_total_rows_count = ',MatchedEntity_total_rows.count())

#Number of distinct Rogers customer id (ROGERS_ECID)
MatchedEntity_total_customers_Rogers = MatchedEntity.select('ROGERS_ECID').distinct()
print('MatchedEntity_total_customers_Rogers_count = ',MatchedEntity_total_customers_Rogers.count())

#Number of distinct Shaw customer id (SHAW_MASTER_PARTY_ID)
MatchedEntity_total_customers_Shaw = MatchedEntity.select('SHAW_MASTER_PARTY_ID').distinct()
print('MatchedEntity_total_customers_Shaw_count = ',MatchedEntity_total_customers_Shaw.count())

#Number of distinct records where both Rogers customer id (ROGERS_ECID) and Shaw customer id (SHAW_MASTER_PARTY_ID) are not Null
MatchedEntity_total_customers_Rogers_Shaw_Not_Null = MatchedEntity.filter((MatchedEntity.SHAW_MASTER_PARTY_ID.isNotNull()) & (MatchedEntity.ROGERS_ECID.isNotNull())).distinct()
print('MatchedEntity_total_customers_Rogers_Shaw_Not_Null = ',MatchedEntity_total_customers_Rogers_Shaw_Not_Null.count())

# COMMAND ----------

df_matchedentity_pair = MatchedEntity_total_customers_Rogers_Shaw_Not_Null.select('ROGERS_ECID','SHAW_MASTER_PARTY_ID').distinct()
df_matchedentity_pair.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Account_Matched_Ruleset

# COMMAND ----------

AccountMatchedRuleset.select('RULESET_TYPE').distinct().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Counts

# COMMAND ----------

#Total number of rows
AccountMatchedRuleset_total_rows = AccountMatchedRuleset
print('AccountMatchedRuleset_total_rows_count = ',AccountMatchedRuleset_total_rows.count())

#Rogers customer discitnt count
AccountMatchedRuleset_total_customer_Rogers = AccountMatchedRuleset.select('ROGERS_ECID').distinct()
print('AccountMatchedRuleset_total_customer_Rogers_count = ',AccountMatchedRuleset_total_customer_Rogers.count())

#Rogers account discitnt count
AccountMatchedRuleset_total_account_Rogers = AccountMatchedRuleset.select('ROGERS_ACCOUNT_ID').distinct()
print('AccountMatchedRuleset_total_account_Rogers_count = ',AccountMatchedRuleset_total_account_Rogers.count())

#Shaw customer discitnt count
AccountMatchedRuleset_total_customer_Shaw = AccountMatchedRuleset.select('SHAW_MASTER_PARTY_ID').distinct()
print('AccountMatchedRuleset_total_customer_Shaw_count = ',AccountMatchedRuleset_total_customer_Shaw.count())

#Shaw account discitnt count
AccountMatchedRuleset_total_account_Shaw = AccountMatchedRuleset.select('SHAW_ACCOUNT_ID').distinct()
print('AccountMatchedRuleset_total_account_Shaw_count = ',AccountMatchedRuleset_total_account_Shaw.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##TC-C-07 match rate

# COMMAND ----------

# MAGIC %md
# MAGIC ######Matched (deterministic only)

# COMMAND ----------

#Matched dataframe (deterministic only)
matched_AccountMatchedRuleset = AccountMatchedRuleset.filter(AccountMatchedRuleset.RULESET_TYPE == 'deter')
matched_AccountMatchedRuleset = matched_AccountMatchedRuleset.filter(AccountMatchedRuleset.ROGERS_ECID.isNotNull())
print('matched_AccountMatchedRuleset = ',matched_AccountMatchedRuleset.count())

matched_AccountMatchedRuleset_deterministic_distinct = matched_AccountMatchedRuleset.select('ROGERS_ECID','ROGERS_ACCOUNT_ID','SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID').distinct()
print('matched_AccountMatchedRuleset_deterministic_distinct = ',matched_AccountMatchedRuleset_deterministic_distinct.count())

matched_AccountMatchedRuleset_deterministic_distinct_pair = matched_AccountMatchedRuleset.select('ROGERS_ECID','SHAW_MASTER_PARTY_ID').distinct()
print('matched_AccountMatchedRuleset_deterministic_distinct_pair = ',matched_AccountMatchedRuleset_deterministic_distinct_pair.count())

# COMMAND ----------

#All deterministic records 
matched_AccountMatchedRuleset_lvl = matched_AccountMatchedRuleset.withColumn('ads_lvl', when(matched_AccountMatchedRuleset.RULESET_ID > 29, 1).otherwise(0))
matched_AccountMatchedRuleset_lvl = matched_AccountMatchedRuleset_lvl.withColumn('cust_lvl', when(matched_AccountMatchedRuleset.RULESET_ID < 16, 1).otherwise(0))
matched_AccountMatchedRuleset_lvl.toPandas()

# COMMAND ----------

matched_AccountMatchedRuleset_lvl_1 = matched_AccountMatchedRuleset_lvl.select('ROGERS_ECID','ROGERS_ACCOUNT_ID','SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID','ROGERS_ACCOUNT_TYPE','SHAW_ACCOUNT_TYPE','RULESET_ID','RULESET_ATTR','ads_lvl','cust_lvl')
matched_AccountMatchedRuleset_lvl_1.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Account type

# COMMAND ----------

# matched_AccountMatchedRuleset_lvl_1.filter((col('ROGERS_ACCOUNT_TYPE') == col('SHAW_ACCOUNT_TYPE'))).groupBy('ROGERS_ECID','ROGERS_ACCOUNT_ID','SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID').agg(sum('ads_lvl').alias('sum_a').toPands()

matched_AccountMatchedRuleset_lvl_1_copy = matched_AccountMatchedRuleset_lvl_1
matched_AccountMatchedRuleset_lvl_1_copy = matched_AccountMatchedRuleset_lvl_1_copy.withColumn('rogers_type',when(matched_AccountMatchedRuleset_lvl_1_copy.ROGERS_ACCOUNT_TYPE == 'wireless', 0).otherwise(1))
matched_AccountMatchedRuleset_lvl_1_copy = matched_AccountMatchedRuleset_lvl_1_copy.withColumn('shaw_type',when(matched_AccountMatchedRuleset_lvl_1_copy.SHAW_ACCOUNT_TYPE == 'wireless', 0).otherwise(1))
matched_AccountMatchedRuleset_lvl_1_copy.toPandas()

# COMMAND ----------

matched_AccountMatchedRuleset_lvl_1_copy = matched_AccountMatchedRuleset_lvl_1_copy.withColumn('sum_type',matched_AccountMatchedRuleset_lvl_1_copy.rogers_type+matched_AccountMatchedRuleset_lvl_1_copy.shaw_type)
matched_AccountMatchedRuleset_lvl_1_copy.toPandas()

# COMMAND ----------

matched_AccountMatchedRuleset_lvl_1_copy_1 = matched_AccountMatchedRuleset_lvl_1_copy.groupBy('ROGERS_ECID','ROGERS_ACCOUNT_ID','SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID','rogers_type','shaw_type').count().sort('count', ascending=False)
matched_AccountMatchedRuleset_lvl_1_copy_1.toPandas()

# COMMAND ----------

matched_AccountMatchedRuleset_lvl_1_copy_1.agg(sum('rogers_type').alias('sum_rogers_wireline'), sum('shaw_type').alias('sum_shaw_wireline')).sort('sum_rogers_wireline','sum_shaw_wireline', ascending=False).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #####RULESET_ID

# COMMAND ----------

#Distinct account Customer/Address level classification
df_matched_deter_distinct = matched_AccountMatchedRuleset_lvl.groupBy('ROGERS_ECID','ROGERS_ACCOUNT_ID','SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID').agg(sum('ads_lvl').alias('sum_a'), sum('cust_lvl').alias('sum_c')).sort('sum_a','sum_c', ascending=False)
df_matched_deter_distinct.toPandas()

# COMMAND ----------

#Add flags to Customer and Address level accounts
df_matched_deter_distinct_copy = df_matched_deter_distinct
df_matched_deter_distinct_copy = df_matched_deter_distinct_copy.withColumn('address_lvl', when(df_matched_deter_distinct.sum_a != 0, 'Yes').otherwise('No'))
df_matched_deter_distinct_copy = df_matched_deter_distinct_copy.withColumn('customer_lvl', when(df_matched_deter_distinct.sum_c != 0, 'Yes').otherwise('No'))
df_matched_deter_distinct_copy.toPandas()

# COMMAND ----------

#Count Customer and Address level accounts
count_ads_lvl_only = df_matched_deter_distinct_copy.filter((col('address_lvl') == 'Yes') & ((col('customer_lvl') == 'No'))).count()
print('count_ads_lvl_only = ', count_ads_lvl_only)
count_cus_lvl_only = df_matched_deter_distinct_copy.filter((col('customer_lvl') == 'Yes') & ((col('address_lvl') == 'No'))).count()
print('count_cus_lvl_only = ', count_cus_lvl_only)
count_ads_cus_both = df_matched_deter_distinct_copy.filter((col('address_lvl') == 'Yes') & ((col('customer_lvl') == 'Yes'))).count()
print('count_ads_cus_both = ', count_ads_cus_both)

# COMMAND ----------

matched_AccountMatchedRuleset.groupBy('RULESET_ID').count().sort('RULESET_ID').toPandas()

# COMMAND ----------

matched_AccountMatchedRuleset.groupBy('RULESET_ID','ROGERS_ACCOUNT_TYPE','SHAW_ACCOUNT_TYPE').count().sort('RULESET_ID').toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ##TC-C-05 RULESET_ID roll up

# COMMAND ----------

#Matched_Entity 'DET_MATCH_IND' is 'YES'
df_MatchedEntity_pair_deter = MatchedEntity.filter(col('DET_MATCH_IND') == 'YES')
df_MatchedEntity_pair_deter.toPandas()

# COMMAND ----------

df_MatchedEntity_pair_deter = df_MatchedEntity_pair_deter.select('ROGERS_ECID','SHAW_MASTER_PARTY_ID','DET_MATCH_IND','BEST_DET_MATCH_RULESET_ID')
df_MatchedEntity_pair_deter.toPandas()

# COMMAND ----------

matched_AccountMatchedRuleset_lvl_min_rulesetid = matched_AccountMatchedRuleset.select('ROGERS_ECID','ROGERS_ACCOUNT_ID','SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID','ROGERS_ACCOUNT_TYPE','SHAW_ACCOUNT_TYPE','RULESET_ID','RULESET_ATTR')


# COMMAND ----------

matched_AccountMatchedRuleset_lvl_min_rulesetid = matched_AccountMatchedRuleset_lvl_min_rulesetid.withColumn('RULESET_ID',matched_AccountMatchedRuleset_lvl_min_rulesetid.RULESET_ID.cast('integer'))
matched_AccountMatchedRuleset_lvl_min_rulesetid = matched_AccountMatchedRuleset_lvl_min_rulesetid.withColumn('RULESET_ATTR',matched_AccountMatchedRuleset_lvl_min_rulesetid.RULESET_ATTR.cast('integer'))
matched_AccountMatchedRuleset_lvl_min_rulesetid.toPandas()

# COMMAND ----------

#check if the min ruleset_id is selected
df_amr_id = matched_AccountMatchedRuleset_lvl_min_rulesetid.groupBy('ROGERS_ECID', 'SHAW_MASTER_PARTY_ID').agg(min('RULESET_ID').alias('min_ruleset_id'), min('RULESET_ATTR').alias('min_ruleset_attr'))
df_amr_id.toPandas()

# COMMAND ----------

df_rulesetid_joined = df_MatchedEntity_pair_deter.join(df_amr_id, ['ROGERS_ECID', 'SHAW_MASTER_PARTY_ID'])
df_rulesetid_joined.toPandas()

# COMMAND ----------

matched_AccountMatchedRuleset.filter( (col('ROGERS_ECID')=='78367287') & (col('SHAW_MASTER_PARTY_ID')=='837494')).toPandas()

# COMMAND ----------

df_rulesetid_joined.filter(col('BEST_DET_MATCH_RULESET_ID') != col('min_ruleset_id')).count()

# COMMAND ----------

df_rulesetid_joined.filter(col('BEST_DET_MATCH_RULESET_ID') != col('min_ruleset_attr')).count()

# COMMAND ----------

#Example
df_matchedentity_exp = matched_AccountMatchedRuleset.select('ROGERS_ECID','ROGERS_ACCOUNT_ID','ROGERS_ACCOUNT_TYPE','SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID','SHAW_ACCOUNT_TYPE','RULESET_ID','RULESET_TYPE','RULESET_ATTR').filter((col('ROGERS_ECID')=='409718068') & (col('SHAW_MASTER_PARTY_ID')=='14576361'))
df_matchedentity_exp.groupBy('ROGERS_ECID','ROGERS_ACCOUNT_ID','SHAW_MASTER_PARTY_ID','SHAW_ACCOUNT_ID').agg(sum(when(col('ROGERS_ACCOUNT_TYPE') != col('SHAW_ACCOUNT_TYPE'),1))).toPandas()
