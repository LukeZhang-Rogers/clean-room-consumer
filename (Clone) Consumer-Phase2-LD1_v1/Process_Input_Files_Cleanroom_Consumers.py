# Databricks notebook source
# DBTITLE 1,Install Libraries
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import json
from pyspark.sql.types import DataTypeSingleton

# COMMAND ----------

# DBTITLE 1,remove previous ready.json file
path = "dbfs:/mnt/derived/ConsumerInput/"
file_name = 'ready.json'

result = dbutils.fs.rm(f"{path}/{file_name}")
if result:
    print(f'file removed: {path}/{file_name}')
else:
    print(f'ready file does not exist')

# COMMAND ----------

# DBTITLE 1,Define current date from now()
now = datetime.now()
display(now.strftime("%Y-%m-%d"))
yesterday_date = (now - timedelta(days=1)).strftime('%Y-%m-%d')

# COMMAND ----------

# DBTITLE 1,Define source paths
#LD1/Post-LD1 Paths
shaw_consumer_contact             = 'dbfs:/mnt/rawstd/clean-room/consumer/consumer_contact' #rawstd__shaw_cr_cons.consumer_contact
shaw_consumer_wireline_account    = 'dbfs:/mnt/rawstd/clean-room/consumer/consumer_wireline_account' #rawstd__shaw_cr_cons.consumer_wireline_account
shaw_consumer_wireless_account    = 'dbfs:/mnt/rawstd/clean-room/consumer/consumer_wireless_account' #rawstd__shaw_cr_cons.consumer_wireless_account
shaw_consumer_wireless_service    = 'dbfs:/mnt/rawstd/clean-room/consumer/consumer_wireless_service' #rawstd__shaw_cr_cons.consumer_wireless_service

rogers_contact                    = 'dbfs:/mnt/derived/app_rci_cr_cons/contact' #drvd__app_rci_cr_cons.contact
rogers_wireline_account           = 'dbfs:/mnt/derived/app_rci_cr_cons/consumer_wireline_account' #drvd__app_rci_cr_cons.consumer_wireline_account
rogers_wireless_account           = 'dbfs:/mnt/derived/app_rci_cr_cons/consumer_wireless_accounts' #drvd__app_rci_cr_cons.consumer_wireless_accounts
rogers_wireless_service           = 'dbfs:/mnt/derived/app_rci_cr_cons/consumer_wireless_service' #drvd__app_rci_cr_cons.consumer_wireless_service

rogers_wireless_serviceability    = 'dbfs:/mnt/rawstd/rci_cr_cons/wireless_serviceability' #rawstd__rci_cr_cons.wireless_serviceability
rogers_wireline_serviceability    = 'dbfs:/mnt/rawstd/rci_cr_cons/geographic_address' #rawstd__rci_cr_cons.geographic_address
shaw_wireline_serviceability      = 'dbfs:/mnt/rawstd/clean-room/consumer/wireline_serviceability' #rawstd__shaw_cr_cons.wireline_serviceability
rogers_service_qualification      = "dbfs:/mnt/rawstd/rci_cr_cons/service_qualification" #rawstd__rci_cr_cons.service_qualification
rogers_service_qualification_item = "dbfs:/mnt/rawstd/rci_cr_cons/service_qualification_item" #rawstd__rci_cr_cons.service_qualification_item

# COMMAND ----------

# DBTITLE 1,Define target paths
shaw_consumer_contact_tgt = 'dbfs:/mnt/derived/ConsumerInput/shaw_consumer_contact'#rawstd__shaw_cr_cons.consumer_contact
shaw_consumer_wireline_account_tgt = 'dbfs:/mnt/derived/ConsumerInput/shaw_consumer_wireline_account'#rawstd__shaw_cr_cons.consumer_wireline_account
shaw_consumer_wireless_account_tgt = 'dbfs:/mnt/derived/ConsumerInput/shaw_consumer_wireless_account'#rawstd__shaw_cr_cons.consumer_wireless_account
shaw_consumer_wireless_service_tgt = 'dbfs:/mnt/derived/ConsumerInput/shaw_consumer_wireless_service'#rawstd__shaw_cr_cons.consumer_wireless_service

rogers_contact_tgt = 'dbfs:/mnt/derived/ConsumerInput/rogers_contact'#drvd__app_rci_cr_cons.contact
rogers_wireline_account_tgt = 'dbfs:/mnt/derived/ConsumerInput/rogers_consumer_wireline_account'#drvd__app_rci_cr_cons.consumer_wireline_account
rogers_wireless_account_tgt = 'dbfs:/mnt/derived/ConsumerInput/rogers_consumer_wireless_accounts'#drvd__app_rci_cr_cons.consumer_wireless_accounts
rogers_wireless_service_tgt = 'dbfs:/mnt/derived/ConsumerInput/rogers_consumer_wireless_service'#drvd__app_rci_cr_cons.consumer_wireless_service

rogers_wireless_serviceability_tgt = 'dbfs:/mnt/derived/ConsumerInput/wireless_serviceability'#rawstd__rci_cr_cons.wireless_serviceability
rogers_wireline_serviceability_tgt = 'dbfs:/mnt/derived/ConsumerInput/geographic_address'#rawstd__rci_cr_cons.geographic_address
shaw_wireline_serviceability_tgt = 'dbfs:/mnt/derived/ConsumerInput/wireline_serviceability'#rawstd__shaw_cr_cons.wireline_serviceability
rogers_service_qualification_tgt = 'dbfs:/mnt/derived/ConsumerInput/service_qualification'#rawstd__rci_cr_cons.service_qualification
rogers_service_qualification_item_tgt = 'dbfs:/mnt/derived/ConsumerInput/service_qualification_item'#rawstd__rci_cr_cons.service_qualification_item

# COMMAND ----------

# DBTITLE 1,Read source delta files
shaw_consumer_contact_df = spark.read.format("delta").load(shaw_consumer_contact)
shaw_consumer_wireline_account_df = spark.read.format("delta").load(shaw_consumer_wireline_account)
shaw_consumer_wireless_account_df = spark.read.format("delta").load(shaw_consumer_wireless_account)
shaw_consumer_wireless_service_df = spark.read.format("delta").load(shaw_consumer_wireless_service) 

rogers_contact_df = spark.read.format("delta").load(rogers_contact)
rogers_wireline_account_df = spark.read.format("delta").load(rogers_wireline_account)
rogers_wireless_account_df = spark.read.format("delta").load(rogers_wireless_account)
rogers_wireless_service_df = spark.read.format("delta").load(rogers_wireless_service)

rogers_wireless_serviceability_df = spark.read.format("delta").load(rogers_wireless_serviceability)
rogers_wireline_serviceability_df = spark.read.format("delta").load(rogers_wireline_serviceability)
shaw_wireline_serviceability_df = spark.read.format("delta").load(shaw_wireline_serviceability)
rogers_service_qualification_df = spark.read.format("delta").load(rogers_service_qualification)
rogers_service_qualification_item_df = spark.read.format("delta").load(rogers_service_qualification_item)

# COMMAND ----------

# DBTITLE 1,Extract only the latest snapshot
shaw_consumer_contact_filtered = shaw_consumer_contact_df.filter(col('SNAPSHOT_STAMP') == yesterday_date)
shaw_consumer_wireline_account_filtered = shaw_consumer_wireline_account_df.filter(col('SNAPSHOT_STAMP') == yesterday_date)
shaw_consumer_wireless_account_filtered = shaw_consumer_wireless_account_df.filter(col('SNAPSHOT_STAMP') == yesterday_date)
shaw_consumer_wireless_service_filtered = shaw_consumer_wireless_service_df.filter(col('SNAPSHOT_STAMP') == yesterday_date)

rogers_contact_filtered = rogers_contact_df
rogers_wireline_account_filtered = rogers_wireline_account_df
rogers_wireless_account_filtered = rogers_wireless_account_df
rogers_wireless_service_filtered = rogers_wireless_service_df

rogers_wireless_serviceability_filtered = rogers_wireless_serviceability_df
rogers_wireline_serviceability_filtered = rogers_wireline_serviceability_df
shaw_wireline_serviceability_filtered = shaw_wireline_serviceability_df.filter(col('SNAPSHOT_STAMP') == yesterday_date)
rogers_service_qualification_filtered = rogers_service_qualification_df
rogers_service_qualification_item_filtered = rogers_service_qualification_item_df

# COMMAND ----------

# DBTITLE 1,Write to ADLS as delta format
shaw_consumer_contact_filtered.write.format('delta').mode('overwrite').save(shaw_consumer_contact_tgt)
shaw_consumer_wireline_account_filtered.write.format('delta').mode('overwrite').save(shaw_consumer_wireline_account_tgt)
shaw_consumer_wireless_account_filtered.write.format('delta').mode('overwrite').save(shaw_consumer_wireless_account_tgt)
shaw_consumer_wireless_service_filtered.write.format('delta').mode('overwrite').save(shaw_consumer_wireless_service_tgt)

rogers_contact_filtered.write.format('delta').mode('overwrite').save(rogers_contact_tgt)
rogers_wireline_account_filtered.write.format('delta').mode('overwrite').save(rogers_wireline_account_tgt)
rogers_wireless_account_filtered.write.format('delta').mode('overwrite').save(rogers_wireless_account_tgt)
rogers_wireless_service_filtered.write.format('delta').mode('overwrite').save(rogers_wireless_service_tgt)

rogers_wireless_serviceability_filtered.write.format('delta').mode('overwrite').save(rogers_wireless_serviceability_tgt)
rogers_wireline_serviceability_filtered.write.format('delta').mode('overwrite').save(rogers_wireline_serviceability_tgt)
shaw_wireline_serviceability_filtered.write.format('delta').mode('overwrite').save(shaw_wireline_serviceability_tgt)
rogers_service_qualification_filtered.write.format('delta').mode('overwrite').save(rogers_service_qualification_tgt)
rogers_service_qualification_item_filtered.write.format('delta').mode('overwrite').save(rogers_service_qualification_item_tgt)

# COMMAND ----------

dbutils.fs.ls('dbfs:/mnt/derived/ConsumerInput/')

# COMMAND ----------

shaw_consumer_contact_filtered.count()

# COMMAND ----------

shaw_consumer_contact_filtered.select('SNAPSHOT_STAMP').distinct().collect()

# COMMAND ----------

shaw_consumer_wireline_account_filtered.count()

# COMMAND ----------

shaw_consumer_wireline_account_filtered.select('SNAPSHOT_STAMP').distinct().collect()

# COMMAND ----------

shaw_consumer_wireless_account_filtered.count()

# COMMAND ----------

shaw_consumer_wireless_account_filtered.select('SNAPSHOT_STAMP').distinct().collect()

# COMMAND ----------

shaw_consumer_wireless_service_filtered.count()

# COMMAND ----------

shaw_consumer_wireless_service_filtered.select('SNAPSHOT_STAMP').distinct().collect()

# COMMAND ----------

rogers_contact_filtered.count()

# COMMAND ----------

rogers_contact_filtered.select('SNAPSHOT_STAMP').distinct().collect()

# COMMAND ----------

rogers_wireline_account_filtered.count()

# COMMAND ----------

rogers_wireline_account_filtered.select('SNAPSHOT_STAMP').distinct().collect()

# COMMAND ----------

rogers_wireless_account_filtered.count()

# COMMAND ----------

rogers_wireless_account_filtered.select('SNAPSHOT_STAMP').distinct().collect()

# COMMAND ----------

rogers_wireless_service_filtered.count()

# COMMAND ----------

rogers_wireless_service_filtered.select('SNAPSHOT_STAMP').distinct().collect()

# COMMAND ----------

rogers_wireless_serviceability_filtered.count()

# COMMAND ----------

rogers_wireline_serviceability_filtered.count()

# COMMAND ----------

shaw_wireline_serviceability_filtered.count()

# COMMAND ----------

shaw_wireline_serviceability_filtered.select('SNAPSHOT_STAMP').distinct().collect()

# COMMAND ----------

rogers_service_qualification_filtered.count()

# COMMAND ----------

rogers_service_qualification_item_filtered.count()

# COMMAND ----------

# DBTITLE 1,Message after successful execution
name = "Successful execution of 500 Databricks Pipeline to load Consumer Input Files Derived Layer"
json_output_dict = {
"name": name,
"messageDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}
dbutils.notebook.exit(json.dumps(json_output_dict, indent=1))

# COMMAND ----------


df=spark.read.format("parquet").load("dbfs:/mnt/derived/Consumption/CustomerMarketing/")
display(df)

# COMMAND ----------

df=spark.read.format("parquet").load("/mnt/raw/cr-consumer-output/prod-consumption/customermarketing/2023/06/22/")
display(df)
