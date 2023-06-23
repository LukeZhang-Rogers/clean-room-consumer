# Databricks notebook source
# MAGIC %md ## Daily Run Orchestration End Job 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC This notebook performs the final orchestration steps that need to take place at the end of a scheduled run. It confirms that all expected output files have been created in the OutputStaging location and copies them over to the Outbound data share for Rogers/Shaw. It will only copy things if all of the output files are there.
# MAGIC
# MAGIC ### Requirements:
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC   <li>The notebooks have all completed successfully</li>
# MAGIC   <li>The output data should be available in the OutputStaging storage layer</li>
# MAGIC
# MAGIC </ul>

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# COMMAND ----------

# MAGIC %run ../2.Common/ADLSDesignedPaths

# COMMAND ----------

#Create a function check whether the provided path/file exists
def path_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

# MAGIC %md ###Check for Missing Files & Move Data to Consumption

# COMMAND ----------

#Create a list with the OutputStaging paths of all expected output files/consolidated input files for the UCA and CustomerMarketing output models
expectedFiles = [account_matched_ruleset, matched_entity, serviceability_matched, customer_marketing_op, ruleset, unioned_contact, unioned_wireless_acc, unioned_wireline_acc, unioned_wireless_ser, rogers_geo_serviceability, rogers_ser_qualification, rogers_ser_qualification_item, rogers_wls_serviceability, shaw_wln_serviceability]

#Create a list with the final Outbound paths of all expected output files/consolidated input files for the UCA and CustomerMarketing output models
outputFilesFinal = [account_matched_ruleset_final, matched_entity_final, serviceability_matched_final, customer_marketing_final, ruleset_final, unioned_contact_final, unioned_wireless_acc_final, unioned_wireline_acc_final, unioned_wireless_ser_final, rogers_geo_serviceability_final, rogers_service_qualification_final, rogers_service_qualification_item_final, rogers_wls_serviceability_final, shaw_wln_serviceability_final]

#Loop through each of the expected OutputStaging paths, check that they exist. If they exist increment the counter otherwise add the missing path to a string.
expectedFilesCount = 14
expectedFilesFoundCount = 0
missingFiles = ""

for file in expectedFiles:
    if path_exists(file):
        expectedFilesFoundCount += 1
    else:
        if missingFiles == "":
            missingFiles = file + '\n'
        else:
            missingFiles = missingFiles + file + '\n'
            
#Confirm that all paths were found in the OutputStaging location and if they are, clear the outbound/consumption layer and copy all data to Outbound location
if expectedFilesFoundCount < expectedFilesCount:
    raise Exception("Missing Output Files: "+missingFiles)
else:
    for tempoutputfile, outputfile in zip(expectedFiles, outputFilesFinal):
        #dbutils.fs.rm(outputFilesFinal, True)
        dbutils.fs.cp(tempoutputfile, outputfile, True)

# COMMAND ----------

# # Delete everything from Shaw inbound layer
#ShawInboundPath = "/mnt/shawinbound/shaw-consumer-input"

#if path_exists(ShawInboundPath):
#    ShawInputFolders = dbutils.fs.ls(ShawInboundPath)
#    for folder in ShawInputFolders:
#       dbutils.fs.rm(folder.path, True)
# # # Delete everything from Rogers inbound layer
# RogersInboundPath = "/mnt/rogersinbound/rogers-consumer-input"

# if path_exists(RogersInboundPath):
#     RogersInputFolders = dbutils.fs.ls(RogersInboundPath)
#     for folder in ShawInputFolders:
#         dbutils.fs.rm(folder.path, True)

# COMMAND ----------

# DBTITLE 1,write data to delta table
tables = [
    {
        "parquet_df": matched_entity_final,
        "table": "drvd__cr_output_cons.ConsumerMatchedEntity"
    },
    {
        "parquet_df": account_matched_ruleset_final,
        "table": "drvd__cr_output_cons.ConsumerAccountMatchedRuleset"
    },
    {
        "parquet_df": serviceability_matched_final,
        "table": "drvd__cr_output_cons.ServiceabilityMatchedAccounts"
    },
    {
        "parquet_df": unioned_wireless_acc_final,
        "table": "drvd__cr_output_cons.ConsumerWirelessAccount"
    },
    {
        "parquet_df": unioned_wireless_ser_final,
        "table": "drvd__cr_output_cons.ConsumerWirelessServices"
    },
    {
        "parquet_df": unioned_wireline_acc_final,
        "table": "drvd__cr_output_cons.ConsumerWirelineAccount"
    },
    {
        "parquet_df": unioned_contact_final,
        "table": "drvd__cr_output_cons.ConsumerContact"
    },
    {
        "parquet_df": customer_marketing_final,
        "table": "drvd__cr_output_cons.CustomerMarketing"
    },
    {
        "parquet_df": ruleset_final,
        "table": "drvd__cr_output_cons.ConsumerRuleset"
    },
    {
        "parquet_df": rogers_wls_serviceability_final,
        "table": "drvd__cr_output_cons.RogersWirelessServiceability"
    },
    {
        "parquet_df": rogers_geo_serviceability_final,
        "table": "drvd__cr_output_cons.RogersGeographicAddress"
    },
    {
        "parquet_df": rogers_service_qualification_item_final,
        "table": "drvd__cr_output_cons.RogersServiceQualificationItem"
    },
    {
        "parquet_df": rogers_service_qualification_final,
        "table": "drvd__cr_output_cons.RogersServiceQualification"
    },
    {
        "parquet_df": shaw_wln_serviceability_final,
        "table": "drvd__cr_output_cons.ShawWirelineServiceability"
    }
]

# requires Databricks Runtime 12.0 or above
for table in tables:
    df = spark.read.parquet(table['parquet_df']) \
        .withColumn("_az_insert_ts", current_timestamp()) \
        .withColumn("_az_update_ts", current_timestamp())
    
    df.write \
        .format("delta") \
        .mode('overwrite') \
        .option('partitionOverwriteMode', 'dynamic') \
        .saveAsTable(table['table'])

    print(f'updated {df.count()} rows in table {table["table"]}')

# COMMAND ----------

# Create table scripts - For one time only

# COMMAND ----------

#  %sql
#  CREATE SCHEMA if not exists drvd__cr_output_cons
#  LOCATION 'dbfs:/mnt/derived/clean-room/Consumption/';

# COMMAND ----------

# %python

# df = spark.read.parquet('dbfs:/mnt/derived/r4b/Consumption/BusinessAccountMatchedRuleset/')

# df.createOrReplaceTempView('temp')

# spark.sql('''CREATE TABLE drvd__cr_output_r4b.BusinessAccountMatchedRuleset USING DELTA PARTITIONED BY (SNAPSHOT_STAMP) LOCATION 'dbfs:/mnt/derived/clean-room/Consumption/BusinessAccountMatchedRuleset/' AS SELECT *, current_timestamp() AS _az_insert_ts, current_timestamp() AS _az_update_ts FROM temp''')

# df = spark.read.parquet('dbfs:/mnt/derived/r4b/Consumption/BusinessMatchedEntity/')

# df.createOrReplaceTempView('temp')

# spark.sql('''CREATE TABLE drvd__cr_output_r4b.BusinessMatchedEntity USING DELTA PARTITIONED BY (SNAPSHOT_STAMP) LOCATION 'dbfs:/mnt/derived/clean-room/Consumption/BusinessMatchedEntity/' AS SELECT *, current_timestamp() AS _az_insert_ts, current_timestamp() AS _az_update_ts FROM temp''')

# df = spark.read.parquet('dbfs:/mnt/derived/r4b/Consumption/BusinessRuleset/')

# df.createOrReplaceTempView('temp')

# spark.sql('''CREATE TABLE drvd__cr_output_r4b.BusinessRuleset USING DELTA  LOCATION 'dbfs:/mnt/derived/clean-room/Consumption/BusinessRuleset/' AS SELECT *, current_timestamp() AS _az_insert_ts, current_timestamp() AS _az_update_ts FROM temp''')

# df = spark.read.parquet('dbfs:/mnt/derived/r4b/Consumption/CCMM/BusinessAccount/').drop('_az_insert_ts')

# df.createOrReplaceTempView('temp')

# spark.sql('''CREATE TABLE drvd__cr_output_r4b.BusinessAccount USING DELTA PARTITIONED BY (SNAPSHOT_STAMP) LOCATION 'dbfs:/mnt/derived/clean-room/Consumption/BusinessAccount/' AS SELECT *, current_timestamp() AS _az_insert_ts FROM temp''')

# df = spark.read.parquet('dbfs:/mnt/derived/r4b/Consumption/CCMM/BusinessContact/').drop('_az_insert_ts')

# df.createOrReplaceTempView('temp')

# spark.sql('''CREATE TABLE drvd__cr_output_r4b.BusinessContact USING DELTA PARTITIONED BY (SNAPSHOT_STAMP) LOCATION 'dbfs:/mnt/derived/clean-room/Consumption/BusinessContact/' AS SELECT *, current_timestamp() AS _az_insert_ts FROM temp''')

# df = spark.read.parquet('dbfs:/mnt/derived/r4b/Consumption/CCMM/BusinessServiceLocation/').drop('_az_insert_ts')

# df.createOrReplaceTempView('temp')

# spark.sql('''CREATE TABLE drvd__cr_output_r4b.BusinessServiceLocation USING DELTA PARTITIONED BY (SNAPSHOT_STAMP) LOCATION 'dbfs:/mnt/derived/clean-room/Consumption/BusinessServiceLocation/' AS SELECT *, current_timestamp() AS _az_insert_ts FROM temp''')

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/derived/clean-room/Consumption")
