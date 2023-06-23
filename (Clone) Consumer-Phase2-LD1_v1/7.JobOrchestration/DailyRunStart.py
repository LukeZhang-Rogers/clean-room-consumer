# Databricks notebook source
# MAGIC %md ## Daily Run Orchestration Start Job 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC This notebook performs the initial orchestration steps that need to take place before a scheduled run. It confirms that all expected files have been uploaded, deletes any interim data/folders created from the last run, creates a copy of the initial datasets to be used in the run and deletes the initial datsets from the landing data share so that they are not overwritten by the next day's upload
# MAGIC
# MAGIC
# MAGIC ### Requirements:
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC   <li>The raw data should be available in the storage layer</li>
# MAGIC
# MAGIC </ul>

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

from pathlib import Path
from email.mime.base import MIMEBase
from email.utils import COMMASPACE, formatdate
from email import encoders


import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import io

# COMMAND ----------

# DBTITLE 1,UDF for sending mail notification
def send_mail(send_from="dmt-azure-noreply@rci.rogers.com", send_to="indranil.chaudhury@rci.rogers.ca,shalini.palakurthi@rci.rogers.com,bindupriya.darshini@rci.rogers.com", subject = "Cleanroom - Consumers : Missing Source files and folders", message = "Cleanroom Consumer Input files and folders are not generated in ADLS yet", server='10.9.40.62'):
    
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = send_to #COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    
    msg.attach(MIMEText(message))

    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to.split(','), msg.as_string())
    smtp.quit()

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

# MAGIC %md ### Checking Files in Derived Layer

# COMMAND ----------

# DBTITLE 1,Derived ADLS location for Rogers and Shaw
derived_landing_path = 'dbfs:/mnt/derived/ConsumerInput/'

# COMMAND ----------

# DBTITLE 1,Check if folders and files are present in Derived location
try:
    if not path_exists(derived_landing_path):
        print("Folder does not exist")
        send_mail()
        raise Exception("Folder does not exist")
    elif len(dbutils.fs.ls(derived_landing_path)) == 0: 
      print ("Empty Folder")
      send_mail()
      raise Exception("Folder is empty")
    else:
       print("Folder present")
except Exception as e:
    raise

# COMMAND ----------

# DBTITLE 1,Verify if all expected folders are created for Cleanroom Consumer process to start
#Define the derived data share locations for Rogers and Shaw
# derived_landing_path = 'dbfs:/mnt/derived/ConsumerInput/'

folderListRogers = dbutils.fs.ls(derived_landing_path)

#Define the list of all files expected to be provided each day
expectedFiles = ["shaw_consumer_contact", "shaw_consumer_wireline_account", "shaw_consumer_wireless_account", "shaw_consumer_wireless_service", "rogers_contact", "rogers_consumer_wireline_account", "rogers_consumer_wireless_accounts", "rogers_consumer_wireless_service", "wireless_serviceability", "geographic_address", "wireline_serviceability", "service_qualification", "service_qualification_item"]

#The total number of files expected each day
expectedFilesCount = 13

sentFilesColumns = ["filename", "modificationtime", "num_rows"]
sentFilesData = []

#Loop through all of the files/folders in the Rogers inbound data share and put the file name and last modification timestamp into an array
for item1 in folderListRogers:
    filename1 = item1.name.replace('/', '')
    timestamp1 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(item1.modificationTime / 1000))
    try:
        num_rows = spark.read.format('delta').load(item1.path).count()
    except:
        num_rows = None
    item1data = (filename1, timestamp1, num_rows)
    sentFilesData.append(item1data)

#Loop through all of the files/folders in the Shaw inbound data share and put the file name and last modification timestamp into an array
# for item2 in folderListShaw:
#     filename2 = item2.name.replace('/', '')
#     timestamp2 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(item2.modificationTime / 1000))
#     item2data = (filename2, timestamp2)
#     sentFilesData.append(item2data)

#Create a DataFrame containing the filename and modification time for all files/folders in the Shaw and Rogers inbound data shares
sentFilesRdd = spark.sparkContext.parallelize(sentFilesData)
sentFilesRdd_df = spark.createDataFrame(sentFilesRdd).toDF(*sentFilesColumns)

#Loop through the list of the expected files/folders and check whether it was found. It is found increment counter otherwise add the missing filename to a string
expectedFilesFoundCount = 0
missingFiles = ""
for file in expectedFiles:
    sentFilesCompare_df = sentFilesRdd_df.filter(col('filename') == lit(file)).filter(col('num_rows') > 0)
    if sentFilesCompare_df.count() == 0:
        if missingFiles == "":
            missingFiles = file
        else:
            missingFiles = missingFiles + ", " + file
    else:
        expectedFilesFoundCount += sentFilesCompare_df.count()


#Check that all of the expected files/folders were found and if not fail the notebook and job with an error message saying what is missing
if expectedFilesFoundCount != expectedFilesCount:
    send_mail()
    raise Exception("Missing Input Files: "+missingFiles)

# COMMAND ----------

# MAGIC %md ### Clean Up Staging Locations

# COMMAND ----------

# Delete everything from InterimOutput
interimOutputPath = "dbfs:/mnt/derived/Processed"

if path_exists(interimOutputPath):
    interimOutputFolders = dbutils.fs.ls(interimOutputPath)
    for folder in interimOutputFolders:
        dbutils.fs.rm(folder.path, True)

# COMMAND ----------

# -- This command line removed as RawStaging is pointing to source ADLS paths

# Delete everything from RawStaging
# rawStagingPath = "/mnt/development/RawStaging"

# if path_exists(rawStagingPath):
#     rawStagingFolders = dbutils.fs.ls(rawStagingPath)
#     for folder in rawStagingFolders:
#         dbutils.fs.rm(folder.path, True)

# COMMAND ----------

# Delete everything from OutputStaging
outputStagingPath = "dbfs:/mnt/derived/OutputStaging/"

if path_exists(outputStagingPath):
    outputStagingFolders = dbutils.fs.ls(outputStagingPath)
    for folder in outputStagingFolders:
        dbutils.fs.rm(folder.path, True)

# COMMAND ----------

# Delete everything from consumption layer
consumptionPath = "dbfs:/mnt/derived/Consumption/"

if path_exists(consumptionPath):
    consumptionFolders = dbutils.fs.ls(consumptionPath)
    for folder in consumptionFolders:
        dbutils.fs.rm(folder.path, True)

# COMMAND ----------

# MAGIC %md ### Copy Data to Raw Staging

# COMMAND ----------

# -- This command line removed as RawStaging is pointing to source ADLS paths and we do not need the Landing locations

#Get list of datasets that have been sent and backup each dataset in a raw staging location during processing
# rogers_raw_staging_path = '/mnt/development/RawStaging/Rogers/Inbound/'
# shaw_raw_staging_path   = '/mnt/development/RawStaging/Shaw/Inbound/'

# for item1 in folderListRogers:
#     filename1 = item1.name.replace('/', '')
#     newLocation1 = rogers_raw_staging_path + filename1
#     if filename1 in expectedFiles:
#         dbutils.fs.cp(item1.path, newLocation1, True)

# for item2 in folderListShaw:
#     filename2 = item2.name.replace('/', '')
#     newLocation2 = shaw_raw_staging_path + filename2
#     if filename2 in expectedFiles:
#         dbutils.fs.cp(item2.path, newLocation2, True)
