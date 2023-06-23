# Databricks notebook source
# MAGIC %md On successful completion of Cleanroom Consumer process, this NB will be executed to delete files generated in "/derived/ConsumerInput/" which have been used as source

# COMMAND ----------

# DBTITLE 0,Recursive Delete command
from datetime import datetime

dbutils.fs.rm("dbfs:/mnt/derived/ConsumerInput/", recurse=True)
dbutils.fs.mkdirs("dbfs:/mnt/derived/ConsumerInput/")

ready_file_path = f'/dbfs/mnt/derived/ConsumerInput/ready.json'
# create an empty file
open(ready_file_path, 'w').close()
print(f'ready file created: {ready_file_path}')
