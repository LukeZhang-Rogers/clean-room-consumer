# Databricks notebook source
# MAGIC %md ## Cluster Mount
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC
# MAGIC This notebook can be used to mount the required ADLS paths to Databricks mount points during initial set up. It is to be run only once during environment set up.

# COMMAND ----------

# Replace the parameters with values specific to your environment
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope=<Key_Vault_Scope>,key="ServicePrincipalClientID"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope=<Key_Vault_Scope>,key="ServicePrincipalClientSecret"),
          "fs.azure.account.oauth2.client.endpoint": <auth_token>}

dbutils.fs.mount(
  source = "abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/",
  mount_point = <mount path>,
  extra_configs = configs)

# COMMAND ----------

# Example
# Mounting Shaw container
# configs = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="kvscope",key="SPClientID"),
#           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="kvscope",key="SPClientSecret"),
#           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/9d176b3c-7284-488b-87a1-b6f04c15ea73/oauth2/token"}

#  

# # Optionally, you can add <directory-name> to the source URI of your mount point.
# dbutils.fs.mount(
#   source = "abfss://shaw-snflk-con-01@shawsaprod.dfs.core.windows.net/",
#   mount_point = "/mnt/shawsaprod",
#   extra_configs = configs)
