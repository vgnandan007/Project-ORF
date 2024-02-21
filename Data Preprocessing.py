# Databricks notebook source
pip install adal

# COMMAND ----------

import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from datetime import date,datetime, timedelta
import adal
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import trim
from pyspark.sql.types import IntegerType

import numpy as np
import pandas as pd

# COMMAND ----------

spark = SparkSession.builder.appName('sparkdf').getOrCreate()

#Update when changing environments
service_credential = dbutils.secrets.get(scope="loi-dev-db-secretscope",key="loi-dna-dev-spn-secret")
spark.conf.set("fs.azure.account.oauth2.client.id.shell01eunadls2dobdchjqb.dfs.core.windows.net", "ec881d53-a552-4c61-afa4-b4355b9d90dd")
service_principal_id = "ec881d53-a552-4c61-afa4-b4355b9d90dd"

#configure connection to ADLS Gen2
spark.conf.set("fs.azure.account.auth.type.shell01eunadls2dobdchjqb.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.shell01eunadls2dobdchjqb.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.shell01eunadls2dobdchjqb.dfs.core.windows.net",service_principal_id )
spark.conf.set("fs.azure.account.oauth2.client.secret.shell01eunadls2dobdchjqb.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.shell01eunadls2dobdchjqb.dfs.core.windows.net", "https://login.microsoftonline.com/db1e96a8-a3da-442a-930b-235cac24cd5c/oauth2/token")

# COMMAND ----------

#Update when changing environments
###SQL CONNECTION###
azure_sql_url = "jdbc:sqlserver://shell-01-eun-sq-wipoyrayztawpxguwycu.database.windows.net"
snow_database = "shell-01-eun-sqdb-cgljxfpzifaquvuwplpe"
it4it_database = "shell-01-eun-sqdb-xjmqfmltvxsfnjbstdsu"

tenant_id = "db1e96a8-a3da-442a-930b-235cac24cd5c"
resource_app_id_url = "https://database.windows.net/"
authority = "https://login.windows.net/" + tenant_id
encrypt = "true"
host_name_in_certificate = "*.database.windows.net"
context = adal.AuthenticationContext(authority)
token = context.acquire_token_with_client_credentials(resource_app_id_url, service_principal_id, service_credential)
access_token = token["accessToken"]

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

cmdb_app = spark.read \
             .format("com.microsoft.sqlserver.jdbc.spark") \
             .option("url", azure_sql_url) \
             .option("dbtable", 'stg.SNV_cmdb_ci_business_app') \
             .option("databaseName", snow_database) \
             .option("accessToken", access_token) \
             .option("encrypt", "true") \
             .option("hostNameInCertificate", "*.database.windows.net") \
             .load()

cmdb_app = cmdb_app['number','name','dv_portfolio','u_operating_business_unit','dv_u_owning_business','install_status','sys_id']

cmdb_app = cmdb_app.withColumnRenamed('number','Business_Application_Number')
cmdb_app = cmdb_app.withColumnRenamed('name','Business_Application_Name')
cmdb_app = cmdb_app.withColumnRenamed('dv_portfolio','Portfolio')
cmdb_app = cmdb_app.withColumnRenamed('u_operating_business_unit','Operating_SOM')
cmdb_app = cmdb_app.withColumnRenamed('dv_u_owning_business','Owning_Business')
cmdb_app = cmdb_app.withColumnRenamed('install_status','Business_Application_Status')
cmdb_app = cmdb_app.withColumnRenamed('sys_id','ap_sys_id')

cmdb_app = cmdb_app.withColumn("Owning_Business", split(col("Owning_Business"), "/"))
cmdb_app = cmdb_app.withColumn("Owning_Business", col("Owning_Business")[1])
cmdb_app = cmdb_app.withColumn("Owning_Business", trim(col("Owning_Business")))

# COMMAND ----------

cmdb_service = spark.read \
             .format("com.microsoft.sqlserver.jdbc.spark") \
             .option("url", azure_sql_url) \
             .option("dbtable", 'stg.SNV_cmdb_ci_service_discovered') \
             .option("databaseName", snow_database) \
             .option("accessToken", access_token) \
             .option("encrypt", "true") \
             .option("hostNameInCertificate", "*.database.windows.net") \
             .load()

cmdb_service = cmdb_service['number','name','dv_change_control','dv_support_group','dv_business_contact','dv_owned_by','sys_id']

cmdb_service = cmdb_service.withColumnRenamed('number','Deployment_Number')
cmdb_service = cmdb_service.withColumnRenamed('name','Deployment_Name')
cmdb_service = cmdb_service.withColumnRenamed('dv_business_contact','Business_Application_Owner')
cmdb_service = cmdb_service.withColumnRenamed('dv_owned_by','CI_Owner')
cmdb_service = cmdb_service.withColumnRenamed('dv_change_control','Approval_Group')
cmdb_service = cmdb_service.withColumnRenamed('dv_support_group','Support_Group')
cmdb_service = cmdb_service.withColumnRenamed('sys_id','ap_service_sys_id')

# COMMAND ----------

#ORF_onboarded

ORF_ONBOARDED_Path = 'abfss://shell01eunadls2dobdchjqb@shell01eunadls2dobdchjqb.dfs.core.windows.net/RAW/W00039-SHELL-MSDEVOPS-CONFIDENTAL/ORF_Onboarded_Products/ORF_Onboarded_Products.parquet'

ORF_ONBOARDED = spark.read.format("parquet").option("header", "true").option("multiLine", "true").load(ORF_ONBOARDED_Path).select("Business_Application","Deployment_ID","Current_Stage")
ORF_ONBOARDED = ORF_ONBOARDED.withColumn("Current_Stage", trim(upper(col("Current_Stage"))))

# COMMAND ----------

#Map onboarded to application table based on business application
ORF_ONBOARDED = ORF_ONBOARDED.withColumn("Business_Application", trim(upper(col("Business_Application"))))
cmdb_app = cmdb_app.withColumn("Business_Application_Number", trim(upper(col("Business_Application_Number"))))

onboarded_intermediate = ORF_ONBOARDED.join(cmdb_app, ORF_ONBOARDED['Business_Application'] == cmdb_app['Business_Application_Number'], "left")

onboarded_intermediate = onboarded_intermediate.drop('Business_Application_Number')
onboarded_intermediate = onboarded_intermediate.withColumnRenamed('Business_Application','Business_Application_Number')

# COMMAND ----------

#Map with deployment table
onboarded_intermediate = onboarded_intermediate.withColumn("Deployment_ID", trim(upper(col("Deployment_ID"))))
cmdb_service = cmdb_service.withColumn("Deployment_Number", trim(upper(col("Deployment_Number"))))

onboarded_list = onboarded_intermediate.join(cmdb_service, onboarded_intermediate['Deployment_ID'] == cmdb_service['Deployment_Number'], "left")

onboarded_list = onboarded_list.drop('Deployment_Number')
onboarded_list = onboarded_list.withColumnRenamed('Deployment_ID','Deployment_Number')

onboarded_list = onboarded_list['Business_Application_Number','Deployment_Number','Business_Application_Name','Deployment_Name','Portfolio', 'Operating_SOM', 'Business_Application_Owner','CI_Owner','Owning_Business', 'Business_Application_Status',   'Approval_Group', 'Support_Group','Current_Stage','ap_sys_id','ap_service_sys_id']

onboarded_list = onboarded_list.dropDuplicates()
onboarded_list = onboarded_list.na.fill("")
