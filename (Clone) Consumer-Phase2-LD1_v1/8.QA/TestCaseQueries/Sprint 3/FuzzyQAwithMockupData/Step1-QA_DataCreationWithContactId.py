# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### address for saving cleaned files

# COMMAND ----------

#################### input Path #################
mockup_path = "/mnt/development/InterimOutput/Fuzzy/QAMockData/Sprint3/Input"


shaw_contact             = mockup_path + '/shaw_contacts'
shaw_wireline_account    = mockup_path + '/shaw_wireline_accounts'
shaw_wireless_account    = mockup_path + '/shaw_wireless_accounts'

rogers_contact                    = mockup_path + '/rogers_contact'
rogers_wireline_account           = mockup_path + '/rogers_wireline_account'
rogers_wireless_account           = mockup_path + '/rogers_wireless_account'


# COMMAND ----------

# MAGIC %md
# MAGIC ### Execution

# COMMAND ----------

# Create schema for Rogers contact
schema_rogers_contact = StructType([StructField("contact_id", StringType(), True)
                         ,StructField("x_rcis_id", StringType(), True)\
                       ,StructField("fa_id", StringType(), True)\
                       ,StructField("first_name", StringType(), True)\
                        ,StructField("last_name", StringType(), True)\
                       ,StructField("e_mail", StringType(), True)\
                        ,StructField("primary_phone", StringType(), True)\
                     ,StructField("alternate_phone_1", StringType(), True)\
                       ,StructField("alternate_phone_2", StringType(), True)\
                       ,StructField("address", StringType(), True)\
                       ,StructField("city", StringType(), True)\
                       ,StructField("state", StringType(), True)\
                        ,StructField("zipcode", StringType(), True)])

# COMMAND ----------

# Create schema for Rogers wireline
schema_rogers_wireline = StructType([StructField("contact_id", StringType(), True)
                        ,StructField("x_rcis_id", StringType(), True)\
                       ,StructField("fa_id", StringType(), True)\
                       ,StructField("service_address", StringType(), True)\
                       ,StructField("service_street_name", StringType(), True)\
                       ,StructField('service_street_type', StringType(), True)\
                       ,StructField('service_street_direction', StringType(), True)\
                       ,StructField('service_quadrant', StringType(), True)\
                        ,StructField("service_city", StringType(), True)\
                       ,StructField("service_province", StringType(), True)\
                        ,StructField("service_postal_code", StringType(), True)
                                    ])

# COMMAND ----------

# Create schema for Rogers wireless
schema_rogers_wireless = StructType([StructField("x_rcis_id", StringType(), True)\
                       ,StructField("fa_id", StringType(), True)])

# COMMAND ----------

# Create schema for Shaw contact
schema_shaw_contact = StructType([StructField("contact_id", StringType(), True)
                        ,StructField("rcis_id", StringType(), True)\
                       ,StructField("fa_id", StringType(), True)\
                       ,StructField("first_name", StringType(), True)\
                        ,StructField("last_name", StringType(), True)\
                       ,StructField("e_mail", StringType(), True)\
                        ,StructField("primary_phone", StringType(), True)\
                     ,StructField("alternate_phone_1", StringType(), True)\
                       ,StructField("alternate_phone_2", StringType(), True)\
                       ,StructField("address", StringType(), True)\
                       ,StructField("city", StringType(), True)\
                       ,StructField("state", StringType(), True)\
                        ,StructField("zipcode", StringType(), True)])

# COMMAND ----------

# Create schema for Shaw wireline
schema_shaw_wireline = StructType([StructField("contact_id", StringType(), True)
                        ,StructField("rcis_id", StringType(), True)\
                       ,StructField("fa_id", StringType(), True)\
                       ,StructField("service_address", StringType(), True)\
                       ,StructField("service_street_name", StringType(), True)\
                       ,StructField('service_street_type', StringType(), True)\
                       ,StructField('service_street_direction', StringType(), True)\
                       ,StructField('service_quadrant', StringType(), True)\
                        ,StructField("service_city", StringType(), True)\
                       ,StructField("service_province", StringType(), True)\
                        ,StructField("service_postal_code", StringType(), True)
                                  ])

# COMMAND ----------

# Create schema for Shaw wireless
schema_shaw_wireless = StructType([StructField("rcis_id", StringType(), True)\
                       ,StructField("fa_id", StringType(), True)])

# COMMAND ----------

# Create the Rogers contact mock data
r_row = Row('contact_id', 'fa_id', 'x_rcis_id', 'first_name', 'last_name', 'e_mail', 'primary_phone', 'alternate_phone_1', 'alternate_phone_2', 'address', 'city', 'state', 'zipcode')

r1 = r_row(1001,11111100, 100000, 'Nickolas', 'kennedy', 'nick.smith@yahoo.com', 4168720956, None, None, 'BOX 446 STN F, Kitchener, ON L4J 538', 'Unknown', 'Unknown', 'Unknown')
r2 = r_row(1002,11111101, 100001, 'Alex', 'blank', 'alex.blan@gmail.com', None, None, None, 'SITE 888 PO BOX 90 RR 800 new branch', 'Unknown', 'Unknown', 'Unknown')
r3 = r_row(1003,11111102, 100002, 'Richard', 'Ågård', None, 4185676585, None, None, 'Unknown', 'Unknown', 'Unknown', 'Unknown')
r4 = r_row(1004,11111103, 100003, 'Renée', "blake's", 'blake_ma@yahoo.com', 5198528318, None, None,'unknown', 'Unknown', 'Unknown', 'Unknown')
r5 = r_row(1005,11111104, 100004, 'Sport & aquafitness', ' thornhill', 'info@thornhillsportsaqua.com', None, None, None, 'suite 5000 780 Strathearn St, Toronto, ON', 'Unknown', 'Unknown', 'Unknown')
r6 = r_row(1006,11111105, 100005, 'Charli', 'underson', None, 4165247622, None, None, 'Unknown', 'Unknown', 'Unknown', 'Unknown')
r7 = r_row(1007,11111106, 100006, 'sadie', 'hanidi', 'sa.hanidi@mail.com', None, None, None, '754 North St RR 56 NHG MAIN Maastricht AB V6Y 6J6', 'Unknown', 'Unknown', 'Unknown')
r8 = r_row(1008,11111107, 100007, 'katelyn', 'blak', 'kate_blak@hotmail.com', 4164651207, None, None, 'Research institute, 95 734 Avenue St W PO BOX 160004 STN C Vancouver BC V99 3894', 'Unknown', 'Unknown', 'Unknown')
r9 = r_row(1009,11111108, 100008, 'rian', 'sanita ', 'ryan.san@outlook,com', 4035670897, 9999999999, None, 'Unknown', 'Unknown', 'Unknown', 'Unknown')




rogers_mockup_test_data = spark.createDataFrame([r1,r2,r3,r4,r5,r6,r7,r8,r9],schema=schema_rogers_contact)


# COMMAND ----------

# Create the Shaw contact mock data

s_row = Row('contact_id','fa_id', 'rcis_id', 'first_name', 'last_name', 'e_mail', 'primary_phone', 'alternate_phone_1', 'alternate_phone_2', 'address', 'city', 'state', 'zipcode')

s1 = s_row(101,20020020000, 200000, 'Nick', 'kenedy', 'nick_smith@gmail.com', 2048153638, 4168720956, None, 'BOX 3002 STN C', 'OTTAWA', 'ON', 'K1Y4J3')
s2 = s_row(102,20020020001, 200001, 'Alexander', 'blank', 'alex.blank+shaw@gmail.com', None, None, None, "Data Department 27 1425 Python St E PO BOX 4001 STN A", 'Victoria', 'BC', 'V8X 3X4')
s3 = s_row(103,20020020002, 200002, 'Dick', 'Agaard', None, 4185676584, None, None, '462 Cedar St RR 4 LCD MAIN', 'LLOYDMINSTER', 'AB', ' T9V 2Z9')
s4 = s_row(104,20020020003, 200003, 'blake', 'Rene', 'blake-ma@yahoo.com', 5198528318, None, None, 'Suite 9001 50 Charles St E', 'Toronto', 'ON', 'M4Y 0B9')
s5 = s_row(105,20020020004, 200004, 'COMP', 'Sport and aqua-fitness thornhill', 'contact@thornhillsportsaqua.com', None, None, None, 'Unknown', 'Unknown', 'Unknown', 'Unknown')
s6 = s_row(106,20020020005, 200005, 'Unknown', 'Charlie Anderson', None, 2049465942, 4165247622, None, 'Unknown', 'Unknown', 'Unknown', 'Unknown')
s7 = s_row(107,20020020006, 200006, 'saieidé', 'hanifi', 'sai.hanidi@gmail.com', None, None, None, 'Unknown', 'Unknown', 'Unknown', 'Unknown')
s8 = s_row(108,20020020007, 200007, 'ryan', 'santoni ', 'no-reply@nodomain.com', 9999999999, None, None, 'Unknown', 'Unknown', 'Unknown', 'Unknown')
s9 = s_row(109,11111108, 100008, 'kathy', 'black ', 'kate.black@outlook.com', 4164651200, 9999999999, None, 'SITE 308 BOX 15 RR 3 LCD MAIN', 'London', 'Unknown', 'Unknown')




shaw_mockup_test_data = spark.createDataFrame([s1,s2,s3,s4,s5,s6,s7,s8,s9],schema=schema_shaw_contact)

# COMMAND ----------

# Generate Rogers Wireline data (with city, state and postal code in address)
rw_row = Row('contact_id','x_rcis_id', 'fa_id', 'service_address', 'service_street_name', 'service_street_type', 'service_street_direction', 'service_quadrant', 'service_city', 'service_province', 'service_postal_code')
rw1 = rw_row(1001,100000,11111100,'BOX 3002 STN C, OTTAWA, ON K1Y4J3','Unknown','Unknown','Unknown','Unknown','OTTAWA','ON','K1Y 4J3',)
rw2 = rw_row(1002,100001,11111101,"Data Department 27 1425 Python St E PO BOX 4001 STN A Victoria BC V8X 3X4",'Unknown','Unknown','Unknown','Unknown','Victoria','BC','V8X 3X4')
rw3 = rw_row(1003,100002,11111102,'462 Cedar St RR 4 LCD MAIN LLOYDMINSTER AB T9V 2Z9','Unknown','Unknown','Unknown','Unknown','LLOYDMINSTER','AB','M5G 2R3')
rw4 = rw_row(1004,100003,11111103,'Suite 9001 50 Charles St E, Toronto, ON M4Y 0B9','Unknown','Unknown','Unknown','Unknown','Toronto','ON','M4Y 0B9')
rw5 = rw_row(1005,100004,11111104, 'SITE 308 BOX 15 RR 3 LCD MAIN','Unknown','Unknown','Unknown','Unknown', 'London', 'ON', 'N9U 0P0')
r_wireline_mockup_data = spark.createDataFrame([rw1,rw2,rw3,rw4, rw5],schema=schema_rogers_wireline)
r_wireline_mockup_data.write.parquet(rogers_wireline_account,mode='overwrite')


# COMMAND ----------

# Rogers wireless mock data
# r_row = Row('x_rcis_id', 'fa_id')
r_wireless_mockup_data = spark.createDataFrame([],schema=schema_rogers_wireless)
r_wireless_mockup_data.write.parquet(rogers_wireless_account,mode='overwrite')

# COMMAND ----------

# Generate Shaw wireline mock data (No city, state and postal code in address)
sw_row = Row('contact_id','rcis_id', 'fa_id', 'service_address', 'service_street_name', 'service_street_type', 'service_street_direction', 'service_quadrant', 'service_city', 'service_province', 'service_postal_code')
sw1 = sw_row(101,200000,20020020000,'BOX 3002 STN C','Unknown','Unknown','Unknown','Unknown','OTTAWA','ON','K1Y 4J3')
sw2 = sw_row(102,200001,20020020001,"Data Department 27 1425 Python St E PO BOX 4001 STN A",'Unknown','Unknown','Unknown','Unknown','Victoria','BC','V8X 3X4')
sw3 = sw_row(103,200002,20020020002,'462 Cedar St RR 4 LCD MAIN','Unknown','Unknown','Unknown','Unknown','LLOYDMINSTER','AB','M5G 2R3')
sw4 = sw_row(104,200003,20020020003,'Suite 9001 50 Charles St E','Unknown','Unknown','Unknown','Unknown','Toronto','ON','M4Y 0B9')
sw5 = sw_row(105,200004,20020020004, 'SITE 308 BOX 15 RR 3 LCD MAIN','Unknown','Unknown','Unknown','Unknown', '', '', '')
s_wireline_mockup_data = spark.createDataFrame([sw1,sw2,sw3,sw4,sw5],schema=schema_shaw_wireline)
s_wireline_mockup_data.write.parquet(shaw_wireline_account,mode='overwrite')

# COMMAND ----------

# Generate Shaw wireless mock data
# r_row = Row('x_rcis_id', 'fa_id', 'service_address', 'service_city', 'service_province', 'service_postal_code')
s_wireless_mockup_data = spark.createDataFrame([],schema=schema_shaw_wireless)
s_wireless_mockup_data.write.parquet(shaw_wireless_account,mode='overwrite')


# COMMAND ----------

display(rogers_mockup_test_data)

# COMMAND ----------

display(shaw_mockup_test_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Save Mock up data

# COMMAND ----------

rogers_mockup_test_data.write.parquet(rogers_contact,mode='overwrite')
shaw_mockup_test_data.write.parquet(shaw_contact,mode='overwrite')

# COMMAND ----------

from ez_address_parser import AddressParser
name = 'BOX 3002 STN C, OTTAWA, ON K1Y4J3'
ap = AddressParser()
result = ap.parse(name)
result

# COMMAND ----------

# Prepared mock up address 
# Canada Post reference: https://origin-www.canadapost.ca/cpc/en/support/articles/addressing-guidelines/canadian-addresses.page?msclkid=f49e02b9bae411ecb99ad4bfa342a6bb

#1 Unit, Building, Street
a1 = "Suite 9001 50 Charles St E, Toronto, ON M4Y 0B9"
#2 Rural route, station, municipality
a2 = "462 Cedar St RR 4 LCD MAIN LLOYDMINSTER AB T9V 2Z9"
#3 Additional Info, PO BOX, Station
a3 ="Data Department 27 1425 Python St E PO BOX 4001 STN A Victoria BC V8X 3X4"
#4 Additionally info,  Rural route, station, municipality
a4 ="Site 6 Comp 10 RR 4 LCD MAIN LLOYDMINSTER AB T9V 2Z9"
#5 Unit number
a5 ="3609-2 St.Mary Blvd, Toronto, ON M5S 1J5"
#6 PO BOX 
a6 ="PO BOX 4001 Station 5 VICTORIA BC V8X 3X4"
#7 Municipality
a7 ="Athabasca County 3602 – 48th Avenue Athabasca, AB T9S 1M8"
#8 Building, additional info
a8 ="Scotia Arena Area 2 Unit 11 80 Blue Jays Way, Toronto, ON M5V 2G3"

# COMMAND ----------

# Test mock up address
ap = AddressParser()
address_lst = [a1,a2,a3,a4,a5,a6,a7,a8]
result = []
for a in address_lst:
    result+=[ap.parse(a)]

# COMMAND ----------


