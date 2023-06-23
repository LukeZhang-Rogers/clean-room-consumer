# Databricks notebook source
#test

# COMMAND ----------

# MAGIC %md ## Implementing Data Quality and Address Standardization 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC This notebook applies all the data quality rules driven by the data profiling results. The cleansed data is then fed through the address standardization mechanism to create cleansed and standardized datasets that are consumed by all downstream processing and modelling workloads.
# MAGIC
# MAGIC
# MAGIC ### Requirements:
# MAGIC
# MAGIC <ul>
# MAGIC
# MAGIC   <li>All input data models should be available in the Raw Zone</li>
# MAGIC
# MAGIC </ul>

# COMMAND ----------

# MAGIC %run ../2.Common/ADLSDesignedPaths

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurations

# COMMAND ----------

from pyspark.sql.functions import *
from ez_address_parser import AddressParser
from pyspark.sql.types import StringType, MapType

# COMMAND ----------

######### Look-up Lists ############
#Ensure the order is consistent in the look-up lists

keyword      = ['avenue', 'boulevard', 'boul', 'by-pass', 'centre', 'circle', 'circuit', 'concession', 'corners', 'court', 'crescent', 'cresent', 'crossing', 'cul-de-sac', 'diversion', 'drive', 'esplanade', 'estates', 'expressway', 'extension', 'freeway', 'gardens', 'glen', 'grounds', 'harbour', 'heights', 'highlands', 'highway', 'landing', 'limits', 'lookout', 'mountain', 'orchard', 'park', 'parkway', 'passage', 'pathway', 'place', 'plateau', 'point', 'private', 'promenade', 'range', 'ridge', 'road', 'route', 'square', 'street', 'subdivision', 'terrace', 'thicket', 'townline', 'turnabout', 'village', 'lane', 'close', 'loop', 'plaza', 'trail', 'st st', 'mount', 'chase', 'castle', 'gate', 'grove', 'way']

lookupid     = ['ave', 'blvd', 'blvd', 'bypass', 'ctr', 'cir', 'circt', 'conc', 'crnrs', 'crt', 'cres', 'cres', 'cross', 'cds', 'divers', 'dr', 'espl', 'estate', 'expy', 'exten', 'fwy', 'gdns', 'glen&', 'grnds', 'harbr', 'hts', 'hghlds', 'hwy', 'landng', 'lmts', 'lkout', 'mtn', 'orch', 'pk', 'pky', 'pass', 'ptway', 'pl', 'plat', 'pt', 'pvt', 'prom', 'rg', 'ridge', 'rd', 'rte', 'sq', 'st', 'subdiv', 'terr', 'thick', 'tline', 'trnabt', 'villge', 'ln', 'cls', 'lp', 'pz', 'tr', 'st', 'mt', 'ch', 'cs', 'ga', 'gr', 'wy']

phone_cols   = ['primary_phone', 'alternate_phone_1', 'alternate_phone_2']

filler_phone = ['0000000', '0000808', '1111111', '1111234', '1112222', '1231234', '1234567', '2222222', '3333333', '4111111', '4444444',  '5551111', '5551212', '5551234', '5555555', '6111111', '6666666', '7777777', '8888888', '9999999']

filler_email = ['^DNS@', '@DNS.', 'DONOTREPLY', 'NOREPLY', '^DH@\d', '^DONOTEMAIL', '^WG@\d', '^DK@\d', '^ABC@', '^ROGERS@ROGERS', '^NONE@', '^NO@', '^NOBODY@', '^E_MAIL@NOE_MAIL', '^EMAIL@EMAIL', '^E_MAIL@NO-E_MAIL',  '^NO-E_MAIL@', '^NO_E_MAIL@', '@NOEMAIL', '^DECLINED@']

french_char  = 'çáéíóúàèìòùñãõäëïöüÿâêîôûåøæœ'

eng_char     = 'caeiouaeiounaoaeiouyaeiouaoaeoe'

special_char = "[!#$%&\'()*+,-./:;<=>?@\\^_`{|}~]"

states_lookupid = ['ns','nl','nt','qc','bc','mb','nu','sk','on','ab','pe','yt','nb','al','ak','az','ar','ca','co','ct','de','dc','fl','ga','hi','id','il','in','ia','ks','ky','la','me','md','ma','mi','mn','ms','mo','mt','ne','nv','nh','nj','nm','ny','nc','nd','oh','ok','or','pa','ri','sc','sd','tn','tx','ut','vt','va','wa','wv','wi','wy','as','gu','mp','pr','vi']

# COMMAND ----------

#Column names for basic cleaning
contact_cols = ['rcis_id', 'fa_id','first_name', 'last_name', 'dq_e_mail', 'dq_primary_phone', 'dq_alternate_phone_1', 'dq_alternate_phone_2', 'address', 'city', 'state', 'zipcode', 'mailing_address_no_zipcode_dq', 'mailing_address_no_zipcode', 'mailing_address_full']

wln_cols     = ['rcis_id', 'fa_id', 'service_address', 'service_street_name', 'service_street_type', 'service_street_direction', 'service_quadrant', 'service_city', 'service_province', 'service_postal_code', 'service_address_no_zipcode_dq', 'service_address_no_zipcode', 'service_address_full']

wls_cols     = ['rcis_id', 'fa_id']

wls_ser_cols = ['fa_id']

# COMMAND ----------

#Function to parse address and return address segments as a dict
ap = AddressParser()

def func_ap(p):
    p = ap.parse(p) #parses the supplied input and generates a list of dictionaries. Ex. [{'34':'street_name', 'road':'street_type'}]
    p = [t[::-1] for t in p] #reverses the key value pairs
    d = {}
    for x, y in p:
        d.setdefault(x, []).append(y) #collates multiple values with same key under one key. Ex. {'municipality':['PO', 'BOX']}
    for i,j in d.items(): 
        d[i] = ' '.join(j) #converts list to string. Ex. {'municipality':'PO BOX'}
    return d

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the Datasets

# COMMAND ----------

shaw_consumer_wireless_account_df = spark.read.format("delta").load(shaw_consumer_wireless_account)
shaw_consumer_wireline_account_df = spark.read.format("delta").load(shaw_consumer_wireline_account)
shaw_consumer_contact_df          = spark.read.format("delta").load(shaw_consumer_contact)
shaw_consumer_wireless_service_df = spark.read.format("delta").load(shaw_consumer_wireless_service)

rogers_contact_df                 = spark.read.format("delta").load(rogers_contact)
rogers_wireless_account_df        = spark.read.format("delta").load(rogers_wireless_account)
rogers_wireline_account_df        = spark.read.format("delta").load(rogers_wireline_account)
rogers_wireless_service_df        = spark.read.format("delta").load(rogers_wireless_service)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rogers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contact

# COMMAND ----------

# MAGIC %md
# MAGIC Pre-processing input data for address parser

# COMMAND ----------

rogers_contact_df_temp = rogers_contact_df
rogers_contact_df_temp = rogers_contact_df_temp.withColumn('ingestion_date', current_timestamp())
rogers_contact_df_temp = rogers_contact_df_temp.withColumn("city_cleansed", when((col("city").like("%\'%"))| (col("city").like('%-%')) |\
                                                                                 (col("city").like('% %')) | (col("city").like('%.%')) ,translate(col("city")," -'.",""))\
                                                           .otherwise(col("city")))

rogers_contact_df_temp = rogers_contact_df_temp.withColumn("address_cleansed", when((col("city").like("%\'%")) | (col("city").like('%-%')) |\
                                                                                    (col("city").like('% %')) | (col("city").like('%.%')) ,expr("replace(address, city, city_cleansed)"))\
                                                           .otherwise(col("address")))

rogers_contact_df_temp = rogers_contact_df_temp.withColumn("address_cleansed", lower(regexp_replace(col("address_cleansed"), "[%#\']", "")))

# COMMAND ----------

#Generating temporary IDs to be used for joins
rogers_contact_df_temp = rogers_contact_df_temp.withColumn("id", monotonically_increasing_id())
rogers_contact_df_temp = rogers_contact_df_temp.na.fill(value=' ', subset=['address_cleansed'])

# COMMAND ----------

#Create and apply UDF to address attribute to generate key-value pairs
udf_star_desc        = udf(lambda x:func_ap(x),MapType(StringType(), StringType())) #list(tuples(string,string))

my_rogers_contact_df = rogers_contact_df_temp.withColumn("parsed_cols",udf_star_desc(col("address_cleansed"))).select("parsed_cols", "id")

# COMMAND ----------

#Transpose the parsed elements to separate columns
rogers_contact_parsed_add_df = my_rogers_contact_df.select("id", explode('parsed_cols')).groupBy("id").pivot('key').agg(first('value'))

# COMMAND ----------

#Tie the newly created dataframes back to the input

rogers_contact_combined_add_df = rogers_contact_df_temp.join(rogers_contact_parsed_add_df, ['id'], 'left').drop('id')

# COMMAND ----------

#Standardize the StreetType column extracted by the parser
rogers_contact_normalized_add_df = rogers_contact_combined_add_df
rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('StreetType_Cleansed', trim(col('StreetType')))

for pattern, replacement in zip(keyword, lookupid):
    rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('StreetType_Cleansed',regexp_replace('StreetType_Cleansed', pattern, replacement))

rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('StreetType_Cleansed', when(col('StreetType_Cleansed') == 'av', 'ave')\
                                                                                             .when(col('StreetType_Cleansed') == 'cr', 'cres')\
                                                                                             .when(col('StreetType_Cleansed') == 'bl', 'blvd')\
                                                                                             .otherwise(col('StreetType_Cleansed')))

#Standardize the StreetDirection column extracted by the parser
rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('StreetDirection_Cleansed', 
                             when((col('StreetDirection') == 'nord') 
                                  | (col('StreetDirection') == 'north'), 'n')
                            .when((col('StreetDirection') == 'o') 
                                  | (col('StreetDirection') == 'ouest') | (col('StreetDirection') == 'west'), 'w')
                            .when((col('StreetDirection') == 'est') 
                                  | (col('StreetDirection') == 'east'), 'e')
                            .when((col('StreetDirection') == 'sud') 
                                  | (col('StreetDirection') == 'south'), 's')
                            .when((col('StreetDirection') == 'nord ouest') 
                                  | (col('StreetDirection') == 'northwest') 
                                  | (col('StreetDirection') == 'north west') 
                                  | (col('StreetDirection') == 'no'), 'nw')
                            .when((col('StreetDirection') == 'nord est') 
                                  | (col('StreetDirection') == 'northeast')
                                  | (col('StreetDirection') == 'north east'), 'ne')
                            .when((col('StreetDirection') == 'sud ouest') 
                                  | (col('StreetDirection') == 'southwest') 
                                  | (col('StreetDirection') == 'south west') 
                                  | (col('StreetDirection') == 'so'), 'sw')
                            .when((col('StreetDirection') == 'sud est') 
                                  | (col('StreetDirection') == 'southeast') 
                                  | (col('StreetDirection') == 'south east'), 'se')
                            .otherwise(regexp_replace(
                                regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace
                                   (
                                      col('StreetDirection'), 'north', 'n')
                                     , 'south', 's')
                                     , 'nord', 'n')
                                     , 'sud', 's')
                                     , 'east', 'e')
                                     , 'west', 'w')
                                     , 'est', 'e')
                                     , 'ouest', 'w')
                                   ))
  
#Standardize the PostalBox column extracted by the parser
rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('PostalBox_Cleansed', 
                             when((col('PostalBox') == 'null postal box') | (col('PostalBox') == 'null box'), 'po box')
                            .when(col('PostalBox').like('%postal box'), 'po box')
                            .when(col('PostalBox').like('%po box'), 'po box')
                            .when(col('PostalBox') == 'box' , 'po box')
                            .when(col('PostalBox') == 'p o box' , 'po box')
                            .when(col('PostalBox') == 'please specify box' , 'po box')
                            .when((col('PostalBox') == 'box box') | (col('PostalBox') == 'po box box'), 'po box')
                            .otherwise(col('PostalBox')))

#Standardize the Province column extracted by the parser
rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('Province_Cleansed',
                                           when(rogers_contact_normalized_add_df.Province.isin(states_lookupid),
                                                rogers_contact_normalized_add_df.Province)
                                          .when(rogers_contact_normalized_add_df.state.isin(states_lookupid), 
                                                rogers_contact_normalized_add_df.state)
                                          .when((rogers_contact_normalized_add_df.Province.isin('pq','qu','qb')) | \
                                                (rogers_contact_normalized_add_df.state.isin('pq','qu','qb')), 'qc')
                                          .when((rogers_contact_normalized_add_df.Province.isin('nf','nfl')) | \
                                                  (rogers_contact_normalized_add_df.state.isin('nf','nfl')), 'nl')
                                          .when((rogers_contact_normalized_add_df.Province == 'yk') | (rogers_contact_normalized_add_df.state == 'yk'), 'yt')
                                          .when((rogers_contact_normalized_add_df.Province == 'sa') | (rogers_contact_normalized_add_df.state == 'sa'), 'sk')
                                          .when((rogers_contact_normalized_add_df.Province == 'nw') | (rogers_contact_normalized_add_df.state == 'nw'), 'nt')
                                          .when(rogers_contact_normalized_add_df.Province.isin('xx', '**'), None)
                                          .when(rogers_contact_normalized_add_df.Province.like('% xx'), 
                                                rogers_contact_normalized_add_df.Province.substr(lit(1), length(rogers_contact_normalized_add_df.Province)-3))
                                          .otherwise(rogers_contact_normalized_add_df.Province))

#Standardize the PostalCode column extracted by the parser
rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('PostalCode_Cleansed', 
                             when(rogers_contact_normalized_add_df.Province_Cleansed.isin('ns','nl','nt','qc','bc','mb','nu','sk','on','ab','pe','yt','nb'), \
                                  regexp_replace(rogers_contact_normalized_add_df.PostalCode, " ", ""))
                               .otherwise(rogers_contact_normalized_add_df.PostalCode)
                              )

#For MailingAddress, there are 3 versions -
# MailingAddress_no_zipcode_dq : contains all elements except zipcode and direction/quadrant
# MailingAddress_no_zipcode    : contains all elements except zipcode
# MailingAddress_full          : contains all elements


#Standardize the MailingAddress_no_zipcode_dq column extracted by the parser
rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('mailing_address_no_zipcode_dq', \
                                     lower(trim(concat((when(col('UnitNumber').isNull(),"")
                                                    .otherwise(concat(col('UnitNumber'), lit(' ')))),\
                                                   (when(col('BuildingNumber').isNull(), "")
                                                    .otherwise(concat(col('BuildingNumber'), lit(' ')))), \
                                                   (when(col('StreetNumber').isNull(), "")
                                                    .otherwise(concat(col('StreetNumber'), lit(' ')))), \
                                                   (when(col('StreetName').isNull(), "")
                                                    .otherwise(concat(col('StreetName'), lit(' ')))), \
                                                   (when(col('StreetType_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('StreetType_Cleansed'), lit(' ')))), \
                                                   (when(col('PostalBox_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('PostalBox_Cleansed'), lit(' ')))), \
                                                   (when(col('PostalBoxNumber').isNull(), "")
                                                    .otherwise(concat(col('PostalBoxNumber'), lit(' ')))), \
                                                   (when(col('Station').isNull(), "")
                                                    .otherwise(concat(col('Station'), lit(' ')))), \
                                                   (when(col('StationNumber').isNull(), "")
                                                    .otherwise(concat(col('StationNumber'), lit(' ')))), \
                                                   (when(col('RuralRoute').isNull(), "")
                                                    .otherwise(concat(col('RuralRoute'), lit(' ')))), \
                                                   (when(col('RuralRouteNumber').isNull(), "")
                                                    .otherwise(concat(col('RuralRouteNumber'), lit(' ')))), \
                                                   (when(col('AdditionalInfo').isNull(), "")
                                                    .otherwise(concat(col('AdditionalInfo'), lit(' ')))), \
                                                   (when(col('Municipality').isNull(), "")
                                                    .otherwise(concat(col('Municipality'), lit(' ')))), \
                                                   (when(col('Province_Cleansed').isNull(), "")
                                                    .otherwise(col('Province_Cleansed'))) \
                                                                     ) \
                                                            ) \
                                                        ) \
                                              )

#Standardize the MailingAddress_no_zipcode column extracted by the parser
rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('mailing_address_no_zipcode', \
                                     lower(trim(concat((when(col('UnitNumber').isNull(),"")
                                                     .otherwise(concat(col('UnitNumber'), lit(' ')))),\
                                                   (when(col('BuildingNumber').isNull(), "")
                                                    .otherwise(concat(col('BuildingNumber'), lit(' ')))), \
                                                   (when(col('StreetNumber').isNull(), "")
                                                    .otherwise(concat(col('StreetNumber'), lit(' ')))), \
                                                   (when(col('StreetName').isNull(), "")
                                                    .otherwise(concat(col('StreetName'), lit(' ')))), \
                                                   (when(col('StreetType_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('StreetType_Cleansed'), lit(' ')))), \
                                                   (when(col('StreetDirection_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('StreetDirection_Cleansed'), lit(' ')))), \
                                                   (when(col('PostalBox_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('PostalBox_Cleansed'), lit(' ')))), \
                                                   (when(col('PostalBoxNumber').isNull(), "")
                                                    .otherwise(concat(col('PostalBoxNumber'), lit(' ')))), \
                                                   (when(col('Station').isNull(), "")
                                                    .otherwise(concat(col('Station'), lit(' ')))), \
                                                   (when(col('StationNumber').isNull(), "")
                                                    .otherwise(concat(col('StationNumber'), lit(' ')))), \
                                                   (when(col('RuralRoute').isNull(), "")
                                                    .otherwise(concat(col('RuralRoute'), lit(' ')))), \
                                                   (when(col('RuralRouteNumber').isNull(), "")
                                                    .otherwise(concat(col('RuralRouteNumber'), lit(' ')))), \
                                                   (when(col('AdditionalInfo').isNull(), "")
                                                    .otherwise(concat(col('AdditionalInfo'), lit(' ')))), \
                                                   (when(col('Municipality').isNull(), "")
                                                    .otherwise(concat(col('Municipality'), lit(' ')))), \
                                                   (when(col('Province_Cleansed').isNull(), "")
                                                    .otherwise(col('Province_Cleansed'))) \
                                                               ) \
                                                      ) \
                                                  ) \
                                          )

#Standardize the MailingAddress_full column extracted by the parser
rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('mailing_address_full', \
                                             lower(trim(concat((when(col('UnitNumber').isNull(),"")
                                                                .otherwise(concat(col('UnitNumber'), lit(' ')))),\
                                                               (when(col('BuildingNumber').isNull(), "")
                                                                .otherwise(concat(col('BuildingNumber'), lit(' ')))), \
                                                               (when(col('StreetNumber').isNull(), "") \
                                                                .otherwise(concat(trim(col('StreetNumber')), lit(' ')))), \
                                                               (when(col('StreetName').isNull(), "") \
                                                                .otherwise(concat(trim(col('StreetName')), lit(' ')))), \
                                                               (when(col('StreetType_Cleansed').isNull(), "") \
                                                                .otherwise(concat(trim(col('StreetType_Cleansed')), lit(' ')))), \
                                                               (when(col('StreetDirection_Cleansed').isNull(), "") \
                                                                .otherwise(concat(trim(col('StreetDirection_Cleansed')), lit(' ')))), \
                                                               (when(col('PostalBox_Cleansed').isNull(), "") \
                                                                .otherwise(concat(trim(col('PostalBox_Cleansed')), lit(' ')))), \
                                                               (when(col('PostalBoxNumber').isNull(), "") \
                                                                .otherwise(concat(trim(col('PostalBoxNumber')), lit(' ')))), \
                                                               (when(col('Station').isNull(), "") \
                                                                .otherwise(concat(trim(col('Station')), lit(' ')))), \
                                                               (when(col('StationNumber').isNull(), "") \
                                                                .otherwise(concat(trim(col('StationNumber')), lit(' ')))), \
                                                               (when(col('RuralRoute').isNull(), "") \
                                                                .otherwise(concat(trim(col('RuralRoute')), lit(' ')))), \
                                                               (when(col('RuralRouteNumber').isNull(), "") \
                                                                .otherwise(concat(trim(col('RuralRouteNumber')), lit(' ')))), \
                                                               (when(col('AdditionalInfo').isNull(), "")
                                                                .otherwise(concat(col('AdditionalInfo'), lit(' ')))), \
                                                               (when(col('Municipality').isNull(), "")
                                                                .otherwise(concat(trim(col('Municipality')), lit(' ')))), \
                                                               (when(col('Province_Cleansed').isNull(), "")
                                                                .otherwise(concat(trim(col('Province_Cleansed')), lit(' ')))), \
                                                               (when(col('PostalCode_Cleansed').isNull(), "")
                                                                .otherwise(trim(col('PostalCode_Cleansed')))) \
                                                                         ) \
                                                                ) \
                                                            ) \
                              )


#Replacing the parsed Null values with the original address values
rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('mailing_address_no_zipcode_dq', 
                                                                           when(col('mailing_address_no_zipcode_dq')=='', col('address'))
                                                                           .otherwise(col('mailing_address_no_zipcode_dq')))
rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('mailing_address_no_zipcode', 
                                                                           when(col('mailing_address_no_zipcode')=='', col('address'))
                                                                           .otherwise(col('mailing_address_no_zipcode')))
rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('mailing_address_full', 
                                                                           when(col('mailing_address_full')=='', col('address'))
                                                                           .otherwise(col('mailing_address_full')))

# COMMAND ----------

#Removing values with length < 4 and filler values (specified in an array in the Configurations section)
for c_name in phone_cols:
    c = '{}{}'.format('dq_', c_name)
    rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn(c, when(length(col(c_name))<4, lit(None)).otherwise(col(c_name)))\
                                                                       .withColumn(c, when(col(c).rlike("|".join(filler_phone)), lit(None)).otherwise(col(c)))

# COMMAND ----------

#Replacing email ID values with username lenghts > 63, removing characters between '+' and '@', removing filler values
rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('dq_e_mail', when(length(substring_index(col('e_mail'), '@', 1))>63, lit(None)).otherwise(col('e_mail')))\
                                                                   .withColumn('dq_e_mail', regexp_replace('dq_e_mail', '[(?+)].*@', '@'))\
                                                                   .withColumn('dq_e_mail', when(upper(col('dq_e_mail')).rlike("|".join(filler_email)), lit(None)).otherwise(col('dq_e_mail')))

# COMMAND ----------

rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumnRenamed('x_rcis_id', 'rcis_id')

# COMMAND ----------

#Replacing french characters with english characters
for c_name in contact_cols:
    c = '{}{}'.format(c_name, '_cl')
    rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn(c, regexp_replace(translate(lower(trim(c_name)), french_char, eng_char), special_char, ""))\
                                                                       .withColumn(c, when((col(c)=='none') | (col(c)=='null') | (col(c) == '' ), lit(None)).otherwise(col(c)))

# COMMAND ----------

#Replacing certain values with Nulls
rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('dq_first_name_cl', when((col('first_name_cl')== 'comp') |\
                                                                                                        (col('first_name_cl')== 'unknown') |\
                                                                                                        (col('first_name_cl')== 'blank'), lit(None))\
                                                                               .otherwise(col('first_name_cl')))

rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('dq_last_name_cl', when((col('last_name_cl')== 'unknown'), lit(None))\
                                                                               .otherwise(col('last_name_cl')))

# COMMAND ----------

#Building full name using parsed first name and last name
rogers_contact_normalized_add_df = rogers_contact_normalized_add_df.withColumn('dq_full_name_cl', concat_ws(' ', col("dq_first_name_cl"), col("dq_last_name_cl")))    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wireline

# COMMAND ----------

rogers_wireline_account_df_temp = rogers_wireline_account_df
rogers_wireline_account_df_temp = rogers_wireline_account_df_temp.withColumn('ingestion_date', current_timestamp())
rogers_wireline_account_df_temp = rogers_wireline_account_df_temp.withColumn("service_city_cleansed", 
                                                                             when((col("service_city").like("%\'%")) | (col("service_city").like('%-%')) |\
                                                                                  (col("service_city").like('% %'))| (col("service_city").like('%.%'))\
                                                                                  ,translate(col("service_city")," -'.",""))\
                                                                             .otherwise(col("service_city")))

rogers_wireline_account_df_temp = rogers_wireline_account_df_temp.withColumn("service_address_cleansed", 
                                                                             when((col("service_city").like("%\'%")) | (col("service_city").like('%-%')) | (col("service_city").like('% %')) | (col("service_city").like('%.%'))
                                                                                  ,expr("replace(service_address, service_city, service_city_cleansed)"))
                                                                             .otherwise(col("service_address")))
rogers_wireline_account_df_temp = rogers_wireline_account_df_temp.withColumn("service_address_cleansed", lower(regexp_replace(col("service_address_cleansed"), "[%#\']", "")))

# COMMAND ----------

#Generating temporary IDs to be used for joins
rogers_wireline_account_df_temp = rogers_wireline_account_df_temp.withColumn("id", monotonically_increasing_id())
rogers_wireline_account_df_temp = rogers_wireline_account_df_temp.na.fill(value=' ', subset=['service_address_cleansed'])

# COMMAND ----------

udf_star_desc2                = udf(lambda x:func_ap(x),MapType(StringType(), StringType())) #list(tuples(string,string))
my_rogers_wireline_account_df = rogers_wireline_account_df_temp.withColumn("parsed_cols",udf_star_desc2(col("service_address_cleansed"))).select("parsed_cols", "id")

# COMMAND ----------

rogers_wireline_account_parsed_add_df = my_rogers_wireline_account_df.select("id", explode('parsed_cols')).groupBy("id").pivot('key').agg(first('value'))

# COMMAND ----------

rogers_wireline_account_combined_add_df = rogers_wireline_account_df_temp.join(rogers_wireline_account_parsed_add_df, ['id'], 'left').drop('id')

# COMMAND ----------

rogers_wireline_account_normalized_add_df = rogers_wireline_account_combined_add_df
rogers_wireline_account_normalized_add_df = rogers_wireline_account_normalized_add_df.withColumn('StreetType_Cleansed', trim(col('StreetType')))

for pattern, replacement in zip(keyword, lookupid):
    rogers_wireline_account_normalized_add_df = rogers_wireline_account_normalized_add_df.withColumn('StreetType_Cleansed',regexp_replace('StreetType_Cleansed', pattern, replacement))

rogers_wireline_account_normalized_add_df = rogers_wireline_account_normalized_add_df.withColumn('StreetType_Cleansed', when(col('StreetType_Cleansed') == 'av', 'ave')\
                                                                                             .when(col('StreetType_Cleansed') == 'cr', 'cres')\
                                                                                             .when(col('StreetType_Cleansed') == 'bl', 'blvd')\
                                                                                             .otherwise(col('StreetType_Cleansed')))

rogers_wireline_account_normalized_add_df = rogers_wireline_account_normalized_add_df.withColumn('StreetDirection_Cleansed', 
                             when((col('StreetDirection') == 'nord') 
                                  | (col('StreetDirection') == 'north'), 'n')
                            .when((col('StreetDirection') == 'o') 
                                  | (col('StreetDirection') == 'ouest') | (col('StreetDirection') == 'west'), 'w')
                            .when((col('StreetDirection') == 'est') 
                                  | (col('StreetDirection') == 'east'), 'e')
                            .when((col('StreetDirection') == 'sud') 
                                  | (col('StreetDirection') == 'south'), 's')
                            .when((col('StreetDirection') == 'nord ouest') 
                                  | (col('StreetDirection') == 'northwest') 
                                  | (col('StreetDirection') == 'north west') 
                                  | (col('StreetDirection') == 'no'), 'nw')
                            .when((col('StreetDirection') == 'nord est') 
                                  | (col('StreetDirection') == 'northeast')
                                  | (col('StreetDirection') == 'north east'), 'ne')
                            .when((col('StreetDirection') == 'sud ouest') 
                                  | (col('StreetDirection') == 'southwest') 
                                  | (col('StreetDirection') == 'south west') 
                                  | (col('StreetDirection') == 'so'), 'sw')
                            .when((col('StreetDirection') == 'sud est') 
                                  | (col('StreetDirection') == 'southeast') 
                                  | (col('StreetDirection') == 'south east'), 'se')
                            .otherwise(regexp_replace(
                                regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace
                                   (
                                      col('StreetDirection'), 'north', 'n')
                                     , 'south', 's')
                                     , 'nord', 'n')
                                     , 'sud', 's')
                                     , 'east', 'e')
                                     , 'west', 'w')
                                     , 'est', 'e')
                                     , 'ouest', 'w')
                                   ))

rogers_wireline_account_normalized_add_df = rogers_wireline_account_normalized_add_df.withColumn('PostalBox_Cleansed', 
                             when((col('PostalBox') == 'null postal box') | (col('PostalBox') == 'null box'), 'po box')
                            .when(col('PostalBox').like('%postal box'), 'po box')
                            .when(col('PostalBox').like('%po box'), 'po box')
                            .when(col('PostalBox') == 'box' , 'po box')
                            .when(col('PostalBox') == 'p o box' , 'po box')
                            .when(col('PostalBox') == 'please specify box' , 'po box')
                            .when((col('PostalBox') == 'box box') | (col('PostalBox') == 'po box box'), 'po box')
                            .otherwise(col('PostalBox')))

rogers_wireline_account_normalized_add_df = rogers_wireline_account_normalized_add_df.withColumn('Province_Cleansed', 
                             when(rogers_wireline_account_normalized_add_df.Province.isin(states_lookupid),
                                  rogers_wireline_account_normalized_add_df.Province)
                            .when(rogers_wireline_account_normalized_add_df.service_province.isin(states_lookupid),
                                  rogers_wireline_account_normalized_add_df.service_province)
                            .when((rogers_wireline_account_normalized_add_df.Province.isin('pq','qu','qb')) 
                                  | (rogers_wireline_account_normalized_add_df.service_province.isin('pq','qu','qb')), 'qc')
                            .when((rogers_wireline_account_normalized_add_df.Province.isin('nf','nfl')) 
                                  | (rogers_wireline_account_normalized_add_df.service_province.isin('nf','nfl')), 'nl')
                            .when((rogers_wireline_account_normalized_add_df.Province == 'yk') 
                                  | (rogers_wireline_account_normalized_add_df.service_province == 'yk'), 'yt')
                            .when((rogers_wireline_account_normalized_add_df.Province == 'sa') 
                                  | (rogers_wireline_account_normalized_add_df.service_province == 'sa'), 'sk')
                            .when((rogers_wireline_account_normalized_add_df.Province == 'nw') 
                                  | (rogers_wireline_account_normalized_add_df.service_province == 'nw'), 'nt')
                            .when(rogers_wireline_account_normalized_add_df.Province.isin('xx', '**'), None)
                            .when(rogers_wireline_account_normalized_add_df.Province.like('% xx'), 
                                  rogers_wireline_account_normalized_add_df.Province.substr(lit(1),
                                                length(rogers_wireline_account_normalized_add_df.Province)-3))
                            .otherwise(rogers_wireline_account_normalized_add_df.Province))

rogers_wireline_account_normalized_add_df = rogers_wireline_account_normalized_add_df.withColumn('PostalCode_Cleansed', 
                             when(rogers_wireline_account_normalized_add_df.Province_Cleansed.isin('ns','nl','nt','qc','bc','mb','nu','sk',
                                                                                                   'on','ab','pe','yt','nb'), \
                                  regexp_replace(rogers_wireline_account_normalized_add_df.PostalCode, " ", ""))
                               .otherwise(rogers_wireline_account_normalized_add_df.PostalCode))

rogers_wireline_account_normalized_add_df = rogers_wireline_account_normalized_add_df.withColumn('service_address_no_zipcode_dq', \
                                 lower(trim(concat((when(col('UnitNumber').isNull(),"")
                                                    .otherwise(concat(col('UnitNumber'), lit(' ')))),\
                                                   (when(col('BuildingNumber').isNull(), "")
                                                    .otherwise(concat(col('BuildingNumber'), lit(' ')))), \
                                                   (when(col('StreetNumber').isNull(), "")
                                                    .otherwise(concat(col('StreetNumber'), lit(' ')))), \
                                                   (when(col('StreetName').isNull(), "")
                                                    .otherwise(concat(col('StreetName'), lit(' ')))), \
                                                   (when(col('StreetType_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('StreetType_Cleansed'), lit(' ')))), \
                                                   (when(col('PostalBox_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('PostalBox_Cleansed'), lit(' ')))), \
                                                   (when(col('PostalBoxNumber').isNull(), "")
                                                    .otherwise(concat(col('PostalBoxNumber'), lit(' ')))), \
                                                   (when(col('StationNumber').isNull(), "")
                                                    .otherwise(concat(col('StationNumber'), lit(' ')))), \
                                                   (when(col('RuralRoute').isNull(), "")
                                                    .otherwise(concat(col('RuralRoute'), lit(' ')))), \
                                                   (when(col('RuralRouteNumber').isNull(), "")
                                                    .otherwise(concat(col('RuralRouteNumber'), lit(' ')))), \
                                                   (when(col('AdditionalInfo').isNull(), "")
                                                    .otherwise(concat(col('AdditionalInfo'), lit(' ')))), \
                                                   (when(col('Municipality').isNull(), "")
                                                    .otherwise(concat(col('Municipality'), lit(' ')))), \
                                                   (when(col('Province_Cleansed').isNull(), "")
                                                    .otherwise(col('Province_Cleansed'))) \
                                                                         ) \
                                                                ) \
                                                            ) \
                              )

rogers_wireline_account_normalized_add_df = rogers_wireline_account_normalized_add_df.withColumn('service_address_no_zipcode', \
                                 lower(trim(concat((when(col('UnitNumber').isNull(),"")
                                                     .otherwise(concat(col('UnitNumber'), lit(' ')))),\
                                                   (when(col('BuildingNumber').isNull(), "")
                                                    .otherwise(concat(col('BuildingNumber'), lit(' ')))), \
                                                   (when(col('StreetNumber').isNull(), "")
                                                    .otherwise(concat(col('StreetNumber'), lit(' ')))), \
                                                   (when(col('StreetName').isNull(), "")
                                                    .otherwise(concat(col('StreetName'), lit(' ')))), \
                                                   (when(col('StreetType_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('StreetType_Cleansed'), lit(' ')))), \
                                                   (when(col('StreetDirection_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('StreetDirection_Cleansed'), lit(' ')))), \
                                                   (when(col('PostalBox_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('PostalBox_Cleansed'), lit(' ')))), \
                                                   (when(col('PostalBoxNumber').isNull(), "")
                                                    .otherwise(concat(col('PostalBoxNumber'), lit(' ')))), \
                                                   (when(col('StationNumber').isNull(), "")
                                                    .otherwise(concat(col('StationNumber'), lit(' ')))), \
                                                   (when(col('RuralRoute').isNull(), "")
                                                    .otherwise(concat(col('RuralRoute'), lit(' ')))), \
                                                   (when(col('RuralRouteNumber').isNull(), "")
                                                    .otherwise(concat(col('RuralRouteNumber'), lit(' ')))), \
                                                   (when(col('AdditionalInfo').isNull(), "")
                                                    .otherwise(concat(col('AdditionalInfo'), lit(' ')))), \
                                                   (when(col('Municipality').isNull(), "")
                                                    .otherwise(concat(col('Municipality'), lit(' ')))), \
                                                   (when(col('Province_Cleansed').isNull(), "")
                                                    .otherwise(col('Province_Cleansed'))) \
                                                                         ) \
                                                                ) \
                                                            ) \
                              )

rogers_wireline_account_normalized_add_df = rogers_wireline_account_normalized_add_df.withColumn('service_address_full', \
                                                       lower(trim(concat((when(col('UnitNumber').isNull(),"")
                                                                .otherwise(concat(col('UnitNumber'), lit(' ')))),\
                                                               (when(col('BuildingNumber').isNull(), "")
                                                                .otherwise(concat(col('BuildingNumber'), lit(' ')))), \
                                                               (when(col('StreetNumber').isNull(), "") \
                                                                .otherwise(concat(trim(col('StreetNumber')), lit(' ')))), \
                                                               (when(col('StreetName').isNull(), "") \
                                                                .otherwise(concat(trim(col('StreetName')), lit(' ')))), \
                                                               (when(col('StreetType_Cleansed').isNull(), "") \
                                                                .otherwise(concat(trim(col('StreetType_Cleansed')), lit(' ')))), \
                                                               (when(col('StreetDirection_Cleansed').isNull(), "") \
                                                                .otherwise(concat(trim(col('StreetDirection_Cleansed')), lit(' ')))), \
                                                               (when(col('PostalBox_Cleansed').isNull(), "") \
                                                                .otherwise(concat(trim(col('PostalBox_Cleansed')), lit(' ')))), \
                                                               (when(col('PostalBoxNumber').isNull(), "") \
                                                                .otherwise(concat(trim(col('PostalBoxNumber')), lit(' ')))), \
                                                               (when(col('StationNumber').isNull(), "") \
                                                                .otherwise(concat(trim(col('StationNumber')), lit(' ')))), \
                                                               (when(col('RuralRoute').isNull(), "") \
                                                                .otherwise(concat(trim(col('RuralRoute')), lit(' ')))), \
                                                               (when(col('RuralRouteNumber').isNull(), "") \
                                                                .otherwise(concat(trim(col('RuralRouteNumber')), lit(' ')))), \
                                                               (when(col('AdditionalInfo').isNull(), "")
                                                                .otherwise(concat(col('AdditionalInfo'), lit(' ')))), \
                                                               (when(col('Municipality').isNull(), "")
                                                                .otherwise(concat(trim(col('Municipality')), lit(' ')))), \
                                                               (when(col('Province_Cleansed').isNull(), "")
                                                                .otherwise(concat(trim(col('Province_Cleansed')), lit(' ')))), \
                                                               (when(col('PostalCode_Cleansed').isNull(), "")
                                                                .otherwise(trim(col('PostalCode_Cleansed')))) \
                                                                         ) \
                                                                ) \
                                                            ) \
                              )

rogers_wireline_account_normalized_add_df = rogers_wireline_account_normalized_add_df.withColumn('service_address_no_zipcode_dq', 
                                                                           when(col('service_address_no_zipcode_dq')=='', col('service_address'))
                                                                           .otherwise(col('service_address_no_zipcode_dq')))

rogers_wireline_account_normalized_add_df = rogers_wireline_account_normalized_add_df.withColumn('service_address_no_zipcode', 
                                                                           when(col('service_address_no_zipcode')=='', col('service_address'))
                                                                           .otherwise(col('service_address_no_zipcode')))

rogers_wireline_account_normalized_add_df = rogers_wireline_account_normalized_add_df.withColumn('service_address_full', 
                                                                           when(col('service_address_full')=='', col('service_address'))
                                                                           .otherwise(col('service_address_full')))

# COMMAND ----------

rogers_wireline_account_normalized_add_df = rogers_wireline_account_normalized_add_df.withColumnRenamed('x_rcis_id', 'rcis_id')

# COMMAND ----------

for c_name in wln_cols:
    c = '{}{}'.format(c_name, '_cl')
    rogers_wireline_account_normalized_add_df = rogers_wireline_account_normalized_add_df.withColumn(c, regexp_replace(translate(lower(trim(c_name)), french_char, eng_char), special_char, ""))\
                                                                                         .withColumn(c, when((col(c)=='none') | (col(c)=='null') | (col(c) == '' ), lit(None)).otherwise(col(c)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wireless

# COMMAND ----------

# MAGIC %md
# MAGIC #### Account

# COMMAND ----------

rogers_wireless_account_df = rogers_wireless_account_df.withColumn('ingestion_date', current_timestamp())
rogers_wireless_account_df = rogers_wireless_account_df.withColumnRenamed('x_rcis_id', 'rcis_id')

#Cleaning certain columns
for c_name in wls_cols:
    c = '{}{}'.format(c_name, '_cl')
    rogers_wireless_account_df = rogers_wireless_account_df.withColumn(c, regexp_replace(translate(lower(trim(c_name)), french_char, eng_char), special_char, ""))\
                                                           .withColumn(c, when((col(c)=='none') | (col(c)=='null') | (col(c) == '' ), lit(None)).otherwise(col(c)))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Service

# COMMAND ----------

rogers_wireless_service_df = rogers_wireless_service_df.withColumn('ingestion_date', current_timestamp())

for c_name in wls_ser_cols:
    c = '{}{}'.format(c_name, '_cl')
    rogers_wireless_service_df = rogers_wireless_service_df.withColumn(c, regexp_replace(translate(lower(trim(c_name)), french_char, eng_char), special_char, ""))\
                                                           .withColumn(c, when((col(c)=='none') | (col(c)=='null') | (col(c) == '' ), lit(None)).otherwise(col(c)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shaw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contact

# COMMAND ----------

shaw_consumer_contact_df = shaw_consumer_contact_df.select([col(x).alias(x.lower()) for x in shaw_consumer_contact_df.columns])

# COMMAND ----------

shaw_contact_df_temp = shaw_consumer_contact_df
shaw_contact_df_temp = shaw_contact_df_temp.withColumn('ingestion_date', current_timestamp())
shaw_contact_df_temp = shaw_contact_df_temp.withColumn("city_cleansed", when((col("city").like("%\'%")) | (col("city").like('%-%'))  | (col("city").like('% %')) | (col("city").like('%.%')) 
                                                                             ,translate(col("city")," -'.",""))
                                                       .otherwise(col("city")))

shaw_contact_df_temp = shaw_contact_df_temp.withColumn("address_full", concat_ws(' ', col('ADDRESS'), col('city_cleansed'),  col('STATE'), col('ZIPCODE')))
shaw_contact_df_temp = shaw_contact_df_temp.withColumn("address_cleansed", lower(regexp_replace(col("address_full"), "[%#\']", "")))

# COMMAND ----------

shaw_contact_df_temp = shaw_contact_df_temp.withColumn("id", monotonically_increasing_id())
shaw_contact_df_temp = shaw_contact_df_temp.na.fill(value=' ', subset=['address_cleansed']) 

# COMMAND ----------

udf_star_desc3     = udf(lambda x:func_ap(x),MapType(StringType(), StringType())) #list(tuples(string,string))
my_shaw_contact_df = shaw_contact_df_temp.withColumn("parsed_cols",udf_star_desc3(col("address_cleansed"))).select("parsed_cols", "id")

# COMMAND ----------

shaw_contact_parsed_add_df = my_shaw_contact_df.select("id", explode('parsed_cols')).groupBy("id").pivot('key').agg(first('value'))

# COMMAND ----------

shaw_contact_combined_add_df = shaw_contact_df_temp.join(shaw_contact_parsed_add_df, ['id'], 'left').drop('id')

# COMMAND ----------

shaw_contact_normalized_add_df = shaw_contact_combined_add_df
shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('StreetType_Cleansed', trim(col('StreetType')))

for pattern, replacement in zip(keyword, lookupid):
    shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('StreetType_Cleansed',regexp_replace('StreetType_Cleansed', pattern, replacement))

shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('StreetType_Cleansed', when(col('StreetType_Cleansed') == 'av', 'ave')\
                                                                                             .when(col('StreetType_Cleansed') == 'cr', 'cres')\
                                                                                             .when(col('StreetType_Cleansed') == 'bl', 'blvd')\
                                                                                             .otherwise(col('StreetType_Cleansed')))


shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('StreetDirection_Cleansed', 
                             when((col('StreetDirection') == 'nord') 
                                  | (col('StreetDirection') == 'north'), 'n')
                            .when((col('StreetDirection') == 'o') 
                                  | (col('StreetDirection') == 'ouest') | (col('StreetDirection') == 'west'), 'w')
                            .when((col('StreetDirection') == 'est') 
                                  | (col('StreetDirection') == 'east'), 'e')
                            .when((col('StreetDirection') == 'sud') 
                                  | (col('StreetDirection') == 'south'), 's')
                            .when((col('StreetDirection') == 'nord ouest') 
                                  | (col('StreetDirection') == 'northwest') 
                                  | (col('StreetDirection') == 'north west') 
                                  | (col('StreetDirection') == 'no'), 'nw')
                            .when((col('StreetDirection') == 'nord est') 
                                  | (col('StreetDirection') == 'northeast')
                                  | (col('StreetDirection') == 'north east'), 'ne')
                            .when((col('StreetDirection') == 'sud ouest') 
                                  | (col('StreetDirection') == 'southwest') 
                                  | (col('StreetDirection') == 'south west') 
                                  | (col('StreetDirection') == 'so'), 'sw')
                            .when((col('StreetDirection') == 'sud est') 
                                  | (col('StreetDirection') == 'southeast') 
                                  | (col('StreetDirection') == 'south east'), 'se')
                            .otherwise(regexp_replace(
                                regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace
                                   (
                                      col('StreetDirection'), 'north', 'n')
                                     , 'south', 's')
                                     , 'nord', 'n')
                                     , 'sud', 's')
                                     , 'east', 'e')
                                     , 'west', 'w')
                                     , 'est', 'e')
                                     , 'ouest', 'w')
                                   ))

shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('PostalBox_Cleansed', 
                             when((col('PostalBox') == 'NULL POSTAL BOX') | (col('PostalBox') == 'NULL BOX'), 'PO BOX')
                            .when(col('PostalBox').like('%POSTAL BOX'), 'PO BOX')
                            .when(col('PostalBox').like('%PO BOX'), 'PO BOX')
                            .when(col('PostalBox') == 'BOX' , 'PO BOX')
                            .when(col('PostalBox') == 'P O BOX' , 'PO BOX')
                            .when(col('PostalBox') == 'PLEASE SPECIFY BOX' , 'PO BOX')
                            .when((col('PostalBox') == 'BOX BOX') | (col('PostalBox') == 'PO BOX BOX'), 'PO BOX')
                            .otherwise(col('PostalBox')))

shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('Province_Cleansed', 
                             when(shaw_contact_normalized_add_df.state.isin(states_lookupid), 
                                  shaw_contact_normalized_add_df.state) 
                            .when((shaw_contact_normalized_add_df.state.isin('pq','qu','qb')), 'qc') 
                            .when((shaw_contact_normalized_add_df.state.isin('nf','nfl')), 'nl') 
                            .when((shaw_contact_normalized_add_df.state == 'yk'), 'yt') 
                            .when((shaw_contact_normalized_add_df.state == 'sa'), 'sk') 
                            .when((shaw_contact_normalized_add_df.state == 'nw'), 'nt') 
                            .otherwise(shaw_contact_normalized_add_df.state))

shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('PostalCode_Cleansed',                                   
                                                                           when(col('Province_Cleansed').isin('ns','nl','nt','qc','bc','mb','nu',
                                                                                                              'sk','on','ab','pe','yt','nb'), 
                                                                                regexp_replace(col('PostalCode'), " ", "")) 
                                                                           .otherwise(col('PostalCode')))

shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('mailing_address_no_zipcode_dq', \
                                     lower(trim(concat((when(col('UnitNumber').isNull(),"")
                                                    .otherwise(concat(col('UnitNumber'), lit(' ')))),\
                                                   (when(col('BuildingNumber').isNull(), "")
                                                    .otherwise(concat(col('BuildingNumber'), lit(' ')))), \
                                                   (when(col('StreetNumber').isNull(), "")
                                                    .otherwise(concat(col('StreetNumber'), lit(' ')))), \
                                                   (when(col('StreetName').isNull(), "")
                                                    .otherwise(concat(col('StreetName'), lit(' ')))), \
                                                   (when(col('StreetType_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('StreetType_Cleansed'), lit(' ')))), \
                                                   (when(col('PostalBox_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('PostalBox_Cleansed'), lit(' ')))), \
                                                   (when(col('PostalBoxNumber').isNull(), "")
                                                    .otherwise(concat(col('PostalBoxNumber'), lit(' ')))), \
                                                   (when(col('Station').isNull(), "")
                                                    .otherwise(concat(col('Station'), lit(' ')))), \
                                                   (when(col('StationNumber').isNull(), "")
                                                    .otherwise(concat(col('StationNumber'), lit(' ')))), \
                                                   (when(col('RuralRoute').isNull(), "")
                                                    .otherwise(concat(col('RuralRoute'), lit(' ')))), \
                                                   (when(col('RuralRouteNumber').isNull(), "")
                                                    .otherwise(concat(col('RuralRouteNumber'), lit(' ')))), \
                                                   (when(col('AdditionalInfo').isNull(), "")
                                                    .otherwise(concat(col('AdditionalInfo'), lit(' ')))), \
                                                   (when(col('Municipality').isNull(), "")
                                                    .otherwise(concat(col('Municipality'), lit(' ')))), \
                                                   (when(col('Province_Cleansed').isNull(), "")
                                                    .otherwise(col('Province_Cleansed'))) \
                                                                         ) \
                                                                ) \
                                                            ) \
                              )

shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('mailing_address_no_zipcode', \
                                     lower(trim(concat((when(col('UnitNumber').isNull(),"")
                                                     .otherwise(concat(col('UnitNumber'), lit(' ')))),\
                                                   (when(col('BuildingNumber').isNull(), "")
                                                    .otherwise(concat(col('BuildingNumber'), lit(' ')))), \
                                                   (when(col('StreetNumber').isNull(), "")
                                                    .otherwise(concat(col('StreetNumber'), lit(' ')))), \
                                                   (when(col('StreetName').isNull(), "")
                                                    .otherwise(concat(col('StreetName'), lit(' ')))), \
                                                   (when(col('StreetType_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('StreetType_Cleansed'), lit(' ')))), \
                                                   (when(col('StreetDirection_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('StreetDirection_Cleansed'), lit(' ')))), \
                                                   (when(col('PostalBox_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('PostalBox_Cleansed'), lit(' ')))), \
                                                   (when(col('PostalBoxNumber').isNull(), "")
                                                    .otherwise(concat(col('PostalBoxNumber'), lit(' ')))), \
                                                   (when(col('Station').isNull(), "")
                                                    .otherwise(concat(col('Station'), lit(' ')))), \
                                                   (when(col('StationNumber').isNull(), "")
                                                    .otherwise(concat(col('StationNumber'), lit(' ')))), \
                                                   (when(col('RuralRoute').isNull(), "")
                                                    .otherwise(concat(col('RuralRoute'), lit(' ')))), \
                                                   (when(col('RuralRouteNumber').isNull(), "")
                                                    .otherwise(concat(col('RuralRouteNumber'), lit(' ')))), \
                                                   (when(col('AdditionalInfo').isNull(), "")
                                                    .otherwise(concat(col('AdditionalInfo'), lit(' ')))), \
                                                   (when(col('Municipality').isNull(), "")
                                                    .otherwise(concat(col('Municipality'), lit(' ')))), \
                                                   (when(col('Province_Cleansed').isNull(), "")
                                                    .otherwise(col('Province_Cleansed'))) \
                                                                         ) \
                                                                ) \
                                                            ) \
                              )

shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('mailing_address_full', \
                                             lower(trim(concat((when(col('UnitNumber').isNull(),"")
                                                                .otherwise(concat(col('UnitNumber'), lit(' ')))),\
                                                               (when(col('BuildingNumber').isNull(), "")
                                                                .otherwise(concat(col('BuildingNumber'), lit(' ')))), \
                                                               (when(col('StreetNumber').isNull(), "") \
                                                                .otherwise(concat(trim(col('StreetNumber')), lit(' ')))), \
                                                               (when(col('StreetName').isNull(), "") \
                                                                .otherwise(concat(trim(col('StreetName')), lit(' ')))), \
                                                               (when(col('StreetType_Cleansed').isNull(), "") \
                                                                .otherwise(concat(trim(col('StreetType_Cleansed')), lit(' ')))), \
                                                               (when(col('StreetDirection_Cleansed').isNull(), "") \
                                                                .otherwise(concat(trim(col('StreetDirection_Cleansed')), lit(' ')))), \
                                                               (when(col('PostalBox_Cleansed').isNull(), "") \
                                                                .otherwise(concat(trim(col('PostalBox_Cleansed')), lit(' ')))), \
                                                               (when(col('PostalBoxNumber').isNull(), "") \
                                                                .otherwise(concat(trim(col('PostalBoxNumber')), lit(' ')))), \
                                                               (when(col('Station').isNull(), "") \
                                                                .otherwise(concat(trim(col('Station')), lit(' ')))), \
                                                               (when(col('StationNumber').isNull(), "") \
                                                                .otherwise(concat(trim(col('StationNumber')), lit(' ')))), \
                                                               (when(col('RuralRoute').isNull(), "") \
                                                                .otherwise(concat(trim(col('RuralRoute')), lit(' ')))), \
                                                               (when(col('RuralRouteNumber').isNull(), "") \
                                                                .otherwise(concat(trim(col('RuralRouteNumber')), lit(' ')))), \
                                                               (when(col('AdditionalInfo').isNull(), "")
                                                                .otherwise(concat(col('AdditionalInfo'), lit(' ')))), \
                                                               (when(col('Municipality').isNull(), "")
                                                                .otherwise(concat(trim(col('Municipality')), lit(' ')))), \
                                                               (when(col('Province_Cleansed').isNull(), "")
                                                                .otherwise(concat(trim(col('Province_Cleansed')), lit(' ')))), \
                                                               (when(col('PostalCode_Cleansed').isNull(), "")
                                                                .otherwise(trim(col('PostalCode_Cleansed')))) \
                                                                         ) \
                                                                ) \
                                                            ) \
                              )

shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('mailing_address_no_zipcode_dq', 
                                                                           when(col('mailing_address_no_zipcode_dq')=='', col('address'))
                                                                           .otherwise(col('mailing_address_no_zipcode_dq')))

shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('mailing_address_no_zipcode', 
                                                                           when(col('mailing_address_no_zipcode')=='', col('address'))
                                                                           .otherwise(col('mailing_address_no_zipcode')))

shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('mailing_address_full', 
                                                                           when(col('mailing_address_full')=='', col('address'))
                                                                           .otherwise(col('mailing_address_full')))

# COMMAND ----------

for c_name in phone_cols:
    c = '{}{}'.format('dq_', c_name)
    shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn(c, when(length(col(c_name))<4, lit(None)).otherwise(col(c_name)))\
                                                                   .withColumn(c, when(col(c).rlike("|".join(filler_phone)), lit(None)).otherwise(col(c)))

# COMMAND ----------

shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('dq_e_mail', when(length(substring_index(col('e_mail'), '@', 1))>63, lit(None)).otherwise(col('e_mail')))\
                                                               .withColumn('dq_e_mail', regexp_replace('dq_e_mail', '[(?+)].*@', '@'))\
                                                               .withColumn('dq_e_mail', when(upper(col('dq_e_mail')).rlike("|".join(filler_email)), lit(None)).otherwise(col('dq_e_mail')))

# COMMAND ----------

for c_name in contact_cols:
    c = '{}{}'.format(c_name, '_cl')
    shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn(c, regexp_replace(translate(lower(trim(c_name)), french_char, eng_char), special_char, ""))\
                                                                   .withColumn(c, when((col(c)=='none') | (col(c)=='null') | (col(c) == '' ), lit(None)).otherwise(col(c)))

# COMMAND ----------

shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('dq_first_name_cl', when((col('first_name_cl')== 'unknown') | (col('first_name_cl')== 'blank'), lit(None)).otherwise(col('first_name_cl')))

shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('dq_last_name_cl', when((col('last_name_cl')== 'unknown'), lit(None)).otherwise(col('last_name_cl')))

# COMMAND ----------

shaw_contact_normalized_add_df = shaw_contact_normalized_add_df.withColumn('dq_full_name_cl', concat_ws(' ', col("dq_first_name_cl"), col("dq_last_name_cl")))    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wireline

# COMMAND ----------

shaw_consumer_wireline_account_df = shaw_consumer_wireline_account_df.select([col(x).alias(x.lower()) for x in shaw_consumer_wireline_account_df.columns])

# COMMAND ----------

shaw_wireline_account_df_temp = shaw_consumer_wireline_account_df
shaw_wireline_account_df_temp = shaw_wireline_account_df_temp.withColumn('ingestion_date', current_timestamp())

shaw_wireline_account_df_temp = shaw_wireline_account_df_temp.withColumn("service_city_cleansed", when((col("service_city").like("%\'%")) | (col("service_city").like('%-%')) | (col("service_city").like('% %')) | (col("service_city").like('%.%'))
                                                                                                       ,translate(col("service_city")," -'.",""))
                                                                         .otherwise(col("service_city")))

shaw_wireline_account_df_temp = shaw_wireline_account_df_temp.withColumn("service_address_temp", concat_ws(' ', col('service_address'), col('service_city_cleansed'), col('service_province'), col('service_postal_code')))

shaw_wireline_account_df_temp = shaw_wireline_account_df_temp.withColumn("service_address_cleansed", lower(regexp_replace(col("service_address_temp"), "[%#\']", "")))

# COMMAND ----------

shaw_wireline_account_df_temp = shaw_wireline_account_df_temp.withColumn("id", monotonically_increasing_id())
shaw_wireline_account_df_temp = shaw_wireline_account_df_temp.na.fill(value=' ', subset=['service_address_cleansed'])

# COMMAND ----------

udf_star_desc4              = udf(lambda x:func_ap(x),MapType(StringType(), StringType())) #list(tuples(string,string))
my_shaw_wireline_account_df = shaw_wireline_account_df_temp.withColumn("parsed_cols",udf_star_desc4(col("service_address_cleansed"))).select("parsed_cols", "id")

# COMMAND ----------

shaw_wireline_account_parsed_add_df = my_shaw_wireline_account_df.select("id", explode('parsed_cols')).groupBy("id").pivot('key').agg(first('value'))

# COMMAND ----------

shaw_wireline_account_combined_add_df = shaw_wireline_account_df_temp.join(shaw_wireline_account_parsed_add_df, ['id'], 'left').drop('id')

# COMMAND ----------

shaw_wireline_account_normalized_add_df = shaw_wireline_account_combined_add_df
shaw_wireline_account_normalized_add_df= shaw_wireline_account_normalized_add_df.withColumn('StreetType_Cleansed', trim(col('StreetType')))

for pattern, replacement in zip(keyword, lookupid):
    shaw_wireline_account_normalized_add_df = shaw_wireline_account_normalized_add_df.withColumn('StreetType_Cleansed',regexp_replace('StreetType_Cleansed', pattern, replacement))


shaw_wireline_account_normalized_add_df = shaw_wireline_account_normalized_add_df.withColumn('StreetType_Cleansed', when(col('StreetType_Cleansed') == 'av', 'ave')\
                                                                                             .when(col('StreetType_Cleansed') == 'cr', 'cres')\
                                                                                             .when(col('StreetType_Cleansed') == 'bl', 'blvd')\
                                                                                             .otherwise(col('StreetType_Cleansed')))

shaw_wireline_account_normalized_add_df = shaw_wireline_account_normalized_add_df.withColumn('StreetDirection_Cleansed', 
                             when((col('StreetDirection') == 'nord') 
                                  | (col('StreetDirection') == 'north'), 'n')
                            .when((col('StreetDirection') == 'o') 
                                  | (col('StreetDirection') == 'ouest') | (col('StreetDirection') == 'west'), 'w')
                            .when((col('StreetDirection') == 'est') 
                                  | (col('StreetDirection') == 'east'), 'e')
                            .when((col('StreetDirection') == 'sud') 
                                  | (col('StreetDirection') == 'south'), 's')
                            .when((col('StreetDirection') == 'nord ouest') 
                                  | (col('StreetDirection') == 'northwest') 
                                  | (col('StreetDirection') == 'north west') 
                                  | (col('StreetDirection') == 'no'), 'nw')
                            .when((col('StreetDirection') == 'nord est') 
                                  | (col('StreetDirection') == 'northeast')
                                  | (col('StreetDirection') == 'north east'), 'ne')
                            .when((col('StreetDirection') == 'sud ouest') 
                                  | (col('StreetDirection') == 'southwest') 
                                  | (col('StreetDirection') == 'south west') 
                                  | (col('StreetDirection') == 'so'), 'sw')
                            .when((col('StreetDirection') == 'sud est') 
                                  | (col('StreetDirection') == 'southeast') 
                                  | (col('StreetDirection') == 'south east'), 'se')
                            .otherwise(regexp_replace(
                                regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace
                                   (
                                      col('StreetDirection'), 'north', 'n')
                                     , 'south', 's')
                                     , 'nord', 'n')
                                     , 'sud', 's')
                                     , 'east', 'e')
                                     , 'west', 'w')
                                     , 'est', 'e')
                                     , 'ouest', 'w')
                                   ))

shaw_wireline_account_normalized_add_df = shaw_wireline_account_normalized_add_df.withColumn('PostalBox_Cleansed', 
                             when((col('PostalBox') == 'NULL POSTAL BOX') | (col('PostalBox') == 'NULL BOX'), 'PO BOX')
                            .when(col('PostalBox').like('%POSTAL BOX'), 'PO BOX')
                            .when(col('PostalBox').like('%PO BOX'), 'PO BOX')
                            .when(col('PostalBox') == 'BOX' , 'PO BOX')
                            .when(col('PostalBox') == 'P O BOX' , 'PO BOX')
                            .when(col('PostalBox') == 'PLEASE SPECIFY BOX' , 'PO BOX')
                            .when((col('PostalBox') == 'BOX BOX') | (col('PostalBox') == 'PO BOX BOX'), 'PO BOX')
                            .otherwise(col('PostalBox'))) 

shaw_wireline_account_normalized_add_df = shaw_wireline_account_normalized_add_df.withColumn('Province_Cleansed', 
                             when(shaw_wireline_account_normalized_add_df.Province.isin(states_lookupid), shaw_wireline_account_normalized_add_df.Province) 
                            .when((shaw_wireline_account_normalized_add_df.Province.isin('pq','qu','qb')), 'qc') 
                            .when((shaw_wireline_account_normalized_add_df.Province.isin('nf','nfl')), 'nl') 
                            .when((shaw_wireline_account_normalized_add_df.Province == 'yk'), 'yt') 
                            .when((shaw_wireline_account_normalized_add_df.Province == 'sa'), 'sk') 
                            .when((shaw_wireline_account_normalized_add_df.Province == 'nw'), 'nt') 
                            .otherwise(shaw_wireline_account_normalized_add_df.Province))

shaw_wireline_account_normalized_add_df = shaw_wireline_account_normalized_add_df.withColumn('PostalCode_Cleansed',                                   
                                                                           when(col('Province_Cleansed').isin('ns','nl','nt','qc','bc','mb','nu',
                                                                                                              'sk','on','ab','pe','yt','nb'), 
                                                                                regexp_replace(col('PostalCode'), " ", "")) 
                                                                           .otherwise(col('PostalCode')))

shaw_wireline_account_normalized_add_df = shaw_wireline_account_normalized_add_df.withColumn('service_address_no_zipcode_dq', \
                                 lower(trim(concat((when(col('UnitNumber').isNull(),"")
                                                    .otherwise(concat(col('UnitNumber'), lit(' ')))),\
                                                   (when(col('BuildingNumber').isNull(), "")
                                                    .otherwise(concat(col('BuildingNumber'), lit(' ')))), \
                                                   (when(col('StreetNumber').isNull(), "")
                                                    .otherwise(concat(col('StreetNumber'), lit(' ')))), \
                                                   (when(col('StreetName').isNull(), "")
                                                    .otherwise(concat(col('StreetName'), lit(' ')))), \
                                                   (when(col('StreetType_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('StreetType_Cleansed'), lit(' ')))), \
                                                   (when(col('PostalBox_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('PostalBox_Cleansed'), lit(' ')))), \
                                                   (when(col('PostalBoxNumber').isNull(), "")
                                                    .otherwise(concat(col('PostalBoxNumber'), lit(' ')))), \
                                                   (when(col('StationNumber').isNull(), "")
                                                    .otherwise(concat(col('StationNumber'), lit(' ')))), \
                                                   (when(col('RuralRoute').isNull(), "")
                                                    .otherwise(concat(col('RuralRoute'), lit(' ')))), \
                                                   (when(col('RuralRouteNumber').isNull(), "")
                                                    .otherwise(concat(col('RuralRouteNumber'), lit(' ')))), \
                                                   (when(col('AdditionalInfo').isNull(), "")
                                                    .otherwise(concat(col('AdditionalInfo'), lit(' ')))), \
                                                   (when(col('Municipality').isNull(), "")
                                                    .otherwise(concat(col('Municipality'), lit(' ')))), \
                                                   (when(col('Province_Cleansed').isNull(), "")
                                                    .otherwise(col('Province_Cleansed'))) \
                                                                         ) \
                                                                ) \
                                                            ) \
                              )

shaw_wireline_account_normalized_add_df = shaw_wireline_account_normalized_add_df.withColumn('service_address_no_zipcode', \
                                 lower(trim(concat((when(col('UnitNumber').isNull(),"")
                                                     .otherwise(concat(col('UnitNumber'), lit(' ')))),\
                                                   (when(col('BuildingNumber').isNull(), "")
                                                    .otherwise(concat(col('BuildingNumber'), lit(' ')))), \
                                                   (when(col('StreetNumber').isNull(), "")
                                                    .otherwise(concat(col('StreetNumber'), lit(' ')))), \
                                                   (when(col('StreetName').isNull(), "")
                                                    .otherwise(concat(col('StreetName'), lit(' ')))), \
                                                   (when(col('StreetType_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('StreetType_Cleansed'), lit(' ')))), \
                                                   (when(col('StreetDirection_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('StreetDirection_Cleansed'), lit(' ')))), \
                                                   (when(col('PostalBox_Cleansed').isNull(), "")
                                                    .otherwise(concat(col('PostalBox_Cleansed'), lit(' ')))), \
                                                   (when(col('PostalBoxNumber').isNull(), "")
                                                    .otherwise(concat(col('PostalBoxNumber'), lit(' ')))), \
                                                   (when(col('StationNumber').isNull(), "")
                                                    .otherwise(concat(col('StationNumber'), lit(' ')))), \
                                                   (when(col('RuralRoute').isNull(), "")
                                                    .otherwise(concat(col('RuralRoute'), lit(' ')))), \
                                                   (when(col('RuralRouteNumber').isNull(), "")
                                                    .otherwise(concat(col('RuralRouteNumber'), lit(' ')))), \
                                                   (when(col('AdditionalInfo').isNull(), "")
                                                    .otherwise(concat(col('AdditionalInfo'), lit(' ')))), \
                                                   (when(col('Municipality').isNull(), "")
                                                    .otherwise(concat(col('Municipality'), lit(' ')))), \
                                                   (when(col('Province_Cleansed').isNull(), "")
                                                    .otherwise(col('Province_Cleansed'))) \
                                                                         ) \
                                                                ) \
                                                            ) \
                              )

shaw_wireline_account_normalized_add_df = shaw_wireline_account_normalized_add_df.withColumn('service_address_full', \
                                                       lower(trim(concat((when(col('UnitNumber').isNull(),"")
                                                                .otherwise(concat(col('UnitNumber'), lit(' ')))),\
                                                               (when(col('BuildingNumber').isNull(), "")
                                                                .otherwise(concat(col('BuildingNumber'), lit(' ')))), \
                                                               (when(col('StreetNumber').isNull(), "") \
                                                                .otherwise(concat(trim(col('StreetNumber')), lit(' ')))), \
                                                               (when(col('StreetName').isNull(), "") \
                                                                .otherwise(concat(trim(col('StreetName')), lit(' ')))), \
                                                               (when(col('StreetType_Cleansed').isNull(), "") \
                                                                .otherwise(concat(trim(col('StreetType_Cleansed')), lit(' ')))), \
                                                               (when(col('StreetDirection_Cleansed').isNull(), "") \
                                                                .otherwise(concat(trim(col('StreetDirection_Cleansed')), lit(' ')))), \
                                                               (when(col('PostalBox_Cleansed').isNull(), "") \
                                                                .otherwise(concat(trim(col('PostalBox_Cleansed')), lit(' ')))), \
                                                               (when(col('PostalBoxNumber').isNull(), "") \
                                                                .otherwise(concat(trim(col('PostalBoxNumber')), lit(' ')))), \
                                                               (when(col('StationNumber').isNull(), "") \
                                                                .otherwise(concat(trim(col('StationNumber')), lit(' ')))), \
                                                               (when(col('RuralRoute').isNull(), "") \
                                                                .otherwise(concat(trim(col('RuralRoute')), lit(' ')))), \
                                                               (when(col('RuralRouteNumber').isNull(), "") \
                                                                .otherwise(concat(trim(col('RuralRouteNumber')), lit(' ')))), \
                                                               (when(col('AdditionalInfo').isNull(), "")
                                                                .otherwise(concat(col('AdditionalInfo'), lit(' ')))), \
                                                               (when(col('Municipality').isNull(), "")
                                                                .otherwise(concat(trim(col('Municipality')), lit(' ')))), \
                                                               (when(col('Province_Cleansed').isNull(), "")
                                                                .otherwise(concat(trim(col('Province_Cleansed')), lit(' ')))), \
                                                               (when(col('PostalCode_Cleansed').isNull(), "")
                                                                .otherwise(trim(col('PostalCode_Cleansed')))) \
                                                                         ) \
                                                                ) \
                                                            ) \
                              )

shaw_wireline_account_normalized_add_df = shaw_wireline_account_normalized_add_df.withColumn('service_address_no_zipcode_dq', 
                                                                           when(col('service_address_no_zipcode_dq')=='', col('service_address'))
                                                                           .otherwise(col('service_address_no_zipcode_dq')))

shaw_wireline_account_normalized_add_df = shaw_wireline_account_normalized_add_df.withColumn('service_address_no_zipcode', 
                                                                           when(col('service_address_no_zipcode')=='', col('service_address'))
                                                                           .otherwise(col('service_address_no_zipcode')))

shaw_wireline_account_normalized_add_df = shaw_wireline_account_normalized_add_df.withColumn('service_address_full', 
                                                                           when(col('service_address_full')=='', col('service_address'))
                                                                           .otherwise(col('service_address_full')))

# COMMAND ----------

for c_name in wln_cols:
    c = '{}{}'.format(c_name, '_cl')
    shaw_wireline_account_normalized_add_df = shaw_wireline_account_normalized_add_df.withColumn(c, regexp_replace(translate(lower(trim(c_name)), french_char, eng_char), special_char, ""))\
                                                                                     .withColumn(c, when((col(c)=='none') | (col(c)=='null') | (col(c) == ''), lit(None)).otherwise(col(c)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wireless

# COMMAND ----------

# MAGIC %md
# MAGIC #### Account

# COMMAND ----------

shaw_consumer_wireless_account_df = shaw_consumer_wireless_account_df.select([col(x).alias(x.lower()) for x in shaw_consumer_wireless_account_df.columns])
shaw_consumer_wireless_account_df = shaw_consumer_wireless_account_df.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

for c_name in wls_cols:
    c = '{}{}'.format(c_name, '_cl')
    shaw_consumer_wireless_account_df = shaw_consumer_wireless_account_df.withColumn(c, regexp_replace(translate(lower(trim(c_name)), french_char, eng_char), special_char, ""))\
                                                                         .withColumn(c, when((col(c)=='none') | (col(c)=='null') | (col(c) == '' ), lit(None)).otherwise(col(c)))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Service

# COMMAND ----------

shaw_consumer_wireless_service_df = shaw_consumer_wireless_service_df.select([col(x).alias(x.lower()) for x in shaw_consumer_wireless_service_df.columns])
shaw_consumer_wireless_service_df = shaw_consumer_wireless_service_df.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

for c_name in wls_ser_cols:
    c = '{}{}'.format(c_name, '_cl')
    shaw_consumer_wireless_service_df = shaw_consumer_wireless_service_df.withColumn(c, regexp_replace(translate(lower(trim(c_name)), french_char, eng_char), special_char, ""))\
                                                                         .withColumn(c, when((col(c)=='none') | (col(c)=='null') | (col(c) == '' ), lit(None)).otherwise(col(c)))

# COMMAND ----------

# MAGIC %md
# MAGIC # Store the Outputs

# COMMAND ----------

rogers_contact_normalized_add_df.write.parquet(cl_rogers_contact,mode='overwrite')
rogers_wireline_account_normalized_add_df.write.parquet(cl_rogers_wireline_account,mode='overwrite')
rogers_wireless_account_df.write.parquet(cl_rogers_wireless_account,mode='overwrite')
rogers_wireless_service_df.write.parquet(cl_rogers_wireless_service,mode='overwrite')

shaw_contact_normalized_add_df.write.parquet(cl_shaw_consumer_contact,mode='overwrite')
shaw_wireline_account_normalized_add_df.write.parquet(cl_shaw_consumer_wireline_account,mode='overwrite')
shaw_consumer_wireless_account_df.write.parquet(cl_shaw_consumer_wireless_account,mode='overwrite')
shaw_consumer_wireless_service_df.write.parquet(cl_shaw_consumer_wireless_service,mode='overwrite')
