# Databricks notebook source
# MAGIC %md ## Implementing Serviceability Data Quality and Address Standardization 
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
# MAGIC   <li>The input data models should be available in the Raw Zone</li>
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

keyword      = ['avenue', 'boulevard', 'boul', 'by-pass', 'centre', 'circle', 'circuit', 'concession', 'corners', 'court', 'crescent', 'cresent', 'crossing', 'cul-de-sac', 'diversion', 'drive', 'esplanade', 'estates', 'expressway', 'extension', 'freeway', 'gardens', 'glen', 'grounds', 'harbour', 'heights', 'highlands', 'highway', 'landing', 'limits', 'lookout', 'mountain', 'orchard', 'park', 'parkway', 'passage', 'pathway', 'place', 'plateau', 'point', 'private', 'promenade', 'range', 'ridge', 'road', 'route', 'square', 'street', 'subdivision', 'terrace', 'thicket', 'townline', 'turnabout', 'village', 'lane', 'close', 'loop', 'plaza', 'trail', 'st st', 'mount', 'chase', 'castle', 'gate', 'ct', 'grove', 'way']

lookupid     = ['ave', 'blvd', 'blvd', 'bypass', 'ctr', 'cir', 'circt', 'conc', 'crnrs', 'crt', 'cres', 'cres', 'cross', 'cds', 'divers', 'dr', 'espl', 'estate', 'expy', 'exten', 'fwy', 'gdns', 'glen&', 'grnds', 'harbr', 'hts', 'hghlds', 'hwy', 'landng', 'lmts', 'lkout', 'mtn', 'orch', 'pk', 'pky', 'pass', 'ptway', 'pl', 'plat', 'pt', 'pvt', 'prom', 'rg', 'ridge', 'rd', 'rte', 'sq', 'st', 'subdiv', 'terr', 'thick', 'tline', 'trnabt', 'villge', 'ln', 'cls', 'lp', 'pz', 'tr', 'st', 'mt', 'ch', 'cs', 'ga', 'crt', 'gr', 'wy']

french_char  = 'çáéíóúàèìòùñãõäëïöüÿâêîôûåøæœ'

eng_char     = 'caeiouaeiounaoaeiouyaeiouaoaeoe'

special_char = "[!#$%&\'()*+,-./:;<=>?@\\^_`{|}~]"

states_lookupid = ['ns','nl','nt','qc','bc','mb','nu','sk','on','ab','pe','yt','nb','al','ak','az','ar','ca','co','ct','de','dc','fl','ga','hi','id','il','in','ia','ks','ky','la','me','md','ma','mi','mn','ms','mo','mt','ne','nv','nh','nj','nm','ny','nc','nd','oh','ok','or','pa','ri','sc','sd','tn','tx','ut','vt','va','wa','wv','wi','wy','as','gu','mp','pr','vi']

# COMMAND ----------

#Column names for basic cleaning
geoaddress_cols     = ['level_type', 'level_number', 'street_number', 'street_number_suffix', 'street_name', 'street_type', 'street_type_suffix', 'city', 'geoaddress_province', 'postal_code', 'serviceability_address_cleansed', 'serviceability_address_no_zipcode_dq', 'serviceability_address_no_zipcode', 'serviceability_address_full']

wireless_cols       = ['PR', 'postal_code', 'serviceability_address_cleansed', 'serviceability_address_no_zipcode_dq', 'serviceability_address_no_zipcode', 'serviceability_address_full']

serviceability_cols = ['formatted_address', 'city', 'province_code', 'postal_code', 'serviceability_address_no_zipcode_dq', 'serviceability_address_no_zipcode', 'serviceability_address_full']

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

shaw_wireline_serviceability_df            = spark.read.format("delta").load(shaw_wireline_serviceability)

rogers_geoaddress_serviceability_df        = spark.read.format("delta").load(rogers_wireline_serviceability)
rogers_wireless_serviceability_df          = spark.read.format("delta").load(rogers_wireless_serviceability)

# Updating 'postalcode' to 'Postal_Code' to accommodate change in column name post-LD1
rogers_wireless_serviceability_df = rogers_wireless_serviceability_df.withColumnRenamed("postalcode","Postal_Code")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rogers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Geographic Address Serviceability

# COMMAND ----------

# MAGIC %md
# MAGIC Pre-processing input data for address parser

# COMMAND ----------

rogers_geoaddress_serviceability_df_temp = rogers_geoaddress_serviceability_df
rogers_geoaddress_serviceability_df_temp = rogers_geoaddress_serviceability_df_temp.withColumnRenamed("Province", "GeoAddress_Province")
rogers_geoaddress_serviceability_df_temp = rogers_geoaddress_serviceability_df_temp.withColumn('ingestion_date', current_timestamp())

rogers_geoaddress_serviceability_df_temp = rogers_geoaddress_serviceability_df_temp.withColumn("city_cleansed", when((col("city").like("%\'%"))| (col("city").like('%-%')) | (col("city").like('% %')) | (col("city").like('%.%')) ,translate(col("city")," -'.",""))
                                                           .otherwise(col("city")))

rogers_geoaddress_serviceability_df_temp = rogers_geoaddress_serviceability_df_temp.withColumn('address_cleansed', concat_ws(' ', col('level_type'), col('level_number'), col('street_number'), col('street_number_suffix'), col('street_name'), col('street_type'), col('street_type_suffix'), col('city_cleansed'), col('geoaddress_province'), col('postal_code')))

rogers_geoaddress_serviceability_df_temp = rogers_geoaddress_serviceability_df_temp.withColumn("serviceability_address_cleansed", lower(regexp_replace(col("address_cleansed"), "[%#\']", "")))

# COMMAND ----------

rogers_geoaddress_serviceability_df_temp = rogers_geoaddress_serviceability_df_temp.withColumn("id", monotonically_increasing_id())
rogers_geoaddress_serviceability_df_temp = rogers_geoaddress_serviceability_df_temp.na.fill(value=' ', subset=['serviceability_address_cleansed'])

# COMMAND ----------

#Create and apply UDF to address attribute to generate key-value pairs
udf_star_desc                        = udf(lambda x:func_ap(x),MapType(StringType(), StringType())) #list(tuples(string,string))
my_rogers_geoaddress_serviceability_df = rogers_geoaddress_serviceability_df_temp.withColumn("parsed_cols",udf_star_desc(col("serviceability_address_cleansed"))).select("parsed_cols", "id")

# COMMAND ----------

rogers_geoaddress_serviceability_parsed_add_df = my_rogers_geoaddress_serviceability_df.select("id", explode('parsed_cols')).groupBy("id").pivot('key').agg(first('value'))

# COMMAND ----------

rogers_geoaddress_serviceability_combined_add_df = rogers_geoaddress_serviceability_df_temp.join(rogers_geoaddress_serviceability_parsed_add_df, ['id']).drop('id')

# COMMAND ----------

rogers_geoaddress_serviceability_normalized_add_df = rogers_geoaddress_serviceability_combined_add_df

rogers_geoaddress_serviceability_normalized_add_df = rogers_geoaddress_serviceability_normalized_add_df.withColumn('Unit_Cleansed', when(lower(col('Unit')) == 'suite', None)\
                                                                                             .when(lower(col('Unit')) == 'apt', None)\
                                                                                             .otherwise(col('Unit')))

rogers_geoaddress_serviceability_normalized_add_df = rogers_geoaddress_serviceability_normalized_add_df.withColumn('StreetType_Cleansed', trim(col('StreetType')))

for pattern, replacement in zip(keyword, lookupid):
    rogers_geoaddress_serviceability_normalized_add_df = rogers_geoaddress_serviceability_normalized_add_df.withColumn('StreetType_Cleansed',regexp_replace('StreetType_Cleansed', pattern, replacement))

rogers_geoaddress_serviceability_normalized_add_df = rogers_geoaddress_serviceability_normalized_add_df.withColumn('StreetType_Cleansed', when(col('StreetType_Cleansed') == 'av', 'ave')\
                                                                                             .when(col('StreetType_Cleansed') == 'cr', 'cres')\
                                                                                             .when(col('StreetType_Cleansed') == 'bl', 'blvd')\
                                                                                             .otherwise(col('StreetType_Cleansed')))

rogers_geoaddress_serviceability_normalized_add_df = rogers_geoaddress_serviceability_normalized_add_df.withColumn('StreetDirection_Cleansed', 
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
                                                                               
rogers_geoaddress_serviceability_normalized_add_df = rogers_geoaddress_serviceability_normalized_add_df.withColumn('PostalBox_Cleansed', 
                             when((col('PostalBox') == 'null postal box') | (col('PostalBox') == 'null box'), 'po box')
                            .when(col('PostalBox').like('%postal box'), 'po box')
                            .when(col('PostalBox').like('%po box'), 'po box')
                            .when(col('PostalBox') == 'box' , 'po box')
                            .when(col('PostalBox') == 'p o box' , 'po box')
                            .when(col('PostalBox') == 'please specify box' , 'po box')
                            .when((col('PostalBox') == 'box box') | (col('PostalBox') == 'po box box'), 'po box')
                            .otherwise(col('PostalBox')))

rogers_geoaddress_serviceability_normalized_add_df = rogers_geoaddress_serviceability_normalized_add_df.withColumn('Province_Cleansed',
                                           when(rogers_geoaddress_serviceability_normalized_add_df.Province.isin(states_lookupid),
                                                rogers_geoaddress_serviceability_normalized_add_df.Province)
                                          .when(rogers_geoaddress_serviceability_normalized_add_df.GeoAddress_Province.isin(states_lookupid), 
                                                rogers_geoaddress_serviceability_normalized_add_df.GeoAddress_Province)
                                          .when((rogers_geoaddress_serviceability_normalized_add_df.Province.isin('pq','qu','qb')) | \
                                                (rogers_geoaddress_serviceability_normalized_add_df.GeoAddress_Province.isin('pq','qu','qb')), 'qc')
                                          .when((rogers_geoaddress_serviceability_normalized_add_df.Province.isin('nf','nfl')) | \
                                                (rogers_geoaddress_serviceability_normalized_add_df.GeoAddress_Province.isin('nf','nfl')), 'nl')
                                          .when((rogers_geoaddress_serviceability_normalized_add_df.Province == 'yk') 
                                                | (rogers_geoaddress_serviceability_normalized_add_df.GeoAddress_Province == 'yk'), 'yt')
                                          .when((rogers_geoaddress_serviceability_normalized_add_df.Province == 'sa') 
                                                | (rogers_geoaddress_serviceability_normalized_add_df.GeoAddress_Province == 'sa'), 'sk')
                                          .when((rogers_geoaddress_serviceability_normalized_add_df.Province == 'nw') 
                                                | (rogers_geoaddress_serviceability_normalized_add_df.GeoAddress_Province == 'nw'), 'nt')
                                          .when(rogers_geoaddress_serviceability_normalized_add_df.Province.isin('xx', '**'), None)
                                          .when(rogers_geoaddress_serviceability_normalized_add_df.Province.like('% xx'), 
                                                rogers_geoaddress_serviceability_normalized_add_df.Province.substr(lit(1),
                                                    length(rogers_geoaddress_serviceability_normalized_add_df.Province)-3))
                                          .otherwise(rogers_geoaddress_serviceability_normalized_add_df.Province))

rogers_geoaddress_serviceability_normalized_add_df = rogers_geoaddress_serviceability_normalized_add_df.withColumn('PostalCode_Cleansed', 
                             when(rogers_geoaddress_serviceability_normalized_add_df.Province_Cleansed.isin('ns','nl','nt','qc','bc','mb','nu','sk','on','ab','pe','yt','nb'), \
                                  regexp_replace(rogers_geoaddress_serviceability_normalized_add_df.PostalCode, " ", ""))
                               .otherwise(rogers_geoaddress_serviceability_normalized_add_df.PostalCode)
                              )

rogers_geoaddress_serviceability_normalized_add_df = rogers_geoaddress_serviceability_normalized_add_df.withColumn('serviceability_address_no_zipcode_dq', \
                                     lower(trim(concat((when(col('Unit_Cleansed').isNull(),"")
                                                    .otherwise(concat(col('Unit_Cleansed'), lit(' ')))),\
                                                   (when(col('UnitNumber').isNull(),"")
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

rogers_geoaddress_serviceability_normalized_add_df = rogers_geoaddress_serviceability_normalized_add_df.withColumn('serviceability_address_no_zipcode', \
                                     lower(trim(concat((when(col('Unit_Cleansed').isNull(),"")
                                                     .otherwise(concat(col('Unit_Cleansed'), lit(' ')))),\
                                                   (when(col('UnitNumber').isNull(),"")
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

rogers_geoaddress_serviceability_normalized_add_df = rogers_geoaddress_serviceability_normalized_add_df.withColumn('serviceability_address_full', \
                                             lower(trim(concat((when(col('Unit_Cleansed').isNull(),"")
                                                                .otherwise(concat(col('Unit_Cleansed'), lit(' ')))),\
                                                               (when(col('UnitNumber').isNull(),"")
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

# COMMAND ----------

for c_name in geoaddress_cols:
    c = '{}{}'.format(c_name, '_cl')
    rogers_geoaddress_serviceability_normalized_add_df = rogers_geoaddress_serviceability_normalized_add_df \
                                                              .withColumn(c, regexp_replace(translate(lower(trim(c_name)), french_char, eng_char), special_char, "")) \
                                                              .withColumn(c, when((col(c)=='none') | (col(c)=='null') | (col(c) == '' ), lit(None)).otherwise(col(c)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wireless Serviceability

# COMMAND ----------

# MAGIC %md
# MAGIC Pre-processing input data for address parser

# COMMAND ----------

rogers_wireless_serviceability_df_temp = rogers_wireless_serviceability_df
rogers_wireless_serviceability_df_temp = rogers_wireless_serviceability_df_temp.withColumn('ingestion_date', current_timestamp())
rogers_wireless_serviceability_df_temp = rogers_wireless_serviceability_df_temp.withColumn('address_cleansed', concat_ws(' ', col('pr'), col('Postal_Code')))
rogers_wireless_serviceability_df_temp = rogers_wireless_serviceability_df_temp.withColumn("serviceability_address_cleansed", lower(regexp_replace(col("address_cleansed"), "[%#\']", "")))

# COMMAND ----------

rogers_wireless_serviceability_df_temp = rogers_wireless_serviceability_df_temp.withColumn("id", monotonically_increasing_id())
rogers_wireless_serviceability_df_temp = rogers_wireless_serviceability_df_temp.na.fill(value=' ', subset=['serviceability_address_cleansed'])

# COMMAND ----------

#Create and apply UDF to address attribute to generate key-value pairs
udf_star_desc2                       = udf(lambda x:func_ap(x),MapType(StringType(), StringType())) #list(tuples(string,string))
my_rogers_wireless_serviceability_df = rogers_wireless_serviceability_df_temp.withColumn("parsed_cols",udf_star_desc2(col("serviceability_address_cleansed"))).select("parsed_cols", "id")

# COMMAND ----------

rogers_wireless_serviceability_parsed_add_df = my_rogers_wireless_serviceability_df.select("id", explode('parsed_cols')).groupBy("id").pivot('key').agg(first('value'))

# COMMAND ----------

rogers_wireless_serviceability_combined_add_df = rogers_wireless_serviceability_df_temp.join(rogers_wireless_serviceability_parsed_add_df, ['id']).drop('id')

# COMMAND ----------

rogers_wireless_serviceability_normalized_add_df = rogers_wireless_serviceability_combined_add_df

rogers_wireless_serviceability_normalized_add_df = rogers_wireless_serviceability_normalized_add_df.withColumn('Province_Cleansed',
                                           when(rogers_wireless_serviceability_normalized_add_df.Province.isin(states_lookupid),
                                                rogers_wireless_serviceability_normalized_add_df.Province)
                                          .when(rogers_wireless_serviceability_normalized_add_df.pr.isin(states_lookupid), 
                                                rogers_wireless_serviceability_normalized_add_df.pr)
                                          .when((rogers_wireless_serviceability_normalized_add_df.Province.isin('pq','qu','qb')) | \
                                                (rogers_wireless_serviceability_normalized_add_df.pr.isin('pq','qu','qb')), 'qc')
                                          .when((rogers_wireless_serviceability_normalized_add_df.Province.isin('nf','nfl')) | \
                                                (rogers_wireless_serviceability_normalized_add_df.pr.isin('nf','nfl')), 'nl')
                                          .when((rogers_wireless_serviceability_normalized_add_df.Province == 'yk') 
                                                | (rogers_wireless_serviceability_normalized_add_df.pr == 'yk'), 'yt')
                                          .when((rogers_wireless_serviceability_normalized_add_df.Province == 'sa') 
                                                | (rogers_wireless_serviceability_normalized_add_df.pr == 'sa'), 'sk')
                                          .when((rogers_wireless_serviceability_normalized_add_df.Province == 'nw') 
                                                | (rogers_wireless_serviceability_normalized_add_df.pr == 'nw'), 'nt')
                                          .when(rogers_wireless_serviceability_normalized_add_df.Province.isin('xx', '**'), None)
                                          .when(rogers_wireless_serviceability_normalized_add_df.Province.like('% xx'), 
                                                rogers_wireless_serviceability_normalized_add_df.Province.substr(lit(1),
                                                    length(rogers_wireless_serviceability_normalized_add_df.Province)-3))
                                          .otherwise(rogers_wireless_serviceability_normalized_add_df.Province))

rogers_wireless_serviceability_normalized_add_df = rogers_wireless_serviceability_normalized_add_df.withColumn('PostalCode_Cleansed', 
                             when(rogers_wireless_serviceability_normalized_add_df.Province_Cleansed.isin('ns','nl','nt','qc','bc','mb','nu','sk','on','ab','pe','yt','nb'), \
                                  regexp_replace(rogers_wireless_serviceability_normalized_add_df.PostalCode, " ", ""))
                               .otherwise(rogers_wireless_serviceability_normalized_add_df.PostalCode)
                              )

rogers_wireless_serviceability_normalized_add_df = rogers_wireless_serviceability_normalized_add_df.withColumn('serviceability_address_no_zipcode_dq', \
                                     lower(trim(concat((when(col('Building').isNull(),"")
                                                    .otherwise(concat(col('Building'), lit(' ')))),\
                                                   (when(col('BuildingNumber').isNull(), "")
                                                    .otherwise(concat(col('BuildingNumber'), lit(' ')))), \
                                                   (when(col('Province_Cleansed').isNull(), "")
                                                    .otherwise(col('Province_Cleansed'))) \
                                                                     ) \
                                                            ) \
                                                        ) \
                                              )

rogers_wireless_serviceability_normalized_add_df = rogers_wireless_serviceability_normalized_add_df.withColumn('serviceability_address_no_zipcode', \
                                     lower(trim(concat((when(col('Building').isNull(),"")
                                                     .otherwise(concat(col('Building'), lit(' ')))),\
                                                   (when(col('BuildingNumber').isNull(), "")
                                                    .otherwise(concat(col('BuildingNumber'), lit(' ')))), \
                                                   (when(col('Province_Cleansed').isNull(), "")
                                                    .otherwise(col('Province_Cleansed'))) \
                                                               ) \
                                                      ) \
                                                  ) \
                                          )

rogers_wireless_serviceability_normalized_add_df = rogers_wireless_serviceability_normalized_add_df.withColumn('serviceability_address_full', \
                                             lower(trim(concat((when(col('Building').isNull(),"")
                                                                .otherwise(concat(col('Building'), lit(' ')))),\
                                                               (when(col('BuildingNumber').isNull(), "")
                                                                .otherwise(concat(col('BuildingNumber'), lit(' ')))), \
                                                               (when(col('Province_Cleansed').isNull(), "")
                                                                .otherwise(concat(trim(col('Province_Cleansed')), lit(' ')))), \
                                                               (when(col('PostalCode_Cleansed').isNull(), "")
                                                                .otherwise(trim(col('PostalCode_Cleansed')))) \
                                                                         ) \
                                                                ) \
                                                            ) \
                              ) 

# COMMAND ----------

for c_name in wireless_cols:
    c = '{}{}'.format(c_name, '_cl')
    rogers_wireless_serviceability_normalized_add_df = rogers_wireless_serviceability_normalized_add_df \
                                                              .withColumn(c, regexp_replace(translate(lower(trim(c_name)), french_char, eng_char), special_char, "")) \
                                                              .withColumn(c, when((col(c)=='none') | (col(c)=='null') | (col(c) == '' ), lit(None)).otherwise(col(c)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shaw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wireline Serviceability

# COMMAND ----------

shaw_wireline_serviceability_df = shaw_wireline_serviceability_df.select([col(x).alias(x.lower()) for x in shaw_wireline_serviceability_df.columns])

# COMMAND ----------

shaw_wireline_serviceability_df_temp = shaw_wireline_serviceability_df
shaw_wireline_serviceability_df_temp = shaw_wireline_serviceability_df_temp.withColumn('ingestion_date', current_timestamp())
shaw_wireline_serviceability_df_temp = shaw_wireline_serviceability_df_temp.withColumn("city_cleansed", when((col("CITY").like("%\'%")) | (col("CITY").like('%-%'))  | (col("CITY").like('% %')) | (col("CITY").like('%.%')) 
                                                                             ,translate(col("city")," -'.",""))
                                                       .otherwise(col("city")))

shaw_wireline_serviceability_df_temp = shaw_wireline_serviceability_df_temp.withColumn("address_cleansed", 
                                            trim(concat(when(col('FORMATTED_ADDRESS').isNull(), '').otherwise(col('FORMATTED_ADDRESS')), lit(' '), 
                                                   when(col('city_cleansed').isNull(), '').otherwise(col('city_cleansed')), lit(' '),  
                                                   when(col('PROVINCE_CODE').isNull(), '').otherwise(col('PROVINCE_CODE')), lit(' '), 
                                                   when(col('POSTAL_CODE').isNull(), '').otherwise(col('POSTAL_CODE'))
                                                  )))

shaw_wireline_serviceability_df_temp = shaw_wireline_serviceability_df_temp.withColumn("serviceability_address_cleansed", lower(regexp_replace(col("address_cleansed"), "[%#\']", "")))

# COMMAND ----------

shaw_wireline_serviceability_df_temp = shaw_wireline_serviceability_df_temp.withColumn("id", monotonically_increasing_id())
shaw_wireline_serviceability_df_temp = shaw_wireline_serviceability_df_temp.na.fill(value='', subset=['serviceability_address_cleansed']) 

# COMMAND ----------

udf_star_desc3     = udf(lambda x:func_ap(x),MapType(StringType(), StringType())) #list(tuples(string,string))
my_shaw_wireline_serviceability_df = shaw_wireline_serviceability_df_temp.withColumn("parsed_cols",udf_star_desc3(col("serviceability_address_cleansed"))).select("parsed_cols", "id")

# COMMAND ----------

shaw_wireline_serviceability_parsed_add_df = my_shaw_wireline_serviceability_df.select("id", explode('parsed_cols')).groupBy("id").pivot('key').agg(first('value'))

# COMMAND ----------

shaw_wireline_serviceability_combined_add_df = shaw_wireline_serviceability_df_temp.join(shaw_wireline_serviceability_parsed_add_df, ['id']).drop('id')

# COMMAND ----------

shaw_wireline_serviceability_normalized_add_df = shaw_wireline_serviceability_combined_add_df

shaw_wireline_serviceability_normalized_add_df = shaw_wireline_serviceability_normalized_add_df.withColumn('Unit_Cleansed', when(lower(col('Unit')) == 'suite', None)\
                                                                                             .when(lower(col('Unit')) == 'apt', None)\
                                                                                             .otherwise(col('Unit')))

shaw_wireline_serviceability_normalized_add_df = shaw_wireline_serviceability_normalized_add_df.withColumn('StreetType_Cleansed', trim(col('StreetType')))

for pattern, replacement in zip(keyword, lookupid):
    shaw_wireline_serviceability_normalized_add_df = shaw_wireline_serviceability_normalized_add_df.withColumn('StreetType_Cleansed',regexp_replace('StreetType_Cleansed', pattern, replacement))
    
shaw_wireline_serviceability_normalized_add_df = shaw_wireline_serviceability_normalized_add_df.withColumn('StreetType_Cleansed', when(col('StreetType_Cleansed') == 'av', 'ave')\
                                                                                             .when(col('StreetType_Cleansed') == 'cr', 'cres')\
                                                                                             .when(col('StreetType_Cleansed') == 'bl', 'blvd')\
                                                                                             .otherwise(col('StreetType_Cleansed')))

shaw_wireline_serviceability_normalized_add_df = shaw_wireline_serviceability_normalized_add_df.withColumn('StreetDirection_Cleansed', 
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

shaw_wireline_serviceability_normalized_add_df = shaw_wireline_serviceability_normalized_add_df.withColumn('PostalBox_Cleansed', 
                             when((col('PostalBox') == 'NULL POSTAL BOX') | (col('PostalBox') == 'NULL BOX'), 'PO BOX')
                            .when(col('PostalBox').like('%POSTAL BOX'), 'PO BOX')
                            .when(col('PostalBox').like('%PO BOX'), 'PO BOX')
                            .when(col('PostalBox') == 'BOX' , 'PO BOX')
                            .when(col('PostalBox') == 'P O BOX' , 'PO BOX')
                            .when(col('PostalBox') == 'PLEASE SPECIFY BOX' , 'PO BOX')
                            .when((col('PostalBox') == 'BOX BOX') | (col('PostalBox') == 'PO BOX BOX'), 'PO BOX')
                            .otherwise(col('PostalBox')))

shaw_wireline_serviceability_normalized_add_df = shaw_wireline_serviceability_normalized_add_df.withColumn('Province_Cleansed', 
                             when(shaw_wireline_serviceability_normalized_add_df.province_code.isin(states_lookupid), 
                                  shaw_wireline_serviceability_normalized_add_df.province_code) 
                            .when((shaw_wireline_serviceability_normalized_add_df.province_code.isin('pq','qu','qb')), 'qc') 
                            .when((shaw_wireline_serviceability_normalized_add_df.province_code.isin('nf','nfl')), 'nl') 
                            .when((shaw_wireline_serviceability_normalized_add_df.province_code == 'yk'), 'yt') 
                            .when((shaw_wireline_serviceability_normalized_add_df.province_code == 'sa'), 'sk') 
                            .when((shaw_wireline_serviceability_normalized_add_df.province_code == 'nw'), 'nt') 
                            .otherwise(shaw_wireline_serviceability_normalized_add_df.province_code))

shaw_wireline_serviceability_normalized_add_df = shaw_wireline_serviceability_normalized_add_df.withColumn('PostalCode_Cleansed',                                   
                                                                           when(col('Province_Cleansed').isin('ns','nl','nt','qc','bc','mb','nu',
                                                                                                              'sk','on','ab','pe','yt','nb'), 
                                                                                regexp_replace(col('PostalCode'), " ", "")) 
                                                                           .otherwise(col('PostalCode')))

shaw_wireline_serviceability_normalized_add_df = shaw_wireline_serviceability_normalized_add_df.withColumn('serviceability_address_no_zipcode_dq', \
                                     lower(trim(concat((when(col('Unit_Cleansed').isNull(),"")
                                                    .otherwise(concat(col('Unit_Cleansed'), lit(' ')))),\
                                                   (when(col('UnitNumber').isNull(),"")
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

shaw_wireline_serviceability_normalized_add_df = shaw_wireline_serviceability_normalized_add_df.withColumn('serviceability_address_no_zipcode', \
                                     lower(trim(concat((when(col('Unit_Cleansed').isNull(),"")
                                                     .otherwise(concat(col('Unit_Cleansed'), lit(' ')))),\
                                                   (when(col('UnitNumber').isNull(),"")
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

shaw_wireline_serviceability_normalized_add_df = shaw_wireline_serviceability_normalized_add_df.withColumn('serviceability_address_full', \
                                             lower(trim(concat((when(col('Unit_Cleansed').isNull(),"")
                                                                .otherwise(concat(col('Unit_Cleansed'), lit(' ')))),\
                                                               (when(col('UnitNumber').isNull(),"")
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

# COMMAND ----------

for c_name in serviceability_cols:
    c = '{}{}'.format(c_name, '_cl')
    shaw_wireline_serviceability_normalized_add_df = shaw_wireline_serviceability_normalized_add_df.withColumn(c, regexp_replace(translate(lower(trim(c_name)), french_char, eng_char), special_char, ""))\
                                                                   .withColumn(c, when((col(c)=='none') | (col(c)=='null') | (col(c) == '' ), lit(None)).otherwise(col(c)))

# COMMAND ----------

# MAGIC %md
# MAGIC # Store the Outputs

# COMMAND ----------

rogers_geoaddress_serviceability_normalized_add_df.write.parquet(cl_rogers_wireline_serviceability,mode='overwrite')
rogers_wireless_serviceability_normalized_add_df.write.parquet(cl_rogers_wireless_serviceability,mode='overwrite')

shaw_wireline_serviceability_normalized_add_df.write.parquet(cl_shaw_wireline_serviceability,mode='overwrite')

# COMMAND ----------


