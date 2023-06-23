# Databricks notebook source
# MAGIC %md ## Ruleset Reference Table
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Description:
# MAGIC
# MAGIC This notebook builds the reference table Ruleset that contains all the rules and related attributes.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %run ../2.Common/ADLSDesignedPaths

# COMMAND ----------

cols = ['RULESET_ID', 'RULESET_NAME', 'RULESET_PRIORITY', 'RULESET_TYPE','IS_CUSTOMER_MATCH', 'IS_ADDRESS_MATCH', 'RULESET_DESCRIPTION']
vals = [(1, 'CUSTOMER_MATCH_1',	'1', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Name, Email, Phone Number and Service Address'),
        (2, 'CUSTOMER_MATCH_2',	'2', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Name, Email, Phone Number and Mailing Address'),
        (3, 'CUSTOMER_MATCH_3',	'3', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Name, Email, Phone Number and Mailing/Service Address'),
        (4, 'CUSTOMER_MATCH_4',	'4', 'DETERMINISTIC', 'TRUE', 'FALSE', '100% match on Customer Name, Email and Phone Number'),
        (5, 'CUSTOMER_MATCH_5',	'5', 'DETERMINISTIC', 'TRUE', 'TRUE','100% match on Customer Name, Email and Service Address'),
        (6, 'CUSTOMER_MATCH_6',	'6', 'DETERMINISTIC', 'TRUE', 'TRUE','100% match on Customer Name, Email and Mailing Address'),
        (7, 'CUSTOMER_MATCH_7',	'7', 'DETERMINISTIC', 'TRUE', 'TRUE','100% match on Customer Name, Email and Mailing/Service Address'),
        (8, 'CUSTOMER_MATCH_8',	'8', 'DETERMINISTIC', 'TRUE', 'TRUE','100% match on Customer Name, Phone Number and Service Address'),
        (9, 'CUSTOMER_MATCH_9',	'9', 'DETERMINISTIC', 'TRUE', 'TRUE','100% match on Customer Name, Phone Number and Mailing Address'),
        (10, 'CUSTOMER_MATCH_10', '10', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Name, Phone Number and Mailing/Service Address'),
        (11, 'CUSTOMER_MATCH_11', '11', 'DETERMINISTIC', 'TRUE', 'FALSE', '100% match on Customer Name and Email'),
        (12, 'CUSTOMER_MATCH_12', '12', 'DETERMINISTIC', 'TRUE', 'FALSE', '100% match on Customer Name and Phone'),
        (13, 'CUSTOMER_MATCH_13', '13', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Name and Service Address'),
        (14, 'CUSTOMER_MATCH_14', '14', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Name and Mailing Address'),
        (15, 'CUSTOMER_MATCH_15', '15', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Name and Mailing/Service Address'),
        (16, 'FUZZY_CUSTOMER_MATCH_3', None, 'FUZZY', 'TRUE', 'TRUE', 'Fuzzy match on Customer Name, Email, Phone Number and Mailing/Service Address'),
        (17, 'FUZZY_CUSTOMER_MATCH_5', None,  'FUZZY','TRUE', 'FALSE', 'Fuzzy match on Customer Name, Email and Phone Number'),
        (18, 'FUZZY_CUSTOMER_MATCH_7', None,  'FUZZY','TRUE', 'TRUE', 'Fuzzy match on Customer Name, Email and Mailing/Service Address'),
        (19, 'FUZZY_CUSTOMER_MATCH_10', None,  'FUZZY','TRUE', 'TRUE', 'Fuzzy match on Customer Name, Phone Number and Mailing/Service Address'),
        (20, 'FUZZY_CUSTOMER_MATCH_11', None, 'FUZZY','TRUE', 'FALSE', 'Fuzzy match on Customer Name and Email'),
        (21, 'FUZZY_CUSTOMER_MATCH_12', None, 'FUZZY','TRUE', 'FALSE', 'Fuzzy match on Customer Name and Phone'),
        (22, 'FUZZY_CUSTOMER_MATCH_15', None, 'FUZZY','TRUE', 'TRUE', 'Fuzzy match on Customer Name and Mailing/Service Address'),
        (23, 'PROB_CUSTOMER_MATCH_3', None,'PROBABILISTIC', 'TRUE', 'TRUE', 'Probabilistic match on Customer Name, Email, Phone Number and Mailing/Service Address'),
        (24, 'PROB_CUSTOMER_MATCH_5', None,'PROBABILISTIC', 'TRUE', 'FALSE', 'Probabilistic match on Customer Name, Email and Phone Number'),
        (25, 'PROB_CUSTOMER_MATCH_7', None,'PROBABILISTIC', 'TRUE', 'TRUE', 'Probabilistic match on Customer Name, Email and Mailing/Service Address'),
        (26, 'PROB_CUSTOMER_MATCH_10', None,'PROBABILISTIC', 'TRUE', 'TRUE', 'Probabilistic match on Customer Name, Phone Number and Mailing/Service Address'),
        (27, 'PROB_CUSTOMER_MATCH_11', None,'PROBABILISTIC', 'TRUE', 'FALSE', 'Probabilistic match on Customer Name and Email'),
        (28, 'PROB_CUSTOMER_MATCH_12',None,'PROBABILISTIC', 'TRUE', 'FALSE', 'Probabilistic match on Customer Name and Phone'),
        (29, 'PROB_CUSTOMER_MATCH_15', None,'PROBABILISTIC', 'TRUE', 'TRUE', 'Probabilistic match on Customer Name and Mailing/Service Address'),
	    (30, 'ADDRESS_MATCH_1', '43', 'DETERMINISTIC', 'FALSE', 'TRUE',	'100% match on Service Address'),
        (31, 'ADDRESS_MATCH_2', '44', 'DETERMINISTIC', 'FALSE', 'TRUE',	'100% match on Mailing Address'),
        (32, 'ADDRESS_MATCH_3', '45', 'DETERMINISTIC', 'FALSE', 'TRUE',	'100% match on Mailing/Service Address'), 
       (33, 'FUZZY_ADDRESS_MATCH_3', None, 'FUZZY', 'FALSE', 'TRUE', 'Fuzzy match on Mailing/Service Address'),
       (34, 'PROB_ADDRESS_MATCH_3', None, 'PROBABILISTIC', 'FALSE', 'TRUE',	'Probabilistic match on Mailing/Service Address'),
        (35, 'CUSTOMER_MATCH_16',	'16', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Last Name, Email, Phone Number and Service Address'),
        (36, 'CUSTOMER_MATCH_17',	'17', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Last Name, Email, Phone Number and Mailing Address'),
        (37, 'CUSTOMER_MATCH_18',	'18', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Last Name, Email, Phone Number and Mailing/Service Address'),
        (38, 'CUSTOMER_MATCH_19',	'19', 'DETERMINISTIC', 'TRUE', 'FALSE', '100% match on Customer Last Name, Email and Phone Number'),
        (39, 'CUSTOMER_MATCH_20',	'20', 'DETERMINISTIC', 'TRUE', 'TRUE','100% match on Customer Last Name, Email and Service Address'),
        (40, 'CUSTOMER_MATCH_21',	'21', 'DETERMINISTIC', 'TRUE', 'TRUE','100% match on Customer Last Name, Email and Mailing Address'),
        (41, 'CUSTOMER_MATCH_22',	'22', 'DETERMINISTIC', 'TRUE', 'TRUE','100% match on Customer Last Name, Email and Mailing/Service Address'),
        (42, 'CUSTOMER_MATCH_23',	'23', 'DETERMINISTIC', 'TRUE', 'TRUE','100% match on Customer Last Name, Phone Number and Service Address'),
        (43, 'CUSTOMER_MATCH_24',	'24', 'DETERMINISTIC', 'TRUE', 'TRUE','100% match on Customer Last Name, Phone Number and Mailing Address'),
        (44, 'CUSTOMER_MATCH_25', '25', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Last Name, Phone Number and Mailing/Service Address'),
        (45, 'CUSTOMER_MATCH_26', '26', 'DETERMINISTIC', 'TRUE', 'FALSE', '100% match on Customer Last Name and Email'),
        (46, 'CUSTOMER_MATCH_27', '27', 'DETERMINISTIC', 'TRUE', 'FALSE', '100% match on Customer Last Name and Phone'),
        (47, 'CUSTOMER_MATCH_28', '28', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Last Name and Service Address'),
        (48, 'CUSTOMER_MATCH_29', '29', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Last Name and Mailing Address'),
        (49, 'CUSTOMER_MATCH_30', '30', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Last Name and Mailing/Service Address'),
        (50, 'CUSTOMER_MATCH_31',	'31', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Email, Phone Number and Service Address'),
        (51, 'CUSTOMER_MATCH_32',	'32', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Email, Phone Number and Mailing Address'),
        (52, 'CUSTOMER_MATCH_33',	'33', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Email, Phone Number and Mailing/Service Address'),
        (53, 'CUSTOMER_MATCH_34',	'34', 'DETERMINISTIC', 'TRUE', 'FALSE', '100% match on Customer Email and Phone Number'),
        (54, 'CUSTOMER_MATCH_35',	'35', 'DETERMINISTIC', 'TRUE', 'TRUE','100% match on Customer Email and Service Address'),
        (55, 'CUSTOMER_MATCH_36',	'36', 'DETERMINISTIC', 'TRUE', 'TRUE','100% match on Customer Email and Mailing Address'),
        (56, 'CUSTOMER_MATCH_37',	'37', 'DETERMINISTIC', 'TRUE', 'TRUE','100% match on Customer Email and Mailing/Service Address'),
        (57, 'CUSTOMER_MATCH_38',	'38', 'DETERMINISTIC', 'TRUE', 'TRUE','100% match on Customer Phone Number and Service Address'),
        (58, 'CUSTOMER_MATCH_39',	'39', 'DETERMINISTIC', 'TRUE', 'TRUE','100% match on Customer Phone Number and Mailing Address'),
        (59, 'CUSTOMER_MATCH_40', '40', 'DETERMINISTIC', 'TRUE', 'TRUE', '100% match on Customer Phone Number and Mailing/Service Address'),
        (60, 'CUSTOMER_MATCH_41', '41', 'DETERMINISTIC', 'TRUE', 'FALSE', '100% match on Customer Email'),
        (61, 'CUSTOMER_MATCH_42', '42', 'DETERMINISTIC', 'TRUE', 'FALSE', '100% match on Customer Phone')
       ]
ruleset_df = spark.createDataFrame(vals, cols)

# COMMAND ----------

ruleset_table = ruleset_df.withColumn('RULESET_CREATED_TIMESTAMP', current_timestamp())\
                          .withColumn('RULESET_UPDATED_TIMESTAMP', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Storage

# COMMAND ----------

ruleset_table.write.parquet(ruleset, mode='overwrite')
