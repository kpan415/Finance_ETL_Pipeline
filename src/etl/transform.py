# This script defines functions to transform data according to the specifications found in the mapping document.

from pyspark.sql.types import *
from pyspark.sql.functions import *


def transform_branch(branch):
    # convert phone number formatting to (xxx)xxx-xxxx
    branch = branch.withColumn('BRANCH_PHONE', concat(lit("("), substring(col('BRANCH_PHONE'), 1, 3), lit(")"), 
                            substring(col('BRANCH_PHONE'), 4, 3), lit("-"), substring(col('BRANCH_PHONE'), 7, 4)))

    # replace null values in branch_zip column with 99999, df.fillna() updates the df in-place
    branch.fillna({'BRANCH_ZIP': 99999})

    # change data types and rearrange columns
    branch = branch.select(branch["BRANCH_CODE"].cast(IntegerType()), 
                           "BRANCH_NAME", 
                           "BRANCH_STREET", 
                           "BRANCH_CITY", 
                           "BRANCH_STATE", 
                           branch["BRANCH_ZIP"].cast(IntegerType()), 
                           "BRANCH_PHONE", 
                           branch["LAST_UPDATED"].cast(TimestampType()))
    return branch


def transform_credit(credit):
    # add new column concatenating year-month-day
    credit = credit.withColumn('TIMEID', concat(col('YEAR'), lit('-'), col('MONTH'), lit('-'), col('DAY')).cast(DateType()))
    credit.show(5)

    # change data types and rearrange columns 
    credit = credit.select(credit['CREDIT_CARD_NO'].alias("CUST_CC_NO"),
                           credit['TIMEID'],
                           credit['CUST_SSN'].cast(IntegerType()),
                           credit['BRANCH_CODE'].cast(IntegerType()),
                           credit['TRANSACTION_TYPE'],
                           credit['TRANSACTION_VALUE'],
                           credit['TRANSACTION_ID'].cast(IntegerType()))
    return credit


def transform_customer(customer):
    # change the case of strings and convert phone number formatting to (xxx)xxx-xxxx
    customer = customer.withColumns({'FIRST_NAME': initcap(col('FIRST_NAME')),
                                     'MIDDLE_NAME': lower(col('MIDDLE_NAME')),
                                     'LAST_NAME': initcap(col('LAST_NAME')),
                                     'FULL_STREET_ADDRESS': concat(col('STREET_NAME'), lit(', '), col('APT_NO')),
                                     'CUST_PHONE': concat(lit('(XXX)'), col('CUST_PHONE')[0:3], lit('-'), col('CUST_PHONE')[4:7])}) # phone num is missing area code
    
    # change data types and rearrange columns
    customer = customer.select(col('SSN').cast(IntegerType()), 
                               'FIRST_NAME', 
                               'MIDDLE_NAME', 
                               'LAST_NAME', 
                               'CREDIT_CARD_NO', 
                               'FULL_STREET_ADDRESS', 
                              'CUST_CITY', 
                              'CUST_STATE', 
                              'CUST_COUNTRY', 
                              col('CUST_ZIP').cast(IntegerType()), 
                              col('CUST_PHONE').cast(StringType()),
                              'CUST_EMAIL', 
                              col('LAST_UPDATED').cast(TimestampType()))
    return customer


def transform_loan(loan): 
    # convert income column to title case
    loan = loan.withColumn('Income', initcap(col('Income')))
    return loan