# This script defines functions to load transformed data into MySQL database.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.database.db_cnx import *


def load_data(ss, df, table_name):
    with connect_to_mysql() as cnx:
        cursor = cnx.cursor()
        # check if db is empty by counting the num of tables inside
        cursor.execute(
            "select count(distinct table_name) from information_schema.tables where table_schema = 'creditcard_capstone'")
        count = cursor.fetchone()  # fetchone() gets the 1st row = (4,)
        if count[0] == 0:  # meaning database is empty
            # load data in append mode
            df.write.format("jdbc")\
                .mode("append")\
                .options(user=config['user'],
                         password=config['pw'],
                         url=f"jdbc:mysql://localhost:3306/{config['db']}",
                         dbtable=f"{config['db']}.{table_name}")\
                .save()
            print(f"{table_name} append success")
        else:
            # load data in overwrite mode
            df.write.format("jdbc")\
                .mode("overwrite")\
                .options(user=config['user'],
                         password=config['pw'],
                         url=f"jdbc:mysql://localhost:3306/{config['db']}",
                         dbtable=f"{config['db']}.{table_name}")\
                .save()
            print(f"{table_name} overwrite success")
