# This script serves as the main entry point and orchestrates the entire process by calling functions from the ETL, API, and Database modules and starting the Dash Plotly app.

from src.etl.extract import *
from src.etl.transform import *
from src.etl.load import *
from src.database.db_cnx import *
from src.database.db_query import *
from src.database.db_visualization import *
# from src.visualization.dash_app import dash_app*

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tabulate import tabulate
import mysql.connector as msc
import requests
import os


def main():

    # initialize a SparkSession to perform data etl
    ss = SparkSession.builder.appName("Credit_Card_System").getOrCreate()

    # check if active session exists
    if SparkSession.getActiveSession() is not None:
        print("An active Spark session is available.")
    else:
        print("Session has ended.")

    # establish cnx to mysql server
    connect_to_mysql()

    # create db
    create_db(config['db'])

    # define the file paths
    file_paths = {
        'cdw_sapp_branch': './data/cdw_sapp_branch.json',
        'cdw_sapp_credit_card': './data/cdw_sapp_credit.json',
        'cdw_sapp_customer': './data/cdw_sapp_customer.json',
        'cdw_sapp_loan_application': "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
    }

    for file, path in file_paths.items():
        # extract data
        if file == 'cdw_sapp_loan_application':
            df = api_get(ss, path)
        else:
            df = extract_data(ss, path)

        # transform data based on file names
        if file == 'cdw_sapp_branch':
            new_df = transform_branch(df)
        elif file == 'cdw_sapp_credit_card':
            new_df = transform_credit(df)
        elif file == 'cdw_sapp_customer':
            new_df = transform_customer(df)
        elif file == 'cdw_sapp_loan_application':
            new_df = transform_loan(df)

        print('_'* 150, f"\nData extraction success. \nData transformation success. \nDisplaying new dataframe {file}.")
        new_df.printSchema()
        new_df.show(20, truncate=False)

        # load transformed data into mysql db
        load_data(ss, new_df, file)

    # display query result
    show_query()

    # display data plots
    show_visualization()

    # end spark session when done
    ss.stop()

    # check if active session exists
    if SparkSession.getActiveSession() is not None:
        print("An active Spark session is available.")
    else:
        print("Session has ended.")

    # # start the Dash Plotly app
    # dash_app.run_app()


if __name__ == "__main__":
    main()
