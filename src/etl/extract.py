# This script defines functions to extract data from the JSON data files.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import requests

def extract_data(ss, file_path):
    # read data from json files into pyspark df
    df = ss.read.json(file_path)
    return df

def api_get(ss, url):
    # get data api
    response = requests.get(url)
    print("API response status code: ", response.status_code)
    if response.status_code == 200:
        data = response.json()
        # read json data into pyspark df
        df = ss.createDataFrame(data)
    return df
