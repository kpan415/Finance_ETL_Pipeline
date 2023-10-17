# This script defines the database connection configurations and credentials.

import mysql.connector as msc
import os 

# define db config
config = {
    "host": 'localhost',
    "user": os.environ.get('MYSQL_DB_USER'),
    "pw": os.environ.get('MYSQL_DB_PW'),
    "db": "creditcard_capstone"
}

def connect_to_mysql():
    # establish connection to mysql db
    cnx = msc.connect(host = config['host'],
                      user = config['user'], 
                      password = config['pw'])
    return cnx 

def create_db(db):
    try:
        # the with statement ensures that the connection is automatically closed when the with block is exited, hence no need to explicitly call cursor.close() and cnx.close()
        with connect_to_mysql() as cnx:
            # create cursor obj to execute sql query
            cursor = cnx.cursor()
            # query to create new db
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
            print(f"Database {db} is created.")

    except msc.Error as e:
        print(e) 