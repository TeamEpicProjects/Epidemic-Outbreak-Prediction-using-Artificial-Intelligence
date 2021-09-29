# For scheduling
import datetime as dt

# For function jsonToCsv
import pandas as pd

# For function csvToSql
import csv
import pymysql

# Backwards compatibility of pymysql to mysqldb
pymysql.install_as_MySQLdb()

# Importing MySQLdb now
import MySQLdb

# For Apache Airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# Step 2: Define functions for operators.

# A JSON string reader to .csv writer function.
def jsonToCsv(url, outputcsv):

    # Reads the JSON string into a pandas DataFrame object.
    data = pd.read_json(url)

    # Convert the object to a .csv file.
    # It is unnecessary to separate the JSON reading and the .csv writing.
    data.to_csv(outputcsv)

    return 'Read JSON and written to .csv'

def csvToSql():

    # Attempt connection to a database
    try:
        dbconnect = MySQLdb.connect(
                host='localhost',
                user='root',
                passwd='databasepwd',
                db='mydb'
                )
    except:
        print('Can\'t connect.')

    # Define a cursor iterator object to function and to traverse the database.
    cursor = dbconnect.cursor()
    # Open and read from the .csv file
    with open('./argentina.csv') as csv_file:

        # Assign the .csv data that will be iterated by the cursor.
        csv_data = csv.reader(csv_file)

        # Insert data using SQL statements and Python
        for row in csv_data:
            cursor.execute(
            'INSERT INTO argentinaDB(date_time, maxtempC, mintempC, totalSnow_cm, \
                    sunHour, uvIndex, moon_illumination, moonset, sunrise, sunset, \
                    DewPointC, FeelsLikeC, HeatIndexC, WindChillC, WindGustKmph, cloudcover, \
                    humidity, precipMM, pressure, tempC, visibility, winddirDegree, windspeedKmph)' \
                    
                    'VALUES("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", \
                    "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s, "%s, "%s, "%s, "%s, "%s")',
                    row
                    )

    # Commit the changes
    dbconnect.commit()

    # Close the connection
    cursor.close()

    # Confirm completion
    return 'Read .csv and written to the MySQL database'

# DAG's arguments
default_args = {
        'owner': 'argentina',
        'start_date':dt.datetime(2021, 8, 28, 11, 00, 00),
        'concurrency': 1,
        'retries': 0
        }

# DAG's operators, or bones of the workflow
with DAG('parsing_govt_data',
        catchup=False, # To skip any intervals we didn't run
        default_args=default_args,
        schedule_interval='* 1 * * * *', # 's m h d mo y'; set to run every minute.
        ) as dag:

    opr_json_to_csv = PythonOperator(
            task_id='json_to_csv',
            python_callable=jsonToCsv,
            op_kwargs={
                'url':'http://api.worldweatheronline.com/premium/v1/past-weather.ashx?key=9553803d74114abe82a83637212708&q=Argentina&format=json&date=2016-03-19&enddate=2017-06-19',
                'outputcsv':'./argentina.csv'
                }
            )

    opr_csv_to_sql = PythonOperator(
            task_id='csv_to_sql',
            python_callable=csvToSql
            )

# The actual workflow
opr_json_to_csv >> opr_csv_to_sql