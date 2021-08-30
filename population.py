import pandas as pd
popdata = pd.read_csv('.\popdataset\population_dataset.csv', index_col=False, delimiter = ',')
popdata.head()

#Create a database
import mysql.connector as msql
from mysql.connector import Error
try:
    conn = msql.connect(host='localhost', user='root',  
                        password='77257')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE population")
        print("Database is created")
except Error as e:
    print("Error while connecting to MySQL", e)

#import all the data into the database
import mysql.connector as mysql
from mysql.connector import Error
try:
    conn = mysql.connect(host='localhost', database='population', user='root', password='77257')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        cursor.execute('DROP TABLE IF EXISTS population_data;')
        print('Creating table....')
        cursor.execute("CREATE TABLE population_data(series_name varchar(255),series_code varchar(255),country_name varchar(255),country_code varchar(255),year_2016 varchar(255),year_2017 varchar(255),year_2018 varchar(255))")
        print("Table is created....")
        #loop through the data frame
        for i,row in popdata.iterrows():
            sql = "INSERT INTO population.population_data VALUES (%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            # the connection is not auto committed by default, so we must commit to save our changes
            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)


# # Execute query
# sql = "SELECT * FROM population.population_data"
# cursor.execute(sql)
# # Fetch all the records
# result = cursor.fetchall()
# for i in result:
#     print(i)
