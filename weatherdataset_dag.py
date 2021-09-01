try:
    from datetime import timedelta
    from airflow import DAG
    from datetime import datetime
    import pandas as pd
    from glob import glob
    from airflow import DAG
    from datetime import datetime, timedelta
    from airflow.operators.bash_operator import BashOperator
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.mysql_operator import MySqlOperator
    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def transform_data():
    '''Transform dataset - Drop columns, Replace NaN value'''
    df = pd.read_csv("~/ip_files/weatherdataset.csv")
    df = df.reset_index()
    df['date_time'] = pd.to_datetime(df['date_time'])
    df['date'] = df['date_time'].dt.date
    df['time'] = df['date_time'].dt.time
    data = df.drop(columns=['index', 'date_time'], axis=True)
    data.to_csv("~/op_files/transformed_weatherdataset.csv", index=False)


default_args = {"owner":"airflow","start_date":datetime(2021,9,1)}
with DAG(dag_id="weatherdataset_dag",default_args=default_args,schedule_interval='@once') as dag:

    extract_data = BashOperator(
        task_id="extract_data",
        bash_command="shasum ~/ip_files/weatherdataset.csv",
        retries = 2,
        retry_delay=timedelta(seconds=15))

    transform_data = PythonOperator(
        task_id = "transform_data",
        python_callable= transform_data,)

    create_table = MySqlOperator(
        task_id = "create_table",
        mysql_conn_id = "mysql_db1",
        sql="""CREATE table IF NOT EXISTS weatherdataset (maxtempC TEXT, mintempC TEXT, totalSnow_cm TEXT,
                    sunHour TEXT, uvIndex TEXT, moon_illumination TEXT,moonrise TEXT, moonset TEXT, sunrise TEXT, sunset TEXT,
                    DewPointC TEXT, FeelsLikeC TEXT, HeatIndexC TEXT, WindChillC TEXT, WindGustKmph TEXT, cloudcover TEXT,
                    humidity TEXT, precipMM TEXT, pressure TEXT, tempC TEXT, visibility TEXT, winddirDegree TEXT, windspeedKmph TEXT,
                    location TEXT, date TEXT, time TEXT)""")

    load_dataset = MySqlOperator(
        task_id='load_dataset',
        mysql_conn_id="mysql_db1",
        sql="LOAD DATA INFILE '/var/lib/mysql-files/transformed_weatherdataset.csv' INTO TABLE weatherdataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;")

extract_data >> transform_data >> create_table >> load_dataset