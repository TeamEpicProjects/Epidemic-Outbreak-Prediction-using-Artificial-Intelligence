try:
    from wwo_hist import retrieve_hist_data
    import mysql.connector as connect
    import pandas as pd
    import os
    import time
    import datetime
    print("All modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))

def extract_data():
    '''Extract the weather data from an api using wwo_hist library'''
    # country name with start date and end date
    country_name = {'Argentina': ['2016-03-19', '2017-06-19'],
                    'Brazil': ['2016-02-13', '2016-12-31'],
                    'Colombia': ['2016-01-09', '2016-12-31'],
                    'Dominican_Republic': ['2016-01-09', '2017-04-01'],
                    'Ecuador': ['2016-03-30', '2017-12-20'],
                    'El_Salvador': ['2015-11-28', '2017-12-22'],
                    'Guatemala': ['2015-12-09', '2017-12-03'],
                    'Haiti': ['2016-02-03', '2016-02-03'],
                    'Mexico': ['2015-11-28', '2018-06-30'],
                    'Nicaragua': ['2016-02-09', '2016-09-20'],
                    'Panama': ['2016-01-06', '2017-12-27'],
                    'Puerto_Rico': ['2016-01-27', '2017-12-28'],
                    'United_States': ['2016-02-16', '2017-09-12'],
                    'Virgin_Islands': ['2016-02-24', '2017-08-24']}
    frequency = 24
    api_key = '9553803d74114abe82a83637212708'

    # iterate all country name from dictionary
    for i in country_name:
        time.sleep(5)
        start_date = country_name[i][0]
        end_date = country_name[i][1]

        location_list = [i]
        hist_weather_data = retrieve_hist_data(api_key,
                                        location_list,
                                        start_date,
                                        end_date,
                                        frequency,
                                        location_label = False,
                                        export_csv=False,
                                        store_df = True)

        # DataFrame
        df = pd.DataFrame(hist_weather_data[0])
        # pass dataframe to transform data
        df = transform_data(df)
        # store data in mysql database
        data = load_data(df,i)
        print(data)


def transform_data(df):
    '''transform each dataset'''
    df = df.reset_index()
    df['date_time'] = pd.to_datetime(df['date_time'])
    df['date'] = df['date_time'].dt.date
    df['time'] = df['date_time'].dt.time
    data = df.drop(columns=['index', 'date_time'], axis=True)
    return data


def load_data(data, country_name):
    '''Load dataset into mysql database'''
    # connect to the database
    db = connect.connect(host="localhost", user="root", password="root", database="weatherdataset")

    mycursor = db.cursor()
    mycursor.execute('use weatherdataset')

    # create table of different country
    mycursor.execute(f"""create table {country_name}(maxtempC TEXT, mintempC TEXT, totalSnow_cm TEXT,
                    sunHour TEXT, uvIndex TEXT, moon_illumination TEXT,moonrise TEXT, moonset TEXT, sunrise TEXT, sunset TEXT,
                    DewPointC TEXT, FeelsLikeC TEXT, HeatIndexC TEXT, WindChillC TEXT, WindGustKmph TEXT, cloudcover TEXT,
                    humidity TEXT, precipMM TEXT, pressure TEXT, tempC TEXT, visibility TEXT, winddirDegree TEXT, windspeedKmph TEXT,
                    location TEXT, date TEXT, time TEXT)""")

    # creating column list for insertion
    cols = "`,`".join([str(i) for i in data.columns.tolist()])

    # Insert DataFrame recrds one by one.
    for i, row in data.iterrows():
        sql = f"INSERT INTO `{country_name}` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        mycursor.execute(sql, tuple(row))

        # the connection is not autocommitted by default, so we must commit to save our changes
        db.commit()

    return f'\n{country_name} Data load successfully'

if __name__ == '__main__':
    extract_data()