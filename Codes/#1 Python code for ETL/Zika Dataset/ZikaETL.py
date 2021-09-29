try:
    import pandas as pd
    from glob import glob
    import os
    import mysql.connector as connect
    print("All modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def extract_data(stock_files):
    '''Extract the data from folder and concatenating all csv file in pandas dataframe'''
    data = pd.concat((pd.read_csv(file, encoding='cp1252')
                      for file in stock_files), ignore_index=True)
    return data


def tranfrom_data(data):
    '''Transform dataset - Drop columns, Replace NaN value'''

    if (("Unnamed: 10" in data) == True) and (("Unnamed: 9" in data) == True) and (("ï»¿report_date" in data) == True):
        data['report_date'].fillna(data['ï»¿report_date'], inplace=True)
        data = data.drop(['Unnamed: 10', 'Unnamed: 9', 'ï»¿report_date'], axis=1)

    elif ("Unnamed: 9" in data) == True and ("ï»¿report_date" in data) == True:
        data['report_date'].fillna(data['ï»¿report_date'], inplace=True)
        data = data.drop(['Unnamed: 9', 'ï»¿report_date'], axis=1)

    elif ("ï»¿report_date" in data) == True:
        data['report_date'].fillna(data['ï»¿report_date'], inplace=True)
        data = data.drop(['ï»¿report_date'], axis=1)

    elif ("Unnamed: 9" in data) == True:
        data = data.drop(['Unnamed: 9'], axis=1)

    # drop unnecessary columns
    data = data.drop(['data_field_code', 'time_period', 'time_period_type', 'unit'], axis=1)
    # replace empty value with 'NaN'
    data = data.fillna('NAN')
    # rename col
    data = data.rename(columns={'value': 'cases'})

    return data


def load_data(data, country_name):
    '''Load dataset into mysql database'''
    # connect to the database
    db = connect.connect(host="localhost", user="root", password="root", database="zikadataset")

    mycursor = db.cursor()
    mycursor.execute('use zikadataset')

    # create table of different country
    mycursor.execute(f"""create table {country_name}(report_date TEXT,
                     location TEXT,
                     location_type TEXT,
                     data_field TEXT,
                     cases TEXT)""")

    # creating column list for insertion
    cols = "`,`".join([str(i) for i in data.columns.tolist()])

    # Insert DataFrame recrds one by one.
    for i, row in data.iterrows():
        sql = f"INSERT INTO `{country_name}` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        mycursor.execute(sql, tuple(row))

        # the connection is not autocommitted by default, so we must commit to save our changes
        db.commit()

    return f'{country_name} Data loaded successfully'


def get_data(data_folder,i):
    stock_files = sorted(glob(f'{data_folder}/{i}/*.csv'))
    print(f'\nGetting {i} data...')
    df = extract_data(stock_files)
    df = tranfrom_data(df)
    data = load_data(df, i)
    print(data)


if __name__ == '__main__':
    data_folder_name = 'Dataset'
    dirList = os.listdir(data_folder_name)
    for country_name in dirList:
        get_data(data_folder_name,country_name)


