from wwo_hist import retrieve_hist_data
import pandas as pd
import joblib
import pickle
import json
import datetime

class GET_DATA:
    def __init__(self):
        self.json_data = json.load(open('static/zikadata.json','r'))
        

    def get_data(self,location_name):
        for data in self.json_data:
            if data["location"] in location_name:
                lat = data["latitude"]
                log = data["longitude"]
                den = data["density_per_km"]

        return den, lat, log
    
    def get_wheather_data(self,location_name,start_date,end_date):

        start_date = pd.to_datetime(start_date, format="%Y-%m-%d")
        start_date = start_date - pd.to_timedelta(7, unit='d')
        start_date = start_date.strftime('%Y-%m-%d')

        end_date = pd.to_datetime(end_date, format="%Y-%m-%d")
        end_date = end_date - pd.to_timedelta(7, unit='d')
        end_date = end_date.strftime('%Y-%m-%d')

        frequency = 24
        api_key = '092a2ffeb04645c5ba855736210909' 

        location_list = [location_name]
        hist_weather_data = retrieve_hist_data(api_key,
                                        location_list,
                                        start_date,
                                        end_date,
                                        frequency,
                                        location_label = False,
                                        export_csv=False,
                                        store_df = True)
        data = hist_weather_data[0]

        data[['maxtempC','mintempC', 'totalSnow_cm', 'sunHour', 'moon_illumination', 'DewPointC',
           'WindGustKmph', 'cloudcover', 'humidity', 'precipMM', 'pressure',
           'visibility', 'winddirDegree']] = data[['maxtempC',
           'mintempC', 'totalSnow_cm', 'sunHour', 'moon_illumination', 'DewPointC',
           'WindGustKmph', 'cloudcover', 'humidity', 'precipMM', 'pressure',
           'visibility', 'winddirDegree']].apply(pd.to_numeric)

        return data
    
    
    def data_for_clf(self,weather_data,latlog_data):

        # create new year and month column from datetime
        weather_data['year'] = pd.DatetimeIndex(weather_data['date_time']).year
        weather_data['month'] = pd.DatetimeIndex(weather_data['date_time']).month
        weather_data = weather_data.drop(['uvIndex','location','moonrise', 
                                          'moonset', 'sunrise','sunset','FeelsLikeC', 
                                          'HeatIndexC', 'WindChillC','tempC','windspeedKmph'], axis = 1)

        weather_data['density_per_km'] = latlog_data[0]
        weather_data['latitude'] = latlog_data[1]
        weather_data['longitude'] = latlog_data[2]
        weather_data = weather_data[['date_time', 'density_per_km', 'latitude', 'longitude', 'maxtempC', 
                                     'mintempC', 'totalSnow_cm', 'sunHour','moon_illumination', 'DewPointC',
                                     'WindGustKmph', 'cloudcover','humidity', 'precipMM', 'pressure', 
                                     'visibility', 'winddirDegree','year', 'month']]
        return weather_data
    
    def data_for_rg(self,weather_data,latlog_data):

        # create new year and month column from datetime
        weather_data['year'] = pd.DatetimeIndex(weather_data['date_time']).year
        weather_data['month'] = pd.DatetimeIndex(weather_data['date_time']).month

        weather_data = weather_data.drop(['totalSnow_cm',
           'uvIndex', 'moon_illumination', 'moonrise', 'moonset', 'sunrise',
           'sunset', 'HeatIndexC', 'WindChillC',
           'WindGustKmph', 'pressure','tempC', 'visibility', 'location'], axis = 1)

        weather_data['density_per_km'] = latlog_data[0]
        weather_data['latitude'] = latlog_data[1]
        weather_data['longitude'] = latlog_data[2]
        weather_data = weather_data[['date_time','maxtempC', 'mintempC', 'windspeedKmph', 'year', 'month', 'density_per_km', 'precipMM', 'cloudcover',
                                       'humidity','DewPointC', 'latitude', 'longitude','FeelsLikeC', 'winddirDegree', 'sunHour']]
        return weather_data


class clf_Model:
    global classifier
    classifier = joblib.load('classifier_model.sav')
       
        
    def classifier_model(self,data):
        global classifier
        predict_data = data.drop(['date_time'], axis = 1)
        result = classifier.predict_proba(predict_data)
        nozika = [x[0] for x in result]
        zika = [x[1] for x in result]
        data['Zika'] = zika
        data['No Zika'] = nozika

        return data


class reg_Model:
    global regressor
    regressor = joblib.load('regression_model.sav')

    def regressor_model(self,predict_data):
        global regressor 
        predict_data = predict_data.drop(['date_time'], axis = 1)
        result = regressor.predict(predict_data)
        return result
 
    

class Display_result:
    def data_for_display(self,prob_data,reg_result,start_date,end_date):
        
        date = []
        start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        date_array = (start + datetime.timedelta(days=x) for x in range(0, ((end-start).days)+1))

        for date_object in date_array:
            date.append(date_object.strftime("%Y-%m-%d"))
    
    
        data = prob_data.drop(['density_per_km', 'latitude', 'longitude',
           'totalSnow_cm', 'sunHour', 'moon_illumination',
           'WindGustKmph', 'cloudcover', 'pressure',
           'visibility', 'winddirDegree', 'year', 'month'], axis = 1)


        data['No. of cases'] = reg_result

        mean_of_zika = round(data['Zika'].mean()*100)
        mean_of_nozika = round(data['No Zika'].mean()*100)
        total_no_of_cases = int(data['No. of cases'].mean())

        
        data['Zika'] = pd.Series(["{0:.0f}%".format(val * 100) for val in data['Zika']], index = data.index)
        data['No Zika'] = pd.Series(["{0:.0f}%".format(val * 100) for val in data['No Zika']], index = data.index)
        data['No. of cases'] = pd.Series(["{0:.0f}".format(val) for val in data['No. of cases']], index = data.index)
        data.insert(0, "Date", date)

        data.rename(columns={'date_time': 'Climate date', 'maxtempC': 'Max temp','mintempC': 'Min temp','DewPointC': 'Dew point', 'humidity': 'Humdity', 'precipMM': 'Precipitation'}, inplace=True)
        data = data.reset_index()
        data = data.drop(['index'],axis=1)

        return data, mean_of_zika, mean_of_nozika, total_no_of_cases





