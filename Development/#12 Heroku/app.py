from os import defpath
import numpy as np
from flask import Flask, request,  render_template
from data import *
import pandas as pd

data = GET_DATA()
reg_model = reg_Model()
clf_model = clf_Model()
result = Display_result()


app = Flask(__name__)

@app.route('/')
def home():
    return render_template('main.html')

@app.route('/predict',methods=['POST'])
def predict():
    features = [x for x in request.form.values()]
    latlog_data = data.get_data(features[0])
    weather_data = data.get_wheather_data(features[0],features[1],features[2])
    clf_data = data.data_for_clf(weather_data,latlog_data)
    clf_result = clf_model.classifier_model(clf_data)
    reg_data = data.data_for_rg(weather_data,latlog_data)
    reg_result = reg_model.regressor_model(reg_data)
    final_result = result.data_for_display(clf_result,reg_result,features[1],features[2])
    labels = final_result[0]['Date'].to_list()
    values = final_result[0]['No. of cases'].to_list()
    Days = len(final_result[0]['No. of cases'])
    return render_template('main.html',tables=[final_result[0].to_html(classes='data', header="true")], location=features[0], startdate=features[1], enddate=features[2],nozika=final_result[2],zika=final_result[1],cases=final_result[3],labels=labels,values=values, Days=Days)

@app.route('/location_name')
def location_name():
    return render_template('location_name.html')

if __name__ == "__main__":
    app.run(host="0.0.0.0",port="8080")