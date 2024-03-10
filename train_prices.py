# Copyright 2024 Gerwin Hoogsteen

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import os

import pandas as pd
import numpy as np
from json import loads, dumps
import os, argparse, requests, shutil, datetime, time, copy

from statistics import mean
import joblib
import requests
from pytz import timezone

from tools.influx_writer import InfluxDBWriter

import sklearn
from sklearn.preprocessing import StandardScaler, MinMaxScaler

import tensorflow as tf
from tensorflow import keras

from keras import Sequential
from keras.layers import Dense, LSTM
from keras.callbacks import EarlyStopping






def influx_read(url, dbname, startTime, endTime, measurement, field, condition, skip=False):
	# Query (what we request)
	query = 	'SELECT mean(\"' + field + '\") FROM \"' + measurement + '\" WHERE ' + condition + ' AND time >= ' + str(startTime) + '000000000 AND time < ' + str(endTime) + '000000000 GROUP BY time(3600s) fill(previous) ORDER BY time ASC'
	
	if skip:
		query = 	'SELECT mean(\"' + field + '\") FROM \"' + measurement + '\" WHERE ' + condition + ' AND time >= ' + str(startTime) + '000000000 AND time < ' + str(endTime) + '000000000 GROUP BY time(3600s) fill(null) ORDER BY time ASC'

	# Request the data
	payload = {}
	payload['db'] = dbname
	payload['q'] = query

	# Execute the query
	r = requests.get(url, params=payload)

	# Handle the data
	result = []
	if('series' in r.json()['results'][0]):
		d = r.json()['results'][0]['series'][0]['values']
		for value in d:
			result.append(value[1])
	else:
		if not skip:
			result.append(0.0)
		else:
			result.append(None)
	
	
	return result
	
	
	
	
	
	
	
	
	
	

# Import the config
try:
	from settings.config import odect_settings
	key_knmi 	= odect_settings['api_knmi']
	key_entsoe 	= odect_settings['api_entsoe']
	key_owm 	= odect_settings['api_owm']
	
	# Database
	influx_host = odect_settings['influx_host']
	influx_port = odect_settings['influx_port']
	influx_db	= odect_settings['influx_db']
except:
	print("No valid config found! Please rename the config.py.example file to config.py and enter yourt API keys")
	exit()
	

	
#Get arguments:
parser = argparse.ArgumentParser()
parser.add_argument('-s', '--start') 
parser.add_argument('-e', '--end') 


#Parse and check initial arguments
args = parser.parse_args()

# Check date input if it exists:
if args.start is not None or args.end is not None:
	if len(args.start)!=8 or len(args.end)!=8:
		print("ODECT needs a start date and end date in format YYYYMMDD to run")
		exit()

	try:
		int(args.start)
		int(args.end)
	except:
		print("ODECT needs a start date and end date in format YYYYMMDD to run")
		exit()

	if int(args.start) > int(args.end):
		print("ODECT needs a start date and end date in format YYYYMMDD to run")
		exit()

	# define start date (Due to daily publication of weather data, the model works up to yesterday)
	s_y = int(args.start[0:4]) 	# year (yy)
	s_m = int(args.start[4:6])  # month (mm)
	s_d = int(args.start[6:8])  # day (dd)
	# define end date
	e_y = int(args.end[0:4]) 	# year (yy)
	e_m = int(args.end[4:6]) 	# month (mm)
	e_d = int(args.end[6:8])  	# day (dd)
	
	start = datetime.datetime(s_y, s_m, s_d, 0, 0, tzinfo=datetime.timezone.utc)
	end = datetime.datetime(e_y, e_m, e_d, 0, 0, tzinfo=datetime.timezone.utc)

# If dates not specified, then we take the whole dataset minus last week as default for training
else:
	# Time settings
	start = datetime.datetime(2023, 1, 2, tzinfo=datetime.timezone.utc)
	end = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)


# Database
# Connect to the database
db = InfluxDBWriter(influx_db, influx_host, influx_port)
db.createDatabase()


# Database settings for reading
url =		influx_host+":"+str(influx_port)+"/query"
dbname =	influx_db
startTime = int(start.timestamp())		
endTime = 	int(end.timestamp()+24*3600)			
timeBase = 	3600 	#in seconds


# Weatherstations
weatherstations = [
	{'name': 'Terschelling','knmi': 251, 'lat': '53.3739', 'lon': '5.4047'},
	{'name': 'Lauwersoog', 	'knmi': 277, 'lat': '53.3944', 'lon': '6.2001'},
	{'name': 'Leeuwarden', 	'knmi': 270, 'lat': '53.2158', 'lon': '5.7803'},
	{'name': 'Eelde', 		'knmi': 280, 'lat': '53.1173', 'lon': '6.5773'},
	{'name': 'Stavoren', 	'knmi': 267, 'lat': '52.8807', 'lon': '5.3614'},
	{'name': 'Kooy', 		'knmi': 235, 'lat': '52.9204', 'lon': '4.7882'},
	{'name': 'Heino', 		'knmi': 278, 'lat': '52.4367', 'lon': '6.2328'},
	{'name': 'Deelen', 		'knmi': 275, 'lat': '52.0592', 'lon': '5.8832'},
	{'name': 'Volkel', 		'knmi': 375, 'lat': '51.6534', 'lon': '5.7043'},
	{'name': 'Maastricht', 	'knmi': 380, 'lat': '50.8400', 'lon': '5.6992'},
	{'name': 'Vlissingen', 	'knmi': 310, 'lat': '51.4557', 'lon': '3.5761'},
	{'name': 'Hoekholland', 'knmi': 330, 'lat': '51.9811', 'lon': '4.1287'},
	{'name': 'Lelystad', 	'knmi': 269, 'lat': '52.5030', 'lon': '5.4740'},
	{'name': 'Bilt', 		'knmi': 260, 'lat': '52.1078', 'lon': '5.1819'},
	{'name': 'Westdorpe', 	'knmi': 319, 'lat': '51.2299', 'lon': '3.8291'},
	{'name': 'Twente', 		'knmi': 290, 'lat': '52.2736', 'lon': '6.8865'} 
]



# initiate the dataframe
# input:
data_in = 	pd.DataFrame()
data_out =  pd.DataFrame()



n_steps = 24








# The following sectiosn define what data will be read from the Influx Database

## >>>> OUTPUT DEFINITION

# add prices to the output dataframe
measurement = 	"prices"									# Either: devices, controllers, host, flows
field = 		"euro"										# Field you'd like to read
condition = 	"\"type\" = 'electricity'"		# Name of the element you want to have the data of

r = influx_read(url, dbname, startTime, endTime, measurement, field, condition)

# for j in range(0, len(r)):
	# if r[j] < -0.1:
		# r[j] = -0.0
data_out[measurement+"_"+field+"_electricity"] = copy.deepcopy(r)



## >>>> INPUT DEFINITION

# add prices of a day before to the output dataframe
measurement = 	"prices"									# Either: devices, controllers, host, flows
field = 		"euro"										# Field you'd like to read
condition = 	"\"type\" = 'electricity'"		# Name of the element you want to have the data of

r = influx_read(url, dbname, startTime-(24*3600), endTime-(24*3600), measurement, field, condition)

data_in[measurement+"_"+field+"_electricity"] = copy.deepcopy(r)



	
# add GHI to dataframe of all weather stations
for el in weatherstations:
	measurement = 	"weather"									# Either: devices, controllers, host, flows
	field = 		"ghi"										# Field you'd like to read
	condition = 	"\"weatherstation\" = '"+el['name']+"'"		# Name of the element you want to have the data of

	r = influx_read(url, dbname, startTime, endTime, measurement, field, condition)
	data_in[measurement+"_"+field+"_"+el['name']] = copy.deepcopy(r)


# add wind data to dataframe of all weather stations
for el in weatherstations:
	measurement = 	"weather"									# Either: devices, controllers, host, flows
	field = 		"wind"										# Field you'd like to read
	condition = 	"\"weatherstation\" = '"+el['name']+"'"		# Name of the element you want to have the data of

	r = influx_read(url, dbname, startTime, endTime, measurement, field, condition)
	data_in[measurement+"_"+field+"_"+el['name']] = copy.deepcopy(r)

# # add temperature data to dataframe of all weather stations
for el in weatherstations:
	measurement = 	"weather"									# Either: devices, controllers, host, flows
	field = 		"temperature"										# Field you'd like to read
	condition = 	"\"weatherstation\" = '"+el['name']+"'"		# Name of the element you want to have the data of

	r = influx_read(url, dbname, startTime, endTime, measurement, field, condition)
	data_in[measurement+"_"+field+"_"+el['name']] = copy.deepcopy(r)












# Prepare data for training
data_in.reset_index(drop=True, inplace=True)
data_out.reset_index(drop=True, inplace=True)


scaler_in = MinMaxScaler()
scaler_out = MinMaxScaler()

di = scaler_in.fit_transform(data_in.values)
do = scaler_out.fit_transform(data_out.values)

si =  np.array([di[i:i + (n_steps)].copy() for i in range(len(di) - (n_steps))])
so =  np.array([do[i:i + (n_steps)].copy() for i in range(len(do) - (n_steps))])



# The full dataset gives problems.
# Instead we train 2 models aith a split in the dataset (see further downstream)
# X_train = si[:, :n_steps]
# Y_train = so[:, :n_steps]


# Save scalars
joblib.dump(scaler_in , "training/scalar_in_prices_forecast") 
joblib.dump(scaler_out, "training/scalar_out_prices_forecast") 






es = EarlyStopping(monitor = 'val_loss')

cols = len(data_in.axes[1])


for i in range(0, 10):
	# FIXME: Hardcoded two datasets that somewhat work
	if i <6:
		X_train = si[:4200, :n_steps]
		Y_train = so[:4200, :n_steps]
	else:
		X_train = si[4500:8000, :n_steps]
		Y_train = so[4500:8000, :n_steps]

	model = keras.models.Sequential([
		keras.layers.SimpleRNN(cols*4, return_sequences=True, input_shape=[None, cols]),		# timeintervals, features
		keras.layers.SimpleRNN(cols*4, return_sequences=True),
		keras.layers.TimeDistributed(keras.layers.Dense(n_steps))
	])



	model.compile(loss="mape", optimizer="adam")
	model.fit(X_train, Y_train, epochs=50, batch_size=48, validation_split=0) # validation_data=(X_valid, Y_valid))
	
	model.save('training/ml_prices_forecast_'+str(i)+'.keras')
