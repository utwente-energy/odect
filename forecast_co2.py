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
	# Start
	date = datetime.datetime.now(datetime.timezone.utc)	 # decode date as datetime
	s_y = int(date.strftime(f'%Y'))  # select year
	s_m = int(date.strftime(f'%m'))  # select month
	s_d = int(date.strftime(f'%d'))  # select day
	s_h = int(date.strftime(f'%H'))  # select hour
	start = datetime.datetime(s_y, s_m, s_d, s_h, 0, tzinfo=datetime.timezone.utc)

	# End
	date = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=5)  # decode date as datetime
	e_y = int(date.strftime(f'%Y'))  # select year
	e_m = int(date.strftime(f'%m'))  # select month
	e_d = int(date.strftime(f'%d'))  # select day	
	e_h = int(date.strftime(f'%H'))  # select hour
	end = datetime.datetime(e_y, e_m, e_d, e_h, 0, tzinfo=datetime.timezone.utc)


# Database
# Connect to the database
db = InfluxDBWriter(influx_db, influx_host, influx_port)
# db.clearDatabase()
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







# Retrieving corrective data

# error of last 7 days:
startTime = int(start.timestamp()) - 7*24*3600 - 7200
endTime = int(start.timestamp()) - 7200

avg_err = 0
last_err = 0

# We correct the CO2 emissions for recently observed trends
try:
	# actual data
	measurement = 	"co2"									# Either: devices, controllers, host, flows
	field = 		"co2"										# Field you'd like to read
	condition = 	"\"type\" = 'AEF'"					# Name of the element you want to have the data of
	r_real = influx_read(url, dbname, startTime, endTime, measurement, field, condition)


	# add prices data to dataframe of a day before
	measurement = 	"forecast"									# Either: devices, controllers, host, flows
	field = 		"co2"										# Field you'd like to read
	condition = 	"\"type\" = 'AEF'"					# Name of the element you want to have the data of
	r_forecast = influx_read(url, dbname, startTime, endTime, measurement, field, condition)


	avg_err = mean(r_real) - mean(r_forecast)
	last_err = (r_real[-1] - r_forecast[-1]) - avg_err
except:
	pass
























# Getting the data
fdata_in = 	pd.DataFrame()



# Load scalars
scaler_in = joblib.load("training/scalar_in_co2_forecast") 
scaler_out = joblib.load("training/scalar_out_co2_forecast") 




endTime = int(end.timestamp())	
realstart = copy.deepcopy(start)

# loop through the days
while start <= end - datetime.timedelta(days = 1):
	startTime = int(start.timestamp()) - 24*3600
	endTime = int(start.timestamp()) + 24*3600


	# add co2 emissions of a day before to the output dataframe
	measurement = 	"co2"										# Either: devices, controllers, host, flows
	field = 		"co2"										# Field you'd like to read
	condition = 	"\"type\" = 'AEF'"							# Name of the element you want to have the data of

	r_real = influx_read(url, dbname, startTime-(24*3600), endTime-(24*3600), measurement, field, condition, True)
	
	measurement = 	"forecast"									# Either: devices, controllers, host, flows
	field = 		"co2"										# Field you'd like to read
	condition = 	"\"type\" = 'AEF'"							# Name of the element you want to have the data of

	r_fc = influx_read(url, dbname, startTime-(24*3600), endTime-(24*3600), measurement, field, condition, True)
	
	r = []
	for i in range(0, 48):
		try:
			if i < len(r_real):
				if r_real[i] is not None:
					r.append(r_real[i])
				else:
					r.append(r_fc[i])
			else:
				r.append(r_fc[i])
		except:
			r.append(0.0)
	
	fdata_in["co2_co2_AEF"] = copy.deepcopy(r)


	# add prices data to dataframe of a day before
	measurement = 	"prices"									# Either: devices, controllers, host, flows
	field = 		"euro"										# Field you'd like to read
	condition = 	"\"type\" = 'electricity'"					# Name of the element you want to have the data of
	
	r_real = influx_read(url, dbname, startTime, endTime, measurement, field, condition, True)
	
	measurement = 	"forecast"									# Either: devices, controllers, host, flows
	field = 		"euro"										# Field you'd like to read
	condition = 	"\"type\" = 'electricity'"					# Name of the element you want to have the data of

	r_fc = influx_read(url, dbname, startTime, endTime, measurement, field, condition, True)
	
	r = []
	for i in range(0, 48):
		try:
			if i < len(r_real):
				if r_real[i] is not None:
					r.append(r_real[i])
				else:
					r.append(r_fc[i])
			else:
				r.append(r_fc[i])
		except:
			r.append(0.0)
				
	fdata_in["prices_euro_electricity"] = copy.deepcopy(r)
	
	


		
	# add GHI to dataframe of all weather stations
	for el in weatherstations:
		field = 		"ghi"										# Field you'd like to read
	
		# First KNMI data
		measurement = 	"weather"									# Either: devices, controllers, host, flows
		condition = 	"\"weatherstation\" = '"+el['name']+"' AND \"source\" = 'knmi'"		# Name of the element you want to have the data of
		r_knmi	= influx_read(url, dbname, startTime, endTime, measurement, field, condition, True) 
	
		# Thenfrom yr, then from owm:
		measurement = 	"forecast"									# Either: devices, controllers, host, flows
		
		condition = 	"\"weatherstation\" = '"+el['name']+"' AND \"source\" = 'yr'"		# Name of the element you want to have the data of
		r_yr	= influx_read(url, dbname, startTime, endTime, measurement, field, condition, True) 
		
		condition = 	"\"weatherstation\" = '"+el['name']+"' AND \"source\" = 'owm'"		# Name of the element you want to have the data of
		r_owm	= influx_read(url, dbname, startTime, endTime, measurement, field, condition) 
		
		
		r = []		
		for i in range(0, 48):
			try:
				if i < len(r_knmi) and r_knmi[i] is not None:
					r.append(r_knmi[i])
				elif i < len(r_yr) and r_yr[i] is not None:
					r.append(r_yr[i])
				else:
					r.append(r_owm[i])
			except:
				r.append(0.0)

		fdata_in["weather_"+field+"_"+el['name']] = copy.deepcopy(r)


	# add wind data to dataframe of all weather stations
	for el in weatherstations:
		field = 		"wind"										# Field you'd like to read
		
		# First KNMI data
		measurement = 	"weather"									# Either: devices, controllers, host, flows
		condition = 	"\"weatherstation\" = '"+el['name']+"' AND \"source\" = 'knmi'"		# Name of the element you want to have the data of
		r_knmi	= influx_read(url, dbname, startTime, endTime, measurement, field, condition, True) 
	
		# Thenfrom yr, then from owm:
		measurement = 	"forecast"									# Either: devices, controllers, host, flows
		
		condition = 	"\"weatherstation\" = '"+el['name']+"' AND \"source\" = 'yr'"		# Name of the element you want to have the data of
		r_yr	= influx_read(url, dbname, startTime, endTime, measurement, field, condition, True) 
		
		condition = 	"\"weatherstation\" = '"+el['name']+"' AND \"source\" = 'owm'"		# Name of the element you want to have the data of
		r_owm	= influx_read(url, dbname, startTime, endTime, measurement, field, condition) 
		
		r = []		
		for i in range(0, 48):
			try:
				if i < len(r_knmi) and r_knmi[i] is not None:
					r.append(r_knmi[i])
				elif i < len(r_yr) and r_yr[i] is not None:
					r.append(r_yr[i])
				else:
					r.append(r_owm[i])
			except:
				r.append(0.0)
		
		fdata_in["weather_"+field+"_"+el['name']] = copy.deepcopy(r)


	# add temperature data to dataframe of all weather stations
	for el in weatherstations:
		field = 		"temperature"										# Field you'd like to read
		
		# First KNMI data
		measurement = 	"weather"									# Either: devices, controllers, host, flows
		condition = 	"\"weatherstation\" = '"+el['name']+"' AND \"source\" = 'knmi'"		# Name of the element you want to have the data of
		r_knmi	= influx_read(url, dbname, startTime, endTime, measurement, field, condition, True) 
	
		# Thenfrom yr, then from owm:
		measurement = 	"forecast"									# Either: devices, controllers, host, flows
		
		condition = 	"\"weatherstation\" = '"+el['name']+"' AND \"source\" = 'yr'"		# Name of the element you want to have the data of
		r_yr	= influx_read(url, dbname, startTime, endTime, measurement, field, condition, True) 
		
		condition = 	"\"weatherstation\" = '"+el['name']+"' AND \"source\" = 'owm'"		# Name of the element you want to have the data of
		r_owm	= influx_read(url, dbname, startTime, endTime, measurement, field, condition) 

		r = []		
		for i in range(0, 48):
			try:
				if i < len(r_knmi) and r_knmi[i] is not None:
					r.append(r_knmi[i])
				elif i < len(r_yr) and r_yr[i] is not None:
					r.append(r_yr[i])
				else:
					r.append(r_owm[i])
			except:
				r.append(0.0)
		
		fdata_in["weather_"+field+"_"+el['name']] = copy.deepcopy(r)
		
	

	
	fdata_in.reset_index(drop=True, inplace=True)
	fsdi = pd.DataFrame(scaler_in.transform(fdata_in.values))
	
	n_steps = 24

	fsi = np.array([fsdi[i:i + (n_steps)].copy() for i in range(len(fsdi) - (n_steps))])
	X_pred = fsi


	for i in range(0, 10):
		model = keras.models.load_model('training/ml_co2_forecast_'+str(i)+'.keras')
		# Perform the prediction
		Y_pred = model.predict(X_pred)

		result = scaler_out.inverse_transform(pd.DataFrame(Y_pred[-1])).tolist()

		# store in the DB
		ts = int(start.timestamp())

		for val in result:
			value = float(val[-1])
			
			# Correction
			value += avg_err
			
			# corection for the first 24 hours
			if (ts - int(realstart.timestamp())) < 24*3600:
				value += last_err * max(1, (1 - ( (ts - int(realstart.timestamp())) / (24*3600) ) ) )
				
			# Write into the database
			measurement = "forecast"
			tags = {'country': 'NL', 'type': 'AEF', 'model': str(i)}
			values = {'co2': value}

			# Send the data to the cache
			db.appendValue(measurement, tags, values, ts)
			
			# Write to the database as it will be caching anyway
			db.writeData()

			ts += 3600



	# Store results for backtracking
		# store in the DB
		ts = int(start.timestamp())
		
		for val in result:
			if ts > int(realstart.timestamp())+(24*3600):
				measurement = "forecast1daysahead"
				# Write into the database
				if ts >= int(realstart.timestamp())+(1*24*3600):
					measurement = "forecast1daysahead"
				if ts >= int(realstart.timestamp())+(2*24*3600):
					measurement = "forecast2daysahead"
				if ts >= int(realstart.timestamp())+(3*24*3600):
					measurement = "forecast3daysahead"
				if ts >= int(realstart.timestamp())+(4*24*3600):
					measurement = "forecast4daysahead"
				if ts >= int(realstart.timestamp())+(5*24*3600):
					measurement = "forecast5daysahead"
				
				
				tags = {'country': 'NL', 'type': 'AEF', 'model': str(i)}
				values = {'co2': float(val[-1])}

				# Send the data to the cache
				db.appendValue(measurement, tags, values, ts)
				
				# Write to the database as it will be caching anyway
				db.writeData()
				
			# increment time
			ts += 3600
			
			
	db.writeData(True)
		
	# increment startTime
	start += datetime.timedelta(days = 1)