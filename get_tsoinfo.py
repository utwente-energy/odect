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

import pandas as pd
from json import loads, dumps
import os, argparse, requests, shutil, datetime, time

# ENTSOE-E API
from entsoe import EntsoePandasClient

#from lib.functions import aef, figure
from tools.influx_writer import InfluxDBWriter

# Import the config
try:
	from settings.config import odect_settings
	key_knmi 	= odect_settings['api_knmi']
	key_entsoe 	= odect_settings['api_entsoe']
	
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

# If dates not specified, then we take the last n days by default
else:
	# Start
	date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2)	 # decode date as datetime
	s_y = int(date.strftime(f'%Y'))  # select year
	s_m = int(date.strftime(f'%m'))  # select month
	s_d = int(date.strftime(f'%d'))  # select day

	# End
	date = datetime.datetime.now(datetime.timezone.utc)	+ datetime.timedelta(days=2) # decode date as datetime
	e_y = int(date.strftime(f'%Y'))  # select year
	e_m = int(date.strftime(f'%m'))  # select month
	e_d = int(date.strftime(f'%d'))  # select day	
	
# Time settings
start_time = datetime.datetime(s_y, s_m, s_d, 0, 0, tzinfo=datetime.timezone.utc)
end_time = datetime.datetime(e_y, e_m, e_d, 0, 0, tzinfo=datetime.timezone.utc)
	



client = EntsoePandasClient(api_key=key_entsoe)

start = pd.Timestamp(int(start_time.timestamp()), unit='s', tzinfo=datetime.timezone.utc)
end = pd.Timestamp(int(end_time.timestamp()), unit='s', tzinfo=datetime.timezone.utc)
country_code = 'NL'  

# Retrieve dataset from ENTSO-E
data = client.query_day_ahead_prices(country_code, start=start, end=end)

# Load the data as a dict
prices = loads(data.to_json())

# Connect to the database
db = InfluxDBWriter(influx_db, influx_host, influx_port)
db.createDatabase()

for ts,price in prices.items():
	ts = int(ts[:-3])
	
	# convert price
	price = float(price)
	price = price/1000
	
	price_zp = price
	price_zp += 0.1088  # Energiebelasting, 0.10880 in 2024
	price_zp += 0.02	# zonneplan handling fee
	price_zp *= 1.21	# BTW
	
	# print(ts, price, price_zp)
	
	# Prepare the data
	measurement = "prices"
	tags = {'country': 'NL', 'type': 'electricity'}
	values = {'euro': price, 'euro_zonneplan': price_zp}
	

	db.appendValue(measurement, tags, values, ts)
		
	# Write to the database as it will be caching anyway
	db.writeData()
	
# Force a last flush
db.writeData(True)

