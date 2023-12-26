# Copyright 2023 Bas Jansen, Gerwin Hoogsteen

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
import os, argparse, requests, shutil, datetime

from lib.functions import aef, figure
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
parser.add_argument('-g', '--graphs', action='store_true') 
parser.add_argument('-j', '--json', action='store_true') 
parser.add_argument('-d', '--database', action='store_true') 
parser.add_argument('--prune', action='store_true') 


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
	s_y = args.start[0:4]  	# year (yy)
	s_m = args.start[4:6]  	# month (mm)
	s_d = args.start[6:8]  	# day (dd)
	# define end date
	e_y = args.end[0:4]  	# year (yy)
	e_m = args.end[4:6]  	# month (mm)
	e_d = args.end[6:8]  	# day (dd)

# If dates not specified, then we take the last n days by default
else:
	# Start
	date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=odect_settings['n_days']+1)	 # decode date as datetime
	s_y = date.strftime(f'%Y')  # select year
	s_m = date.strftime(f'%m')  # select month
	s_d = date.strftime(f'%d')  # select day

	# End
	date = datetime.datetime.now(datetime.timezone.utc)	- datetime.timedelta(days=1) # decode date as datetime
	e_y = date.strftime(f'%Y')  # select year
	e_m = date.strftime(f'%m')  # select month
	e_d = date.strftime(f'%d')  # select day


# Clear the data folder if specified
if args.prune:
	shutil.rmtree('data')


# Create folder to store data
folder = 'data/'
if not os.path.exists(folder):	# check if folder exists
	os.makedirs(folder)	# make new folde


# Running ODECT


# Obtain the data
aef, em, gen = aef(s_y, s_m, s_d, e_y, e_m, e_d, key_entsoe, key_knmi)


# Create the output
# Plot graphs if specified
if args.graphs:
	figure(aef, f'Dynamic Emission Intensity', 'Greenhouse gas emission intensity of the Dutch electricity mix', 'gCO2eq/kWh')
	figure(em, f'Dynamic Emissions', 'Generation weighted life-cycle emissions per generation type', 'kgCO2eq')
	figure(gen, f'Dynamic Generation', 'Electrical power generation per generation type', 'MW')
	
	
# Dump JSON output if specified
if args.json:
	print(dumps(loads(aef.to_json()), indent=4))

# Write to datacase if specified
if args.database:
	# Connect to the database
	db = InfluxDBWriter(influx_db, influx_host, influx_port)
	db.createDatabase()

	# Retrieve the data
	data = loads(aef.to_json())
	for date, value in data['aef'].items():
		# Prepare the data
		measurement = "co2"
		tags = {'country': 'NL', 'type': 'AEF'}
		values = {'co2': float(value)}
		
		# Get the UTC timestamp
		time = int(datetime.datetime.timestamp(datetime.datetime.strptime(date+"+00:00", '%Y-%m-%d %H:%M%z')))
		
		# Send the data to the cache
		db.appendValue(measurement, tags, values, time)
		
		# Write to the database as it will be caching anyway
		db.writeData()
		
	# Force a last flush
	db.writeData(True)

