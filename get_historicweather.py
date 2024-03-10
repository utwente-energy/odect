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
	start = args.start
	# define end date
	end = args.end


# If dates not specified, then we take the last n days by default
else:
	date_start = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=odect_settings['n_days'])	 # decode date as datetime
	date_end = datetime.datetime.now(datetime.timezone.utc) 														 # decode date as datetime
	
	start = date_start.strftime("%Y%m%d")
	end = date_end.strftime("%Y%m%d")
	
	
# Database
# Connect to the database
db = InfluxDBWriter(influx_db, influx_host, influx_port)
db.createDatabase()	

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

# get knmi data
for ws in weatherstations:
	try:
		data = requests.post("https://www.daggegevens.knmi.nl/klimatologie/uurgegevens", data={'start': start+'01', 'end': end+'24', 'vars':'FH:Q:T', 'stns':str(ws['knmi']), 'fmt':'json'})
		j = loads(data.text)
		
		for el in j:
			try:
				ts = datetime.datetime.strptime(el['date'], '%Y-%m-%dT%H:%M:%S.000%z')
				ts += datetime.timedelta(hours = (int(el['hour'])-1) )
				
				wind = int(el['FH']) / 10.0
				temp = int(el['T']) / 10.0
				if el['Q'] is not None:
					ghi = (int(el['Q']) * 10000) / 3600
				
			
				# Write into the database
				measurement = "weather"
				tags = {'country': 'NL', 'type': 'weather', 'weatherstation': ws['name'], 'knmi_station': str(ws['knmi']), 'source':'knmi'}
				if el['Q'] is not None:
					values = {'wind': wind, 'ghi': ghi, 'temperature': temp}
				else:
					values = {'wind': wind}

				# Send the data to the cache
				db.appendValue(measurement, tags, values, int(ts.timestamp()))
				
				# Write to the database as it will be caching anyway
				db.writeData()
			except:
				pass
				
	except:
		pass

# Force a last flush
db.writeData(True)


