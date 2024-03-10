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

#pvlib
from pvlib import location

from tools.influx_writer import InfluxDBWriter

# Import the config
try:
	from settings.config import odect_settings
	key_knmi 	= odect_settings['api_knmi']
	key_entsoe 	= odect_settings['api_entsoe']
	key_owm 	= odect_settings['api_owm']
	ua_yr 		= odect_settings['user_agent_yr']
	
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


# Getting the yr data
# https://yr-weather.readthedocs.io/en/latest/locationforecast/examples.html
# Replace with your own User-Agent. See MET API Terms of Service for correct user agents.
user_agent = {
    "User-Agent": ua_yr 
}

last_time = 0

# Retrieve yr data for all weather stations
for ws in weatherstations:
	try:
		# Create pvllib object for clear sky
		loc = location.Location(latitude=float(ws['lat']), longitude=float(ws['lon']))

		# Retrieve the yr data
		data = requests.get("https://api.met.no/weatherapi/locationforecast/2.0/complete?lat="+ws['lat']+"&lon="+ws['lon'], headers = user_agent) 
		j = loads(data.text)
		
		for el in j['properties']['timeseries']:
			try:
				if "next_1_hours" in el['data']:
					# Prepping time and data
					t = el['time'] # time
					ts = datetime.datetime.strptime(t, '%Y-%m-%dT%H:%M:%S%z')
					d = el['data']['instant']['details']
					
					clouds = d['cloud_area_fraction']
					wind = d['wind_speed']
					temp = d['air_temperature']

					# Calculate the clear sky irradiance and ghi
					pdt = pd.DatetimeIndex([t])
					cs = loc.get_clearsky(pdt)
					
					clouds =  1 - (min(100, max(0, (clouds)))/100)
					ghi = float(cs.iloc[0]['ghi']) * (0.2 + (clouds*0.98))
					
					# Write into the database
					if int(time.time()) >= int(ts.timestamp()):
						measurement = "weather"
					else:
						measurement = "forecast"
					
					tags = {'country': 'NL', 'type': 'weather', 'weatherstation': ws['name'], 'knmi_station': str(ws['knmi']), 'source':'yr'}			
					values = {'wind': wind, 'ghi': ghi, 'cloud_cover': 1-clouds, 'temperature': temp}

					# Send the data to the cache
					db.appendValue(measurement, tags, values, int(ts.timestamp()))
					
					# Write to the database as it will be caching anyway
					db.writeData()
					
					# Keep the last time for the retrieval of openweathermap
					if int(ts.timestamp()) > last_time:
						last_time = int(ts.timestamp())
					
			except:
				pass
		
	except:
		pass
	
# Force a last flush
db.writeData(True)




# Now fetch openweathermap
for ws in weatherstations:
	try:
		# Create pvllib object for clear sky
		loc = location.Location(latitude=float(ws['lat']), longitude=float(ws['lon']))

		# Retrieve the yr data
		data = requests.get("https://api.openweathermap.org/data/2.5/forecast?lat="+ws['lat']+"&lon="+ws['lon']+"&appid="+key_owm) 
		j = loads(data.text)
		
		for el in j['list']:
			try:
				# Make sure to only add data that is further in the future
				if int(el['dt']) > last_time - 3*3600: # Only newer data, incorporating 3 hour forecast interval of open weather map
					# Prepping time and data
					ts = int(el['dt']) # time
					
					for i in range(0, 3):
						# We need to upsampel from 3 hours to 1 hour forecasts
						tsi = ts + 3600*i
						
						if tsi > last_time:
							t = datetime.datetime.fromtimestamp(tsi)
							
							wind = float(el['wind']['speed'])
							clouds = float(el['clouds']['all'])
							temp = float(el['main']['temp'])-273.15

							# Calculate the clear sky irradiance and ghi
							# FIXME: Get datetiem from timestamp
							pdt = pd.DatetimeIndex([t])
							cs = loc.get_clearsky(pdt)
							
							clouds =  1 - (min(100, max(0, (clouds)))/100)
							ghi = float(cs.iloc[0]['ghi']) * (0.2 + (clouds*0.98))
							
							# Write into the database
							measurement = "forecast"
							tags = {'country': 'NL', 'type': 'weather', 'weatherstation': ws['name'], 'knmi_station': str(ws['knmi']), 'source':'owm'}			
							values = {'wind': wind, 'ghi': ghi, 'cloud_cover': 1-clouds, 'temperature': temp}

							# Send the data to the cache
							db.appendValue(measurement, tags, values, tsi)
							
							# Write to the database as it will be caching anyway
							db.writeData()
			
			except:
				pass
		
	except:
		pass
		
# Force a last flush
db.writeData(True)