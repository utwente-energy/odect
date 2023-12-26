# Copyright 2023 Bas Jansen

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import csv
import requests
import pandas as pd
import datetime as dt
import os


def fetch_gb_generation(date):
	date = dt.datetime.strptime(date+"+00:00", '%Y-%m-%d %H:%M%z')
	year = date.astimezone(dt.timezone.utc).strftime('%Y')
	month = date.astimezone(dt.timezone.utc).strftime('%m')
	day = date.astimezone(dt.timezone.utc).strftime('%d')
	date_str = f'{year}-{month}-{day} 00:00'
	filename = 'data/Database_GB_Generation.csv'
	exists = os.path.exists(filename)
	
	if exists:	# Check if Database_GB_Generation.csv is present
		pass  # Do nothing
	else:
		dbgen = pd.DataFrame(columns=['DATETIME', 'GAS', 'COAL', 'NUCLEAR', 'WIND', 'HYDRO', 'IMPORTS',	 # Initiate Database_GB_Generation
									  'BIOMASS', 'OTHER', 'SOLAR', 'STORAGE', 'GENERATION',
									  'CARBON_INTENSITY', 'LOW_CARBON', 'ZERO_CARBON', 'RENEWABLE', 'FOSSIL',
									  'GAS_perc', 'COAL_perc', 'NUCLEAR_perc', 'WIND_perc', 'HYDRO_perc',
									  'IMPORTS_perc', 'BIOMASS_perc', 'OTHER_perc', 'SOLAR_perc',
									  'STORAGE_perc', 'GENERATION_perc', 'LOW_CARBON_perc',
									  'ZERO_CARBON_perc', 'RENEWABLE_perc', 'FOSSIL_perc'])
		dbgen.to_csv(filename)	# create Database_GB_Generation.csv

	df = pd.read_csv(filename)
	try:
		df['DATETIME'] = pd.to_datetime(df['DATETIME'], format='%Y-%m-%dT%H:%M:%S')	# change column into datetime
	except:
		try:
			df['DATETIME'] = pd.to_datetime(df['DATETIME'], format='%Y-%m-%d %H:%M:%S+00:00')  # change column into datetime #df['DATETIME'] = pd.to_datetime(df['DATETIME'], format='%Y-%m-%dT%H:%M:%S')	# change column into datetime
		except:
			df['DATETIME'] = pd.to_datetime(df['DATETIME'], format='%Y-%m-%d %H:%M:%S+00')	# change column into datetime
			
	df['DATETIME'] = df['DATETIME'].dt.strftime('%Y-%m-%d %H:%M%z')  # change datetime format

	if date_str in df.DATETIME.values:	# check if date_str is already present in the database file
		print(f'British data for {date_str} already in database, skipping download')
	else:  # download new file
		print('Downloading British generation data')
		url = 'https://data.nationalgrideso.com/backend/dataset/88313ae5-94e4-4ddc-a790-593554d8c6b9/resource/f93d1835-75bc-43e5-84ad-12472b180a98/download/df_fuel_ckan.csv'

		response = requests.get(url)
		with open(filename, 'w+') as f:
			writer = csv.writer(f)
			for line in response.iter_lines():
				writer.writerow(line.decode('utf-8').split(','))

	gb_gen = pd.DataFrame()	 # initiate empty dataframe

	gb_gen['CCGT_GB'] = df['GAS_perc']	# write Gas columns to the correct format
	gb_gen['COAL_GB'] = df['COAL_perc']
	gb_gen['NUCL_GB'] = df['NUCLEAR_perc']
	gb_gen['WDON_GB'] = df['WIND_perc']
	gb_gen['HYRS_GB'] = df['HYDRO_perc']
	gb_gen['BIOD_GB'] = df['BIOMASS_perc']
	gb_gen['OTHR_GB'] = df['OTHER_perc']
	gb_gen['PVUT_GB'] = df['SOLAR_perc']

	gb_gen = gb_gen.div(100)  # convert percentages to fractions

	gb_gen['datetime'] = df['DATETIME']
	gb_gen = gb_gen.set_index('datetime')

	date_initial = dt.datetime(int(year), int(month), int(day))
	date_var = date_initial
	date_list = []
	for i in range(24):
		date_list.append(date_var.astimezone(dt.timezone.utc).strftime('%Y-%m-%d %H:%M'))  # create date_list with the queried data and every hour of the day
		date_var = date_var + dt.timedelta(hours=1)

	gb_gen = gb_gen.loc[gb_gen.index.isin(date_list)]  # select rows within date_list
	return gb_gen

