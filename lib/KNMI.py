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

# import packages
import datetime as dt
import requests
import sys
import pandas as pd
import netCDF4 as nc
import numpy as np
import math as mt
import os


def fetch_wind(date, key_knmi):
	date = dt.datetime.strptime(date+"+00:00", '%Y-%m-%d %H:%M%z')	 # decode date as datetime
	y = date.astimezone(dt.timezone.utc).strftime(f'%Y')  # select year
	m = date.astimezone(dt.timezone.utc).strftime(f'%m')  # select month
	d = date.astimezone(dt.timezone.utc).strftime(f'%d')  # select day
	delta = dt.timedelta(minutes=60)  # set hourly interval

	date_var = date	 # set initial value for date_var
	df = pd.DataFrame()	 # create empty dataframe
	date_list = []	# create empty list
	for i in range(24):	 # run loops 24 times
		y = date_var.astimezone(dt.timezone.utc).strftime(f'%Y')  # select year
		m = date_var.astimezone(dt.timezone.utc).strftime(f'%m')  # select month
		d = date_var.astimezone(dt.timezone.utc).strftime(f'%d')  # select day
		h = date_var.astimezone(dt.timezone.utc).strftime(f'%H')
		date2 = date_var - dt.timedelta(minutes=120)
		y2 = date2.astimezone(dt.timezone.utc).strftime(f'%Y')	# select year
		m2 = date2.astimezone(dt.timezone.utc).strftime(f'%m')	# select month
		d2 = date2.astimezone(dt.timezone.utc).strftime(f'%d')	# select day
		h2 = date2.astimezone(dt.timezone.utc).strftime(f'%H')
		#wind = knmi_ir(y2, m2, d2, h2, 'wind', y, m, d, h, key_knmi)  # fetch file from KNMI
		wind = knmi_ir(y, m, d, h, 'wind', y, m, d, h, key_knmi)  # fetch file from KNMI
		df = pd.concat([df, wind])	# add data from KNMI to dataframe
		date_list.append(date_var.astimezone(dt.timezone.utc).strftime('%Y-%m-%d %H:%M'))  # add date to date_list
		date_var = date_var + delta	 # increase date_var with delta
	wind_df = df
	wind_df.to_csv('data/wind_df.csv')

	cap_onshore = {'DR': [222],	 # capacity of onshore wind turbines in MW https://opendata.cbs.nl/#/CBS/nl/dataset/70960ned/table
				   'FL': [1351],
				   'FR': [581],
				   'GD': [171],
				   'GR': [734],
				   'LB': [72],
				   'NB': [300],
				   'NH': [668],
				   'OV': [74],
				   'UT': [34],
				   'ZL': [567],
				   'ZH': [535]}
	wind_cap = pd.DataFrame(data=cap_onshore)  # fit capacity data to dataframe

	rot_onshore = {'DR': [847000],	# rotor area of onshore wind turbines in m2 https://opendata.cbs.nl/#/CBS/nl/dataset/70960ned/table
				   'FL': [3724000],
				   'FR': [1951000],
				   'GD': [608000],
				   'GR': [2194000],
				   'LB': [277000],
				   'NB': [974000],
				   'NH': [2129000],
				   'OV': [230000],
				   'UT': [131000],
				   'ZL': [1721000],
				   'ZH': [1640000]}
	wind_rotor = pd.DataFrame(data=rot_onshore)	 # fit capacity data to dataframe

	air_density = 1.246	 # kg/m3
	turbine_efficiency = 0.26  # percentage of total wind power converted into electrical power
	measurement_height = 10	 # height at which wind speed is measured by KNMI
	average_hub_height = 119  # average height of wind turbine axis
	surface_roughness = 0.20  # Hellman exponent 0.10-0.25 for completely flat surface to urban environment respectively https://www.intechopen.com/chapters/17121

	ws_conversion_factor = mt.pow((average_hub_height / measurement_height), surface_roughness)
	cut_in_speed = 5  # speed at which turbine start to produce electricity in m/s
	cut_out_speed = 21	# speed at which turbine shuts off in m/s

	# convert to the right units
	# replace values outside of cutin-cutout with zero
	prov_list = ['GR', 'FR', 'FL', 'OV', 'NH', 'ZH', 'ZL', 'UT', 'GD', 'LB', 'DR', 'NB']
	wind_power = pd.DataFrame()
	wind_speed = pd.DataFrame()
	wind_speed_10m = wind_df

	for prov in prov_list:
		wind_speed[prov] = wind_speed_10m[prov].mul(ws_conversion_factor)
		wind_speed[prov] = wind_speed[prov].where(wind_speed[prov] >= cut_in_speed, other=0)
		wind_speed[prov] = wind_speed[prov].where(wind_speed[prov] <= cut_out_speed, other=0)
		wind_speed[prov] = wind_speed[prov].pow(3)	# convert to (m/s)^3
		wind_power[prov] = wind_speed[prov].mul(wind_rotor[prov])
		wind_power[prov] = wind_power[prov].div(2)	# halving as the equation does
		wind_power[prov] = wind_power[prov].mul(air_density)  # multiplying with air density
		wind_power[prov] = wind_power[prov].div(1000000)  # converting Watts into MW
		wind_power[prov] = wind_power[prov].mul(turbine_efficiency).round(2)  # converting MW of wind power into electric power
		wind_power[prov] = wind_power[prov].clip(0, wind_cap[prov])

	wps = pd.DataFrame()
	wps['WDNS_NL'] = wind_power.sum(axis='columns')
	wps['datetime'] = date_list
	wps = wps.set_index('datetime')

	return wps


def fetch_pv(date, key_knmi):
	date = dt.datetime.strptime(date+"+00:00", '%Y-%m-%d %H:%M%z')	 # decode date as datetime
	y = date.astimezone(dt.timezone.utc).strftime(f'%Y')  # select year
	m = date.astimezone(dt.timezone.utc).strftime(f'%m')  # select month
	d = date.astimezone(dt.timezone.utc).strftime(f'%d')  # select day
	delta = dt.timedelta(minutes=60)  # set hourly interval

	date_var = date	 # set initial value for date_var
	df = pd.DataFrame()	 # create empty dataframe
	date_list = []	# create empty list
	for i in range(24):	 # run loops 24 times
		y = date_var.astimezone(dt.timezone.utc).strftime(f'%Y')  # select year
		m = date_var.astimezone(dt.timezone.utc).strftime(f'%m')  # select month
		d = date_var.astimezone(dt.timezone.utc).strftime(f'%d')  # select day
		h = date_var.astimezone(dt.timezone.utc).strftime(f'%H')
		date2 = date_var - dt.timedelta(minutes=120)
		y2 = date2.astimezone(dt.timezone.utc).strftime(f'%Y')	# select year
		m2 = date2.astimezone(dt.timezone.utc).strftime(f'%m')	# select month
		d2 = date2.astimezone(dt.timezone.utc).strftime(f'%d')	# select day
		h2 = date2.astimezone(dt.timezone.utc).strftime(f'%H')
		#irr = knmi_ir(y2, m2, d2, h2, 'pv', y, m, d, h, key_knmi)  # fetch file from KNMI
		irr = knmi_ir(y, m, d, h, 'pv', y, m, d, h, key_knmi)  # fetch file from KNMI
		df = pd.concat([df, irr])  # add data from KNMI to dataframe
		date_list.append(date_var.astimezone(dt.timezone.utc).strftime('%Y-%m-%d %H:%M'))  # add date to date_list
		date_var = date_var + delta	 # increase date_var with delta
	pv_irr = df
	pv_irr.to_csv('data/pv_irr.csv')

	# set installed PV capacity per province [MWp]
	cap_total = {'DR': [1065],
				 'FL': [636],
				 'FR': [948],
				 'GD': [2240],
				 'GR': [1233],
				 'LB': [1318],
				 'NB': [2991],
				 'NH': [1624],
				 'OV': [1465],
				 'UT': [880],
				 'ZL': [670],
				 'ZH': [1785]}
	pv_cap = pd.DataFrame(data=cap_total)  # fit capacity data to dataframe

	cap_roof = {'DR': [512],
				'FL': [440],
				'FR': [696],
				'GD': [1881],
				'GR': [543],
				'LB': [1233],
				'NB': [2708],
				'NH': [1424],
				'OV': [1178],
				'UT': [792],
				'ZL': [406],
				'ZH': [1612]}
	cap_roof = pd.DataFrame(data=cap_roof)	# fit capacity data to dataframe

	cap_field = {'DR': [553],
				 'FL': [196],
				 'FR': [252],
				 'GD': [358],
				 'GR': [689],
				 'LB': [85],
				 'NB': [282],
				 'NH': [200],
				 'OV': [288],
				 'UT': [88],
				 'ZL': [264],
				 'ZH': [173]}
	cap_field = pd.DataFrame(data=cap_field)  # fit capacity data to dataframe

	col = pv_irr.columns  # read columns from pv_irr
	pvro = pd.DataFrame()  # create empty dataframe
	pvfi = pd.DataFrame()
	pvro[col] = np.multiply(pv_irr[col], cap_roof[col].values[:1])	# multiply irradiation data with capacities
	pvro = pvro.div(1000)  # divide by 1000 W/m2 to get actual generation
	pvfi[col] = np.multiply(pv_irr[col], cap_field[col].values[:1])
	pvfi = pvfi.div(1000)
	pv_n = 0.836  # 1 - all system and placement losses, 16.4 percent in calibration with cbs data
	pvro = pvro.mul(pv_n)  # multiply by PV_n to account for losses
	pvfi = pvfi.mul(pv_n)
	pvro.to_csv('data/pvro.csv')
	pvfi.to_csv('data/pvfi.csv')
	df = pd.DataFrame()	 # create empty dataframe
	df['PVRO_NL'] = pvro.sum(axis=1)  # sum generation over all provinces to obtain national generation
	df['PVFI_NL'] = pvfi.sum(axis=1)
	df['datetime'] = date_list	# add date_list to df
	df = df.set_index('datetime')  # set datetime as index
	return df


def knmi_ir(y, m, d, h, tech, y2, m2, d2, h2, key_knmi):
	# Parameters
	api_url = "https://api.dataplatform.knmi.nl/open-data"
	api_version = "v1"
	api_key = key_knmi
	dataset_name = "Actuele10mindataKNMIstations"
	dataset_version = "2"
	filename = f'KMDS__OPER_P___10M_OBS_L2_{y}{m}{d}{h}00.nc'

	# fetching temporary Download URL
	endpoint = f"{api_url}/{api_version}/datasets/{dataset_name}/versions/{dataset_version}/files/{filename}/url"

	folder = 'data/KNMI_Data'
	if not os.path.exists(folder):	# check if folder exists
	   os.makedirs(folder)	# make new folder

	path = f'{folder}/{filename}'  # path that the file will be downloaded to
	exists = os.path.exists(path)  # True if the file is already there, false if the file still needs to be downloaded

	if exists:	# check if the file already exists
		pass  # file exists, skip the download process
	else:
		get_file_response = requests.get(endpoint, headers={'Authorization': api_key})
		if get_file_response.status_code != 200:  # check if status code is ok
			print(get_file_response.status_code)
			print(get_file_response.text)
			print("Unable to retrieve KNMI download url for file. Adding zeroes")
			
			d = {'DR': [float("NAN")],  # calculate average irradiation in Drenthe
				 'FL': [float("NAN")],
				 'FR': [float("NAN")],
				 'GD': [float("NAN")],
				 'GR': [float("NAN")],
				 'LB': [float("NAN")],
				 'NB': [float("NAN")],
				 'NH': [float("NAN")],
				 'OV': [float("NAN")],
				 'UT': [float("NAN")],
				 'ZL': [float("NAN")],
				 'ZH': [float("NAN")]}
			dr = pd.DataFrame(data=d).astype(float)	 # saving average irradiation data per province to dataframe
			return dr
			
		download_url = get_file_response.json().get("temporaryDownloadUrl")	 # fetch temporary download URL
		print(download_url)

		try:
			with requests.get(download_url, stream=True) as r:
				r.raise_for_status()
				with open(f'data/KNMI_Data/{filename}', "wb") as f:	# create new file in folder
					for chunk in r.iter_content(chunk_size=8192):
						f.write(chunk)	# write file per chunk
		except Exception:
			print("Unable to download KNMI weather file using download URL")
			sys.exit(1)
		print(f'Weather data   NL	{y2}-{m2}-{d2} {h2}:00')

	ds = nc.Dataset(f'data/KNMI_Data/{filename}')  # Converting download .nc file into dataframe

	if tech == 'pv':
		var_name = 'qg'	 # qg is the name for the irradiation column in KNMI data
	elif tech == 'wind':
		var_name = 'ff'	 # ff is the name for the wind column in KNMI data
	else:
		print(f'Unkown technology given: {tech}. Type either pv or wind')

	df = pd.DataFrame(columns=['stationname', 'q'])	 # creation empty dataframe
	
	for i in range(51):	 # include first 51 entries (52-54 are Dutch-caribbean weather stations)
		try:
			sn = ds['stationname'][i]  # reading station names
			q = ds[var_name][i]	 # reading the irradiation/wind data from the dataframe
			if str(q) == '[--]':  # filtering the empty entries
				pass
			else:
				df.loc[len(df)] = [sn, float(q)]
		except:
			pass

	# Following lists show which weather stations are incorporated in the calculation of province averages
	nh = ['DE KOOY VK', 'AMSTERDAM/SCHIPHOL AP', 'BERKHOUT AWS', 'WIJK AAN ZEE AWS']
	zh = ['VOORSCHOTEN AWS', 'AMSTERDAM/SCHIPHOL AP', 'HOEK VAN HOLLAND AWS', 'ROTTERDAM THE HAGUE AP', 'CABAUW TOWER AWS']
	zl = ['VLISSINGEN AWS', 'WESTDORPE AWS', 'WILHELMINADORP AWS', 'HOEK VAN HOLLAND AWS']
	nb = ['GILZE RIJEN', 'HERWIJNEN AWS', 'EINDHOVEN AP', 'VOLKEL']
	lb = ['ELL AWS', 'MAASTRICHT AACHEN AP', 'ARCEN AWS']
	ut = ['AMSTERDAM/SCHIPHOL AP', 'DE BILT AWS', 'CABAUW TOWER AWS', 'HERWIJNEN AWS']
	ov = ['MARKNESSE AWS', 'HEINO AWS', 'HOOGEVEEN AWS', 'HUPSEL AWS', 'TWENTHE AWS']
	gd = ['LELYSTAD AP', 'DEELEN', 'HEINO AWS', 'HUPSEL AWS', 'HERWIJNEN AWS']
	fl = ['STAVOREN AWS', 'LELYSTAD AP', 'MARKNESSE AWS']
	dr = ['MARKNESSE AWS', 'HOOGEVEEN AWS', 'GRONINGEN AP EELDE', 'NIEUW BEERTA AWS']
	fr = ['TERSCHELLING HOORN AWS', 'STAVOREN AWS', 'LEEUWARDEN', 'LAUWERSOOG AWS']
	gr = ['LAUWERSOOG AWS', 'GRONINGEN AP EELDE', 'NIEUW BEERTA AWS']
	# Filtering province selection
	dfnh = df[df['stationname'].isin(nh)]  # create dataframe with irradiation data weather stations in Noord-Holland
	dfzh = df[df['stationname'].isin(zh)]
	dfzl = df[df['stationname'].isin(zl)]
	dfnb = df[df['stationname'].isin(nb)]
	dflb = df[df['stationname'].isin(lb)]
	dfut = df[df['stationname'].isin(ut)]
	dfov = df[df['stationname'].isin(ov)]
	dfgd = df[df['stationname'].isin(gd)]
	dffl = df[df['stationname'].isin(fl)]
	dfdr = df[df['stationname'].isin(dr)]
	dffr = df[df['stationname'].isin(fr)]
	dfgr = df[df['stationname'].isin(gr)]
	# Calculating averages
	d = {'DR': [dfdr['q'].mean(axis=0)],  # calculate average irradiation in Drenthe
		 'FL': [dffl['q'].mean(axis=0)],
		 'FR': [dffr['q'].mean(axis=0)],
		 'GD': [dfgd['q'].mean(axis=0)],
		 'GR': [dfgr['q'].mean(axis=0)],
		 'LB': [dflb['q'].mean(axis=0)],
		 'NB': [dfnb['q'].mean(axis=0)],
		 'NH': [dfnh['q'].mean(axis=0)],
		 'OV': [dfov['q'].mean(axis=0)],
		 'UT': [dfut['q'].mean(axis=0)],
		 'ZL': [dfzl['q'].mean(axis=0)],
		 'ZH': [dfzh['q'].mean(axis=0)]}

	dr = pd.DataFrame(data=d).astype(float)	 # saving average irradiation data per province to dataframe
	return dr


