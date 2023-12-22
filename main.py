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
import os

from lib.functions import aef, figure

# Import the config
try:
	from settings.config import odect_settings
	key_knmi 	= odect_settings['api_knmi']
	key_entsoe 	= odect_settings['api_entsoe']
except:
	print("No valid config found! Please rename the config.py.example file to config.py and enter yourt API keys")
	exit()
	
# Create folder to store data
folder = 'data/'
if not os.path.exists(folder):	# check if folder exists
	os.makedirs(folder)	# make new folder


# define start date (Due to daily publication of weather data, the model works up to yesterday)
s_y = '2023'  # year (yy)
s_m = '12'  # month (mm)
s_d = '20'  # day (dd)
# define end date
e_y = '2023'  # year (yy)
e_m = '12'  # month (mm)
e_d = '21'  # day (dd)

aef, em, gen = aef(s_y, s_m, s_d, e_y, e_m, e_d, key_entsoe, key_knmi)

figure(aef, f'Dynamic Emission Intensity', 'Greenhouse gas emission intensity of the Dutch electricity mix', 'gCO2eq/kWh')
figure(em, f'Dynamic Emissions', 'Generation weighted life-cycle emissions per generation type', 'kgCO2eq')
figure(gen, f'Dynamic Generation', 'Electrical power generation per generation type', 'MW')
