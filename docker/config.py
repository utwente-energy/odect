# Copyright 2023 Gerwin Hoogsteen

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# File that contains settigns for ODECT

import os

odect_settings = {
	'api_knmi': 		str(os.environ['ODECT_API_KNMI']), 			# personal security token of KNMI 			https://developer.dataplatform.knmi.nl/get-started#obtain-an-api-key
	'api_entsoe':		str(os.environ['ODECT_API_ENTSOE']),		# personal security token of ENTSO-e 		https://transparency.entsoe.eu/content/static_content/download?path=/Static%20content/API-Token-Management.pdf
	'api_owm':			str(os.environ['ODECT_API_OWM']),			# personal security token of OpenWeatherMap	https://openweathermap.org/appid
	'user_agent_yr':	str(os.environ['ODECT_UA_YR']),				# Yr.no user agent, can be custom, see https://developer.yr.no/doc/locationforecast/HowTO/
	'n_days':			int(str(os.environ['ODECT_N_DAYS'])), 		# Default days to download data for if no range is specified
	
	'influx_host': 		str(os.environ['ODECT_INFLUXURL']),			# InfluxDB Host, NOTE: Only Inxludb 1.x is supported at this moment
	'influx_port': 		str(os.environ['ODECT_INFLUXPORT']),		# InfluxDB Port, default is 8086
	'influx_db': 		str(os.environ['ODECT_INFLUXDB'])			# Database to write to
	}