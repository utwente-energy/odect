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

import requests

class InfluxDBWriter():
	def __init__(self, database, host="http://localhost", port="8086"):
		self.host = host 
		self.port = port
		self.database = database

		self.maxBuffer = 500000
		self.data = []

	def clearDatabase(self):
		print("clearing database "+self.database)
		payload = {'q':"DROP DATABASE "+self.database}
		try:
			requests.post(self.host + ':'+self.port + '/query', data=payload)
		except:
			print("Could not connect to database, is it running?")
			quit()

		print("creating database " + self.database)
		self.createDatabase()

		print("done with clearing the database")

	def createDatabase(self):
		payload = {'q': "CREATE DATABASE " + self.database}
		try:
			requests.post(self.host + ':' + self.port + '/query', data=payload)
		except:
			print("Could not connect to database, is it running?")
			quit()

	def appendValue(self, measurement, tags, values, time, deltatime=0):
		# create tags
		tagstr = ""
		for key,  value in tags.items():
			if not tagstr == "":
				tagstr += ","
			tagstr += key + "="+value

		# create vals
		valsstr = ""
		for key,  value in values.items():
			if not valsstr == "":
				valsstr += ","
			valsstr += key+ "="+str(value)

		# Check the time
		timestr = str(int(time * 1000000000.0) + (deltatime*1000) )

		s = measurement + ","
		s += tagstr + " "
		s += valsstr + " "
		s += timestr
		s += '\n'

		self.data.append(s)

	def appendValuePrepared(self, data, time):
		timestr = str(int(time.timestamp())) + '000000000'
		self.data.append(data + " " + timestr)

	def writeData(self,  force = False):
		if len(self.data) > self.maxBuffer or force:
			dataToSend = ("\n".join(self.data))
			try:
				requests.post(self.host+ ':'+self.port+ '/write?db='+self.database, data=dataToSend)
			except:
				print("Could not connect to database, is it running?")

			self.data = []
