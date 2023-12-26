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

# Use an official Python runtime as a parent image
FROM python:3.11.2-bullseye

# Install packages
RUN apt-get update && apt-get -y python3-dev install libhdf5-serial-dev netcdf-bin libnetcdf-dev

COPY requirements.txt . 
RUN pip install -r requirements.txt

WORKDIR /app/odect

# Copy the sources that we require
COPY main.py /app/odect
COPY lib /app/odect/lib
COPY tools /app/odect/tools

RUN mkdir /app/odect/data
RUN mkdir /app/odect/settings

# Copy the Docker config
RUN cp -f docker/config.py /app/odect/settings

# Add volumes
VOLUME /app/odect/data

# Set the default environment variables
ENV ODECT_API_KNMI=example
ENV ODECT_API_ENTSOE=example

ENV ODECT_N_DAYS=3

ENV ODECT_INFLUXURL=http://odect_influx
ENV ODECT_INFLUXPORT=8086
ENV ODECT_INFLUXDB=odect

# Setting the python path
ENV PYTHONPATH=/usr/lib/python3.11/site-packages

# ENV PATH "$PATH:/scripts"

EXPOSE 3001

# Run app.py when the container launches
# CMD sh /scripts/autoexec.sh