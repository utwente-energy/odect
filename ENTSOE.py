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

import pandas as pd
import requests as rq
import xml.etree.ElementTree as et
import datetime as dt

# general parameters for ENTSO-e request
api_adress = 'https://web-api.tp.entsoe.eu/api?'


def fetch_generation(date, zone_code, key_entsoe):
    sec_token = key_entsoe
    # specific parameters for data ENTSO-e request
    doc_type = 'A75'  # generation document
    proc_type = 'A16'
    bid_zone = zone_code  # ENTSO-e code of the bidding zone

    date = dt.datetime.strptime(date, '%Y-%m-%d %H:%M')
    date_initial = date

    y = date.strftime('%Y')
    m = date.strftime('%m')
    d = date.strftime('%d')

    date_min1 = date - dt.timedelta(days=1)
    y_m1 = date_min1.strftime('%Y')
    m_m1 = date_min1.strftime('%m')
    d_m1 = date_min1.strftime('%d')

    per_start = f'{y_m1}{m_m1}{d_m1}2200'
    per_end = f'{y}{m}{d}2200'

    if bid_zone == '10Y1001A1001A82H':
        if int(per_end) <= 201809302200:  # and date is earlier than 1-10-2018
            print('changing DE zone name')
            bid_zone = '10Y1001A1001A63L'

    link = f'{api_adress}securityToken={sec_token}&documentType={doc_type}&processType={proc_type}&in_Domain={bid_zone}&periodStart={per_start}&periodEnd={per_end}'
    response = rq.get(link)  # receive response
    xmltext = response.text  # read xml text
    root = et.fromstring(xmltext)  # convert to root structure

    if bid_zone == '10YNL----------L' or bid_zone == '10Y1001A1001A82H' or bid_zone == '10Y1001A1001A63L':
        time_points = 96
    else:
        time_points = 24

    no_ts = len(root.findall('{urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0}TimeSeries'))  # count number of columns (generation types) in received data
    ddt = time_points  # number of timepoints in a day (24h/15m)
    df = pd.DataFrame({'position': range(1, ddt+1)})  # create dataframe of length ddt
    full_list = 1  # initiate full list
    last_type = 0  # initiate last type
    last_pos = 0  # initiate last position
    for i in range(10, (no_ts + 10)):  # loop through all timeseries
        no_p = len(root[i][7].findall('{urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0}Point'))  # count number of points in timeseries
        if last_type != root[i][6][0].text:  # check if current directory is the same as previous loop (in case of missing data)
            full_list = 1
        if (no_p + int(last_pos)) >= ddt:  # check if previous timeseries is full
            full_list = 1
        if full_list == 1 and last_type != root[i][6][0].text:  # give production tag to production timeseries
            ts_tag = '_prod'
        if full_list == 1 and last_type == root[i][6][0].text:  # give consumption tag to consumption timeseries
            ts_tag = '_cons'
        if no_p == ddt:  # true in case of a full list (no missing data)
            entry = pd.DataFrame(columns=['position', root[i][6][0].text + ts_tag + '_full'])  # initiate dataframe to fill
            for a in range(2, (no_p + 2)):  # loop through all the points
                pos = root[i][7][a][0].text  # read the positions (time points)
                qua = root[i][7][a][1].text  # read the quantities (generation data [MW])
                entry.loc[len(entry)] = [int(pos), int(qua)]  # add pos and qua to entry dataframe
            last_pos = root[i][7][(no_p + 1)][0].text  # read last position
            last_type = root[i][6][0].text  # read last generation type
            full_list = 1  # set full_list to 1
            df = pd.merge(df, entry, how='outer', on='position')  # add entry data to df
        elif full_list == 1:  # true in case of incomplete timeseries but previous timeseries is complete
            entry = pd.DataFrame(columns=['position', root[i][6][0].text + ts_tag + '_base'])
            for a in range(2, (no_p + 2)):
                pos = root[i][7][a][0].text
                qua = root[i][7][a][1].text
                entry.loc[len(entry)] = [int(pos), int(qua)]
            last_pos = root[i][7][(no_p + 1)][0].text
            last_type = root[i][6][0].text
            full_list = 0  # note that the timeseries was incomplete
            print(f'WARNING: Missing Data Found in zone {bid_zone}, timeseries {root[i][6][0].text}. Appending missing values to timeseries as 0')
            df = pd.merge(df, entry, how='outer', on='position')
        else:  # in case of incomplete timeseries and incomplete previous timeseries
            entry = pd.DataFrame(columns=['position', root[i][6][0].text + ts_tag + '_part' + f'_{i}'])
            for a in range(2, (no_p + 2)):
                pos = root[i][7][a][0].text
                qua = root[i][7][a][1].text
                entry.loc[len(entry)] = [int(pos) + int(last_pos), int(qua)]
            last_pos = int(pos) + int(last_pos)  # add no_pos to last_pos of previous timeseries
            last_type = root[i][6][0].text
            if last_pos == ddt:  # check if timeseries is complete
                full_list = 1
            else:
                full_list = 0
            df = pd.merge(df, entry, how='outer', on='position')

    prod_raw = df.filter(regex='prod')  # read the production data
    prod = pd.DataFrame()
    cons_raw = df.filter(regex='cons')  # read the consumption data
    cons = pd.DataFrame()

    def type_filter(dtf, dtf2, list_type):
        for ent in list_type:
            dtf[ent] = dtf2.filter(regex=ent).sum(axis=1)

    type_list = ['B01', 'B02', 'BO3', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B10', 'B11', 'B12', 'B13', 'B14', 'B15', 'B16', 'B17', 'B18', 'B19', 'B20']
    type_filter(prod, prod_raw, type_list)  # merge production data of same type
    type_filter(cons, cons_raw, type_list)  # merge consumption data of same type
    df = prod  # - cons # (delete first hashtag to include consumption) subtract consumption from production

    # change generation codes into readable tickers
    names = {'B01': 'BIOD',
             'B02': 'LIGN',
             'BO3': 'COAG',
             'B04': 'CCGT',
             'B05': 'COAL',
             'B06': 'OILS',
             'B07': 'SHAL',
             'B08': 'PEAT',
             'B09': 'GEOT',
             'B10': 'HYPS',
             'B11': 'HYRR',
             'B12': 'HYRS',
             'B13': 'TIDE',
             'B14': 'NUCL',
             'B15': 'OTHR',
             'B16': 'PVUT',
             'B17': 'WSTE',
             'B18': 'WDOF',
             'B19': 'WDON',
             'B20': 'OTHE'}
    df = df.rename(columns=names)

    # make column with datetime
    date_list = []
    date = date_initial
    for i in range(time_points):
        date_list.append(date.strftime('%Y-%m-%d %H:%M'))
        date = date + dt.timedelta(minutes=1440/time_points)
    df['datetime'] = date_list
    df = df.set_index('datetime')

    date_list = []
    date = date_initial
    date_time = pd.DataFrame()
    for i in range(time_points):
        date_list.append(date.strftime('%Y-%m-%d %H:%M'))
        date = date + dt.timedelta(minutes=60)
    date_time['datetime'] = date_list
    df = date_time.merge(df, how='inner', on='datetime')
    df = df.set_index('datetime')
    # df = df.drop(columns=['datetime'])
    print(f'Generation data   {zone_code}   {date_initial.strftime("%Y-%m-%d")}')
    return df


def fetch_import(date, key_entsoe):
    # specific input parameters for data ENTSO-e request
    doc_type = "A11"
    in_dom = '10YNL----------L'
    sec_token = key_entsoe
    date = dt.datetime.strptime(date, '%Y-%m-%d %H:%M')
    y = date.strftime('%Y')
    m = date.strftime('%m')
    d = date.strftime('%d')

    date_min1 = date - dt.timedelta(days=1)
    y_m1 = date_min1.strftime('%Y')
    m_m1 = date_min1.strftime('%m')
    d_m1 = date_min1.strftime('%d')

    per_start = f'{y_m1}{m_m1}{d_m1}2200'
    per_end = f'{y}{m}{d}2200'

    print(per_start, per_end)

    dom_list = ['10YGB----------A',  # Great-Britain
                '10YBE----------2',  # Belgium
                '10Y1001A1001A82H',  # Germany
                '10YDK-1--------W',  # Denmark
                '10YNO-2--------T']  # Norway

    df_main = pd.DataFrame()  # initiate main dataframe
    for out_dom in dom_list:  # loop through bidding zones
        d_min = 60
        if out_dom == '10YDK-1--------W' and int(per_end) <= 201909092200:
            pass
        else:
            if out_dom == '10Y1001A1001A82H':
                if int(per_end) <= 201809302200:  # and date is earlier than 1-10-2018
                    print('changing DE zone name')
                    out_dom = '10Y1001A1001A63L'
                if int(per_end) >= 202108010000:
                    d_min = 15

            if out_dom == '10YGB----------A': # Don't include GB for data before 1-1-2022 the data was hourly. Same for DE before 1-8-2021
                if int(per_start) >= 202112312200:  # and date is 1-1-2022 or later
                    d_min = 15

            link = f'{api_adress}securityToken={sec_token}&documentType={doc_type}&in_Domain={in_dom}&out_Domain={out_dom}&periodStart={per_start}&periodEnd={per_end}'
            # receive response and load xml
            #print(link)
            response = rq.get(link)
            xmltext = response.text
            root = et.fromstring(xmltext)

            # create dataframe from xml
            no_p = len(root[9][6].findall('{urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0}Point'))
            entry = pd.DataFrame(columns=[out_dom])
            for a in range(2, (no_p + 2)):
                # pos = root[9][6][a][0].text
                qua = root[9][6][a][1].text
                entry.loc[len(entry)] = [int(qua)]

            date_list = []
            date_var = date
            for i in range(int(1440/d_min)):
                date_list.append(date_var.strftime('%Y-%m-%d %H:%M'))
                date_var = date_var + dt.timedelta(minutes=d_min)
            entry['datetime'] = date_list

            if out_dom == '10YGB----------A':
                df_main = entry
            else:
                df_main = entry.merge(df_main, how='inner', on='datetime')
            print(f'Import data   {out_dom}   {date.strftime("%Y-%m-%d")}')
    df = df_main
    names = {'10YBE----------2': 'im_BE', '10Y1001A1001A82H': 'im_DE', '10YDK-1--------W': 'im_DK', '10YGB----------A': 'im_GB', '10YNO-2--------T': 'im_NO', '10Y1001A1001A63L': 'im_DE'}
    df = df.rename(columns=names)
    df = df.set_index('datetime')
    # df = df.drop(columns=['datetime'])
    return df
