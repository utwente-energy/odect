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
import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
import plotly.graph_objects as go
import os

# import fetch functions
from lib.ENTSOE import fetch_generation, fetch_import
from lib.GB import fetch_gb_generation
from lib.KNMI import fetch_pv, fetch_wind


def aef(s_y, s_m, s_d, e_y, e_m, e_d, key_entsoe, key_knmi):
    ef = pd.read_csv('settings/Emission_Factors.csv')  # read emission factors
    gen_i = fetch_gen(s_y, s_m, s_d, e_y, e_m, e_d, key_entsoe, key_knmi)  # collect generation data

    gen = pd.DataFrame()
    gen_list = ['BIOD', 'LIGN', 'COAG', 'CCGT', 'COAL', 'OILS', 'SHAL', 'PEAT', 'GEOT', 'HYPS', 'HYRR', 'HYRS', 'TIDE', 'NUCL', 'OTHR', 'PVUT', 'WSTE', 'WDOF', 'WDON', 'OTHE', 'PVFI_NL', 'PVRO_NL', 'WDNS_NL']
    for gen_type in gen_list:
        gen[gen_type] = gen_i.filter(regex=gen_type).sum(axis=1)

    em = calc_em(gen, ef)  # calculate emission data

    aef_list = calc_aef(gen, em)  # calculate Average Emission Factor data

    s_date = dt.datetime(int(s_y), int(s_m), int(s_d), tzinfo=dt.timezone.utc)  # encode start datetime
    e_date = dt.datetime(int(e_y), int(e_m), int(e_d), tzinfo=dt.timezone.utc)  # encode end datetime
    date = s_date  # set initial value of date variable
    date_list = []  # make empty list
    for i in range(1, (int((e_date-s_date).days)+1)*24+1):  # for all days between start date and end date
        date_list.append(date.astimezone(dt.timezone.utc).strftime('%Y-%m-%d %H:%M'))  # add date to date_list
        date = date + dt.timedelta(hours=1)  # add 1 day to date variable

    # refactor dataframes before returning
    aef_list['datetime'] = date_list
    aef_list = aef_list.set_index('datetime')

    rename_list = {'BIOD': 'Biomass', 'LIGN': 'Lignite', 'COAG': 'Gasified Coal', 'CCGT': 'Natural gas', 'COAL': 'Coal', 'OILS': 'Oil products', 'SHAL': 'Shale gas', 'PEAT': 'Peat', 'GEOT': 'Geothermal', 'TIDE': 'Ocean energy', 'NUCL': 'Nuclear', 'OTHR': 'Other Renewable', 'WSTE': 'Waste', 'WDOF': 'Wind Offshore', 'WDON': 'Wind Onshore Imp', 'OTHE': 'Other Fossil', 'WDNS_NL': 'Wind Onshore NL'}

    gen['datetime'] = date_list
    gen = gen.set_index('datetime')
    gen = finalise_df(gen, rename_list)

    em['datetime'] = date_list
    em = em.set_index('datetime')
    em = finalise_df(em, rename_list)

    return aef_list, em, gen


def fetch_gen(s_y, s_m, s_d, e_y, e_m, e_d, key_entsoe, key_knmi):
    database_file = 'data/Database_Generation_Alt.csv'
    exists = os.path.exists(database_file)

    if exists:  # Check if Database_Generation.csv is present
        pass  # Do nothing
    else:
        dbgen = pd.DataFrame(columns=['datetime',  # Make new Database_Generation.csv
                                      'BIOD_NL', 'LIGN_NL', 'COAG_NL', 'CCGT_NL', 'COAL_NL', 'OILS_NL', 'SHAL_NL', 'PEAT_NL', 'GEOT_NL', 'HYPS_NL', 'HYRR_NL', 'HYRS_NL', 'TIDE_NL', 'NUCL_NL', 'OTHR_NL', 'WSTE_NL', 'WDOF_NL', 'OTHE_NL', 'total_NL',
                                      'BIOD_DE', 'LIGN_DE', 'COAG_DE', 'CCGT_DE', 'COAL_DE', 'OILS_DE', 'SHAL_DE', 'PEAT_DE', 'GEOT_DE', 'HYPS_DE', 'HYRR_DE', 'HYRS_DE', 'TIDE_DE', 'NUCL_DE', 'OTHR_DE', 'PVUT_DE', 'WSTE_DE', 'WDOF_DE', 'WDON_DE', 'OTHE_DE',
                                      'BIOD_BE', 'LIGN_BE', 'COAG_BE', 'CCGT_BE', 'COAL_BE', 'OILS_BE', 'SHAL_BE', 'PEAT_BE', 'GEOT_BE', 'HYPS_BE', 'HYRR_BE', 'HYRS_BE', 'TIDE_BE', 'NUCL_BE', 'OTHR_BE', 'PVUT_BE', 'WSTE_BE', 'WDOF_BE', 'WDON_BE', 'OTHE_BE',
                                      'BIOD_NO', 'LIGN_NO', 'COAG_NO', 'CCGT_NO', 'COAL_NO', 'OILS_NO', 'SHAL_NO', 'PEAT_NO', 'GEOT_NO', 'HYPS_NO', 'HYRR_NO', 'HYRS_NO', 'TIDE_NO', 'NUCL_NO', 'OTHR_NO', 'PVUT_NO', 'WSTE_NO', 'WDOF_NO', 'WDON_NO', 'OTHE_NO',
                                      'BIOD_DK', 'LIGN_DK', 'COAG_DK', 'CCGT_DK', 'COAL_DK', 'OILS_DK', 'SHAL_DK', 'PEAT_DK', 'GEOT_DK', 'HYPS_DK', 'HYRR_DK', 'HYRS_DK', 'TIDE_DK', 'NUCL_DK', 'OTHR_DK', 'PVUT_DK', 'WSTE_DK', 'WDOF_DK', 'WDON_DK', 'OTHE_DK',
                                      'CCGT_GB', 'COAL_GB', 'NUCL_GB', 'WDON_GB', 'HYRS_GB', 'BIOD_GB', 'OTHR_GB', 'PVUT_GB',
                                      'PVRO_NL', 'WDNS_NL', 'PVFI_NL'])
        dbgen.to_csv(database_file)  # Save new Database_Generation.csv

    gen = pd.read_csv(database_file)  # read generation data
    s_date = dt.datetime(int(s_y), int(s_m), int(s_d), tzinfo=dt.timezone.utc)  # encode start datetime
    e_date = dt.datetime(int(e_y), int(e_m), int(e_d), tzinfo=dt.timezone.utc)  # encode end datetime
    date = s_date  # set initial value of date variable
    date_list = []  # make empty list
    for i in range(1, int((e_date-s_date).days)+2):  # for all days between start date and end date
        date_list.append(date.astimezone(dt.timezone.utc).strftime('%Y-%m-%d %H:%M'))  # add date to date_list
        date = date + dt.timedelta(days=1)  # add 1 day to date variable

    for date in date_list:
        if date in gen['datetime'].values:  # check if selected date is present in csv
            print(f'{date} is present in database')
        else:
            print(f'Fetching online data for {date}')
            new_data = fetch_data(date, key_entsoe, key_knmi)  # collect new data from APIs
            gen = gen.set_index('datetime')
            gen = pd.concat([gen, new_data])  # append GEN with new_data
            gen = gen.reset_index()
            gen = gen.sort_values(by=['datetime'])  # sort the generation database on date
            gen = gen.round(1)  # round the database values on 1 decimal to limit file size
            gen = gen[gen.columns.drop(list(gen.filter(regex='Unnamed')))]
            gen.to_csv(database_file)  # save the new generation database to csv

    gen = gen[gen.datetime.between(f'{s_y}-{s_m}-{s_d} 00:00', f'{e_y}-{e_m}-{e_d} 23:00')]  # select queried dates from gen
    return gen


def fetch_data(date, key_entsoe, key_knmi):
    i = fetch_import(date, key_entsoe)  # collect import data from ENTSOe

    g_nl = fetch_generation(date, '10YNL----------L', key_entsoe)  # collect NL generation data from ENTSOe
    g_nl = g_nl.add_suffix('_NL')  # add '_NL' tag to column names of NL generation
    g_nl['total_NL'] = np.sum(g_nl, axis=1)
    gen = g_nl

    g_de = fetch_generation(date, '10Y1001A1001A82H', key_entsoe)  # germany
    g_de = g_de.add_suffix('_DE')
    g_de_total = np.sum(g_de, axis=1)  # calculate total generation per time slot
    g_de = g_de.div(g_de_total, axis='rows')  # divide generation data by total generation to get fractional generation
    im_de = i['im_DE']  # create database with German import data
    g_de = g_de.mul(im_de, axis='rows')  # multiply the relative generation with import to obtain absolute import per type
    gen = gen.join(g_de)  # add German data to generation dataframe

    g_be = fetch_generation(date, '10YBE----------2', key_entsoe)  # belgium
    g_be = g_be.add_suffix('_BE')
    g_be_total = np.sum(g_be, axis=1)
    g_be = g_be.div(g_be_total, axis='rows')  # divide generation data by total generation to get fractional generation
    im_be = i['im_BE']
    g_be = g_be.mul(im_be, axis='rows')
    gen = gen.join(g_be)

    g_no = fetch_generation(date, '10YNO-2--------T', key_entsoe)  # norway
    g_no = g_no.add_suffix('_NO')
    g_no_total = np.sum(g_no, axis=1)
    g_no = g_no.div(g_no_total, axis='rows')  # divide generation data by total generation to get fractional generation
    im_no = i['im_NO']
    g_no = g_no.mul(im_no, axis='rows')
    gen = gen.join(g_no)

    if 'im_DK' in i:
        g_dk = fetch_generation(date, '10YDK-1--------W', key_entsoe)  # denmark
        g_dk = g_dk.add_suffix('_DK')
        g_dk_total = np.sum(g_dk, axis=1)
        g_dk = g_dk.div(g_dk_total, axis='rows')  # divide generation data by total generation to get fractional generation
        im_dk = i['im_DK']
        g_dk = g_dk.mul(im_dk, axis='rows')
        gen = gen.join(g_dk)

    g_gb = fetch_gb_generation(date)
    im_gb = i['im_GB']
    # CHECK IF IMPORT DATA GB IS CORRECTLY PROCESSED
    g_gb = g_gb.mul(im_gb, axis='rows')
    gen = gen.join(g_gb)

    gen = gen.drop(['WDON_NL', 'PVUT_NL'], axis=1)  # drop the ENTSO-e PV and onshore wind data for NL zone because these are modelled manually

    print(f'Fetching PV Data for {date}')
    p = fetch_pv(date, key_knmi)  # collect PV generation data from KNMI
    gen = gen.join(p)
    
    print(f'Fetching wind Data for {date}')
    w = fetch_wind(date, key_knmi)  # collect onshore wind generation data from KNMI
    gen = gen.join(w)

    return gen


def calc_em(gen, ef):
    col = gen.columns  # read GEN column names
    em = pd.DataFrame()  # create empty dataframe EM
    em[col] = np.multiply(gen[col], ef[col].values[:1])  # multiply GEN columns with EF values to obtain emission data
    return em


def calc_aef(gen, em):
    gen_agg = np.sum(gen, axis=1)  # calculate total generation per time slot
    aef_type = em.div(gen_agg, axis='rows')  # calculate contribution to AEF per type
    aef_list = pd.DataFrame()
    aef_list['aef'] = np.sum(aef_type, axis=1)  # calculate AEF per time slot
    return aef_list


def finalise_df(df, rlist):  # adapt dataframes for figure creation
    df = df.rename(columns=rlist)
    df['PV'] = df.filter(regex='PV').sum(axis=1)
    df['Hydropower'] = df.filter(regex='HY').sum(axis=1)
    df['Wind Onshore'] = df.filter(regex='Onshore').sum(axis=1)
    df = df.drop(columns=['PVUT', 'PVRO_NL', 'PVFI_NL', 'Wind Onshore NL', 'Wind Onshore Imp', 'HYRR', 'HYPS', 'HYRS'])
    return df


def figure(df, title, subtitle, ytitle):  # make a figure from dataframe
    fig = go.Figure()  # initiate figure
    for col in df.columns:  # loop through columns to add traces
        fig.add_trace(go.Scatter(x=df.index, y=df[col], mode='lines', name=col))
        fig.update_yaxes(title=ytitle)
    colorway = ['#B0A603', '#AA2409', '#AD5606', '#AF8B04', '#AD5606', '#AD5606', '#AD5606', '#AA2409', '#86B300', '#668705', '#668705', '#A1B201', '#B0A603', '#668705', '#AF8B04', '#86B300', '#769D03', '#668705']
    fig.update_layout(title_text=f'{title} <br><sup>{subtitle}</sup>',  # update layout
                      title_font_size=30,
                      title_font_color="#86b300",
                      title_x=0.5,
                      plot_bgcolor="#f2f2f2",
                      colorway=colorway,
                      )
    fig.show()


