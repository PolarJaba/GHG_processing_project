import sys

import numpy as np
import pandas as pd
import logging
import os
from collections import defaultdict
import time

logging.basicConfig(
    format='%(asctime)s: %(threadName)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO
)

processed_directory = 'C:/AARI/GHG/dev/data/processed_data/pika'
directory = 'C:/AARI/GHG/dev/data/processing_data/pika'
files_format = '.dat'
source_name = 'baranova'

selected_cols = ['DATE', 'TIME', 'ALARM_STATUS', 'INST_STATUS',
                 'CavityPressure', 'CavityTemp', 'DasTemp', 'EtalonTemp',
                 'WarmBoxTemp', 'species', 'MPVPosition', 'OutletValve', 'solenoid_valves',
                 'CO', 'CO2', 'CO2_dry',
                 'CH4', 'CH4_dry',
                 'H2O', 'h2o_reported', 'b_h2o_pct',
                 'peak_14', 'peak84_raw']

alarms_cols = ['DATE', 'TIME', 'ALARM_STATUS']
cavity_temp_cols = ['DATE', 'TIME', 'CavityTemp']
cavity_pressure_cols = ['DATE', 'TIME', 'CavityPressure']
warm_box_cols = ['DATE', 'TIME', 'WarmBoxTemp']

stats = pd.DataFrame(columns=['YEAR', 'MONTH',
                              'Total', 'ALARMs', 'solenoid_valves',
                              'CavityTemp', 'WarmBoxTemp', 'CavityPressure',
                              'ALARMs_percent', 'solenoid_valves_percent', 'CavityTemp_percent',
                              'WarmBoxTemp_percent', 'CavityPressure_percent'])


def collect_files_list(main_directory, file_format, data_source_name):
    local_files_lst = []
    for root, dirs, files in os.walk(main_directory + '/' + data_source_name):
        for file in files:
            file_path = os.path.join(root, file)
            if file_path.endswith(file_format):
                local_files_lst.append(file_path)
            else:
                pass

    return local_files_lst


def cleaning_frame(dataframe):

    alarm_data = dataframe[dataframe['ALARM_STATUS'] != 0][['DATE', 'TIME']]
    alarm_data['datetime'] = pd.to_datetime(dataframe['DATE'] + ' ' + dataframe['TIME'])
    alarm_data.drop(['DATE', 'TIME'], axis=1, inplace=True)
    alarm_data['flag_x'] = 4
    alarm_data.set_index('datetime', inplace=True)
    minute_alarms = alarm_data.resample('min').mean()

    dataframe = dataframe[['DATE', 'TIME',
                           'CavityPressure', 'CavityTemp', 'WarmBoxTemp',
                           'CO2_dry', 'CH4_dry']]
    dataframe = dataframe.astype({'DATE': str, 'TIME': str})
    dataframe['datetime'] = pd.to_datetime(dataframe['DATE'] + ' ' + dataframe['TIME'])
    dataframe.drop(['DATE', 'TIME'], axis=1, inplace=True)
    dataframe.set_index('datetime', inplace=True)
    minute_data = dataframe.resample('min').mean()
    std_data = dataframe[['CO2_dry', 'CH4_dry']].resample('min').std()
    if not any(col.startswith('std_') for col in minute_data.columns):
        std_data.columns = ['std_' + col for col in std_data.columns]
        minute_data = minute_data.merge(std_data, left_index=True, right_index=True)
    minute_data['flag'] = 1
    minute_data.loc[(minute_data['CavityTemp'] > 45.005) |
                    (minute_data['CavityTemp'] < 44.995) |
                    (minute_data['WarmBoxTemp'] > 45.005) |
                    (minute_data['WarmBoxTemp'] < 44.995) |
                    (minute_data['CavityPressure'] > 140.02) |
                    (minute_data['CavityPressure'] < 139.98),
                    ['flag']] = 4
    minute_data['DATE'] = minute_data.index.date
    minute_data['TIME'] = minute_data.index.time

    data = pd.merge(minute_alarms, minute_data, how='right', left_index=True, right_index=True)
    data.loc[data['flag_x'] == 4,  'flag'] = 4
    data.drop(['flag_x'], axis=True, inplace=True)
    data.reset_index(drop=True, inplace=True)

    return data


def collect_errors(data, errors_dict):
    errors_dict['alarms'] = pd.concat([errors_dict['alarms'],
                                       (data[['DATE', 'TIME', 'ALARM_STATUS']][data['ALARM_STATUS'] != 0])],
                                      sort=False, ignore_index=True)
    errors_dict['CavityTemp'] = pd.concat([errors_dict['CavityTemp'],
                                          (data[['DATE', 'TIME', 'CavityTemp']][(data['CavityTemp'] > 45.005) |
                                                                                  (data['CavityTemp'] < 44.995)])],
                                          sort=False, ignore_index=True)
    errors_dict['CavityPressure'] = pd.concat([errors_dict['CavityPressure'],
                                              (data[['DATE', 'TIME', 'CavityPressure']][(data['CavityPressure'] > 140.02) |
                                                                                          (data['CavityPressure'] < 139.98)])],
                                              sort=False, ignore_index=True)
    errors_dict['WarmBoxTemp'] = pd.concat([errors_dict['WarmBoxTemp'],
                                           (data[['DATE', 'TIME', 'WarmBoxTemp']][(data['WarmBoxTemp'] > 45.005) |
                                                                                (data['WarmBoxTemp'] < 44.995)])],
                                           sort=False, ignore_index=True)
    return errors_dict


def count_stats(ym, total, errors_dict):
    alarm_amt = len(errors_dict['alarms'])
    cavity_temp_amt = len(errors_dict['CavityTemp'])
    warm_box_amt = len(errors_dict['WarmBoxTemp'])
    cavity_pressure_amt = len(errors_dict['CavityPressure'])
    new_stats = pd.DataFrame({
        'YEAR': ym[:4],
        'MONTH': ym[4:],
        'Total': total,
        'ALARMs': alarm_amt,
        'CavityTemp': cavity_temp_amt,
        'WarmBoxTemp': warm_box_amt,
        'CavityPressure': cavity_pressure_amt
    }, index=[0])
    return new_stats


def save_errors(errors_dict, path, source, name_date):
    for key_name in errors_dict.keys():
        frame_to_save = errors_dict.get(key_name)
        frame_to_save.to_csv(f'{path}/{source}/errors/{source}_{key_name}_{name_date}.csv', sep=';', index=False)
        logging.info(f"Saved {path}/{source}/errors/{source}_{key_name}_{name_date}.csv")


def save_stats(stats_frame, path, source, name_date):
    stats_frame['ALARMs_percent'] = round(stats_frame['ALARMs'] / stats_frame['Total'] * 100, 2)
    stats_frame['CavityTemp_percent'] = round(stats_frame['CavityTemp'] / stats_frame['Total'] * 100, 2)
    stats_frame['WarmBoxTemp_percent'] = round(stats_frame['WarmBoxTemp'] / stats_frame['Total'] * 100, 2)
    stats_frame['CavityPressure_percent'] = round(stats_frame['CavityPressure'] / stats_frame['Total'] * 100, 2)
    stats_frame.to_csv(f'{path}/{source}/stats/{source}_stats_{name_date}.csv', sep=';', index=False)
    logging.info(f"Saved {path}/{source}/stats/{source}_stats_{name_date}.csv")


def save_processed_data(dataframe, path, source, name_date):
    dataframe = dataframe.fillna(-999.9)
    dataframe.to_csv(f'{path}/{source}/data/{source}_{name_date}.csv', sep=';', index=False)
    logging.info(f"Saved {path}/{source}/data/{source}_{name_date}.csv")


# Program
files_lst = collect_files_list(directory, files_format, source_name)

ym_data_lst = defaultdict(list)
for some_file in files_lst:
    year_month = some_file.split('-')[-3][-8:-2]
    ym_data_lst[year_month].append(some_file)

data_lst = sorted(ym_data_lst.keys())
to_next_df = pd.DataFrame()
len_data_lst = len(data_lst)
current_iter = 0
for key in data_lst:
    current_iter += 1
    if not to_next_df.empty:
        all_data_lst = [to_next_df]
    else:
        all_data_lst = []

    errors_dictionary = {'alarms': pd.DataFrame(columns=alarms_cols),
                         'CavityTemp': pd.DataFrame(columns=cavity_temp_cols),
                         'CavityPressure': pd.DataFrame(columns=cavity_pressure_cols),
                         'WarmBoxTemp': pd.DataFrame(columns=warm_box_cols)
                         }
    print(f"""\n\n\nKEY:\n{key}""")
    len_df = 0
    for frame in ym_data_lst.get(key):
        df = pd.read_table(frame, sep='\s+')
        if len(set(df['DATE'].str[:-3])) > 1:
            print(set(df['DATE']))
            df['KEY'] = df['DATE'].apply(lambda x: str(x)[:7])
            df['KEY'] = df['KEY'].str.replace('-', '')

            print('key set: ', set(df['KEY'].to_list()))

            to_next_df = df[df['KEY'] != key]
            df = df[df['KEY'] == key]

            to_next_df.drop(['KEY'], axis=True, inplace=True)
            df.drop(['KEY'], axis=True, inplace=True)

        all_data_lst.append(df)

    all_data = pd.concat(all_data_lst, ignore_index=True)

    errors_dictionary = collect_errors(all_data, errors_dictionary)
    save_errors(errors_dict=errors_dictionary, path=processed_directory, source=source_name, name_date=key)
    stats = count_stats(key, len(all_data), errors_dictionary)
    save_stats(stats, processed_directory, source_name, key)

    all_data = cleaning_frame(all_data)
    save_processed_data(all_data, processed_directory, source_name, key)

    if (not to_next_df.empty) & (current_iter == len_data_lst):
        errors_dictionary = {'alarms': pd.DataFrame(columns=alarms_cols),
                             'CavityTemp': pd.DataFrame(columns=cavity_temp_cols),
                             'CavityPressure': pd.DataFrame(columns=cavity_pressure_cols),
                             'WarmBoxTemp': pd.DataFrame(columns=warm_box_cols)
                             }

        print(10101, key, to_next_df)
        key = str(to_next_df['DATE'].iloc[0])[:7]
        key = key.replace('-', '')
        print('new key: ', key)

        errors_dictionary = collect_errors(to_next_df, errors_dictionary)
        save_errors(errors_dict=errors_dictionary, path=processed_directory, source=source_name, name_date=key)
        stats = count_stats(key, len(to_next_df), errors_dictionary)
        save_stats(stats, processed_directory, source_name, key)

        all_data = cleaning_frame(to_next_df)

        save_processed_data(all_data, processed_directory, source_name, key)
