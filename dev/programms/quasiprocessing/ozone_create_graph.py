import sys

import pandas as pd
import matplotlib.pyplot as plt
import os
import logging
import numpy as np
import re

logging.basicConfig(
    format='%(asctime)s: %(threadName)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO
)


def file_selector(dpath):
    local_files_lst = []
    for root, dirs, files in os.walk(dpath):
        for file in files:
            file_path = os.path.join(root, file)
            local_files_lst.append(file_path)

    return local_files_lst


def concat_n_drawer(files_lst, year_key):
    key = year_key
    frames_lst = []
    for file in files_lst:
        frame = pd.read_csv(file, sep=',')
        selected_frame = frame[['datetime', 'concentration', 'flag']]
        frames_lst.append(selected_frame)
        logging.info(f"Opened file {file}")

    all_data = pd.concat(frames_lst, ignore_index=True)

    all_data['datetime'] = pd.to_datetime(all_data['datetime'])

    """ ALARM (empirical value)"""
    logging.warning("Using empirical threshold for concentration value: 0.15")
    all_data = all_data[all_data['concentration'] <= 0.15]

    plt.figure(figsize=(12, 8))

    logging.info("Drawing graph")

    three_cnt, four_cnt = 0, 0
    for index, row in all_data.iterrows():
        if (row['flag'] == 4) & (four_cnt == 0):
            four_cnt += 1
            plt.axvline(x=row['datetime'], color='#8B0000', linestyle='--', alpha=0.5,
                        label='Temperature or humidity value is out of range')
            print(1)
        elif row['flag'] == 4:
            plt.axvline(x=row['datetime'], color='#8B0000', linestyle='--', alpha=0.5)
        elif (row['flag'] == 3) & (three_cnt == 0):
            plt.axvline(x=row['datetime'], color='#FFC0CB', linestyle='--', alpha=0.5,
                        label='No temperature or humidity data')
            three_cnt += 1
        elif row['flag'] == 3:
            plt.axvline(x=row['datetime'], color='#FFC0CB', linestyle='--', alpha=0.5)

    plt.plot(all_data['datetime'], all_data['concentration'], marker='o', markerfacecolor='#00008B',
             markersize=4, mec='#00008B', color='#00008B', label='O3 concentration',
             linewidth=1)  # , drawstyle='steps-post')
    # plt.scatter(all_data['datetime'], all_data['concentration'], marker='o', c='#00008B', s=15, linewidths=0.1)
    plt.xlabel('datetime')
    plt.ylabel('O3 mg/m3')
    plt.title(f'O3 concentration ({key})')
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.07), shadow=False, ncol=3)
    plt.grid(True)
    plt.show()


def concat_n_drawer_minutes(files_lst, year_key):
    frames_lst = []
    for file in files_lst:
        frame = pd.read_csv(file, sep=';')
        selected_frame = frame[['datetime', 'CO2_dry', 'flag']]
        frames_lst.append(selected_frame)
        logging.info(f"Opened file {file}")

    all_data = pd.concat(frames_lst, ignore_index=True)
    print(all_data)
    all_data['datetime'] = pd.to_datetime(all_data['datetime'])
    all_data.loc[(all_data['CO2_dry'] == 999.9) |
                 (all_data['CO2_dry'] > 450), 'CO2_dry'] = np.NaN

    plt.figure(figsize=(12, 8))

    logging.info("Drawing graph")
    plt.plot(all_data['datetime'], all_data['CO2_dry'], marker='o', markerfacecolor='#000000',
             markersize=2, label='CO2')  # drawstyle='steps-post',

    for index, row in all_data.iterrows():
        if row['flag'] != 0:
            plt.axvline(x=row['datetime'], color='#FF0000' if row['flag'] == 1 else '#193300',
                        linestyle='-' if row['flag'] == 1 else '--')

    plt.xlabel('datetime')
    plt.ylabel('CO2')
    plt.title(f'График CO2 с интервалами отсутствия данных за {year_key} год')
    plt.legend()
    plt.grid(True)
    plt.show()


def draw_years_graphs(data_folder):
    all_files_lst = file_selector(data_folder)
    years_lst = [x.split('\\')[-1].replace('.csv', '').replace('daily_', '') for x in all_files_lst]
    years_lst = list(set([x[:4] for x in years_lst]))
    years_lst = sorted(years_lst)
    print(years_lst)
    data_dict = {}

    for year in years_lst:
        data_dict[year] = [file for file in all_files_lst if re.search(r'_{}(.*?)\.csv'.format(year), file)]

    print(data_dict)

    for key in data_dict.keys():
        concat_n_drawer(data_dict[key], key + ' year')

