import pandas as pd
import matplotlib.pyplot as plt
import os
import logging
import numpy as np

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


def concat_n_drawer(files_lst):
    frames_lst = []
    for file in files_lst:
        frame = pd.read_csv(file, sep=';')
        selected_frame = frame[['datetime', 'CO2_dry', 'flag']]
        frames_lst.append(selected_frame)
        logging.info(f"Opened file {file}")

    all_data = pd.concat(frames_lst, ignore_index=True)
    print(all_data)
    all_data['datetime'] = pd.to_datetime(all_data['datetime'])
    all_data.loc[all_data['CO2_dry'] == 999.9, 'CO2_dry'] = np.NaN

    def nan_by_percent(group):
        group_size = len(group)
        nan_size = group['CO2_dry'].isnull().sum()
        print(group.iloc[0], nan_size / group_size, nan_size, group_size)

        if nan_size / group_size < 0.25:
            group['CO2_dry'] = group['CO2_dry'].fillna(group['CO2_dry'].mean())
        else:
            group['CO2_dry'] = np.NaN

        group['flag'] = group['flag'].max()

        return group.iloc[0]

    data = all_data[['datetime', 'CO2_dry', 'flag']].copy()
    data['datetime'] = pd.to_datetime(data['datetime'])
    data['datetime'] = data['datetime'].dt.date
    daily_avg = data.groupby('datetime').apply(nan_by_percent)

    pd.set_option('display.max_rows', None)

    print(daily_avg)

    plt.figure(figsize=(12, 8))

    logging.info("Drawing graph")
    plt.plot(daily_avg['datetime'], daily_avg['CO2_dry'], marker='o', markerfacecolor='#000000',
             markersize=2, label='CO2_dry') # drawstyle='steps-post',

    for index, row in daily_avg.iterrows():
        if row['flag'] != 0:
            plt.axvline(x=row['datetime'], color='#FF0000' if row['flag'] == 1 else '#193300', linestyle='-' if row['flag'] == 1 else '--')

    plt.xlabel('datetime')
    plt.ylabel('CO2')
    plt.title('График CO2 с интервалами отсутствия данных')
    plt.legend()
    plt.grid(True)
    plt.show()


def concat_n_drawer_minutes(files_lst):
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
             markersize=2, label='CO2') # drawstyle='steps-post',

    for index, row in all_data.iterrows():
        if row['flag'] != 0:
            plt.axvline(x=row['datetime'], color='#FF0000' if row['flag'] == 1 else '#193300', linestyle='-' if row['flag'] == 1 else '--')

    plt.xlabel('datetime')
    plt.ylabel('CO2')
    plt.title('График CO2 с интервалами отсутствия данных')
    plt.legend()
    plt.grid(True)
    plt.show()


# concat_n_drawer_minutes(file_selector(data_path))







