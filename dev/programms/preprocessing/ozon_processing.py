import sys

import pandas as pd
import numpy as np
import os


class DataProcessor:
    @staticmethod
    def collect_files(main_directory, file_format):
        files_lst = []
        for root, dirs, files in os.walk(main_directory):
            for file in files:
                file_path = os.path.join(root, file)
                if file_path.endswith(file_format):
                    files_lst.append(file_path)
                else:
                    pass
        return files_lst

    @staticmethod
    def collect_data(files_list):
        print(files_list)
        data_lst = []
        for file in files_list:
            data_lst.append(pd.read_csv(file, sep=',', names=['date', 'time', 'concentration', 'temperature',
                                                              'humidity']))
        data = pd.concat(data_lst)
        return data

    @staticmethod
    def transform_data_hours(dataframe):
        dataframe['humidity'] = dataframe['humidity'].apply(lambda x: str(x).replace('%', '') if not
                                                            pd.isnull(x) else x)
        dataframe['temperature'] = dataframe['temperature'].apply(lambda x: str(x).replace('(', '') if not
                                                                  pd.isnull(x) else x)
        dataframe['temperature'] = dataframe['temperature'].apply(lambda x: str(x).replace('C)', '') if not
                                                                  pd.isnull(x) else x)
        dataframe = dataframe.astype({'date': str, 'time': str, 'humidity': float, 'temperature': float,
                                      'concentration': float})

        dataframe['datetime'] = pd.to_datetime(dataframe['date'] + ' ' + dataframe['time'])
        dataframe.drop(columns=['date', 'time'], inplace=True)

        def add_flags(group):
            if not group.empty:

                group.loc[((group['humidity'].isnull()) | (group['temperature'].isnull()) |
                           (group['humidity'] == np.NaN) | (group['temperature'] == np.NaN)), 'flag'] = 3

                group.loc[((group['temperature'] < 10) | (group['temperature'] > 35) |
                          (group['humidity'] >= 98)), 'flag'] = 4
                group_size = len(group)
                nan_size = len(group[group['flag'] == 4])

                if nan_size / group_size < 0.5:
                    concentration = group[group['flag'] != 4]['concentration']
                    humidity = group[group['flag'] != 4]['humidity']
                    group['std_concentration'] = concentration.std()
                    group['concentration'] = concentration.mean()
                    if not humidity.isnull().all():
                        group['humidity'] = humidity.mean()
                    else:
                        group['humidity'] = np.NaN
                    flags = group[group['flag'] != 4]['flag']
                    group['flag'] = flags.max()
                    if flags.max() == 3:
                        print(group.iloc[0])
                else:
                    group['concentration'] = group['concentration'].mean()
                    group['std_concentration'] = group['concentration'].std()
                    if not group['humidity'].isnull().all():
                        group['humidity'] = group['humidity'].mean()
                    else:
                        group['humidity'] = np.NaN
                    group['flag'] = group['flag'].max()

                return group.iloc[0]
            else:
                return None

        hour_data = dataframe[['datetime', 'concentration', 'temperature', 'humidity']]
        hour_data['concentration'] = hour_data['concentration'].astype(float)
        hour_data['std_concentration'] = np.NaN
        hour_data['flag'] = 0
        # hour_data.set_index('datetime', inplace=True)
        hour_data = hour_data.groupby(pd.Grouper(key='datetime', freq='H')).apply(add_flags)

        hour_data.drop('datetime', axis=1, inplace=True)
        hour_data.reset_index(drop=False, inplace=True)
        hour_data = hour_data.dropna(subset=['datetime'])
        # hour_data.reset_index(drop=True, inplace=True)
        hour_data['concentration'] = hour_data['concentration'].apply(lambda x: round(x, 3))
        hour_data['std_concentration'] = hour_data['std_concentration'].apply(lambda x: round(x, 6))

        hour_data['humidity'] = hour_data['humidity'].astype(float)
        hour_data['humidity'] = hour_data['humidity'].apply(lambda x: round(x, 1) if not pd.isnull(x) else x)
        hour_data['flag'] = hour_data['flag'].apply(lambda x: int(x) if not pd.isnull(x) else x)

        return hour_data

    @staticmethod
    def transform_data_fr_hours_to_days(dataframe):
        days_data = dataframe[~dataframe['datetime'].isnull()]
        days_data = days_data.astype({'concentration': float, 'temperature': float, 'humidity': float,
                                      'std_concentration': float, 'flag': float})
        days_data['datetime'] = pd.to_datetime(days_data['datetime'])

        def to_days(group):
            print(group)
            if not group.empty:
                three_flag = len(group[group['flag'] == 3])
                four_flag = len(group[group['flag'] == 4])
                group_size = len(group)

                if three_flag <= four_flag:
                    if four_flag / group_size >= 0.5:
                        group['concentration'] = group['concentration'].mean()
                        group['std_concentration'] = group['concentration'].std()
                        if not group['humidity'].isnull().all():
                            group['humidity'] = group['humidity'].mean()
                        else:
                            group['humidity'] = np.NaN
                        group['flag'] = group['flag'].max()
                else:
                    concentration = group[group['flag'] != 4]['concentration']
                    humidity = group[group['flag'] != 4]['humidity']
                    group['concentration'] = concentration.mean()
                    group['std_concentration'] = concentration.std()
                    if not humidity.isnull().all():
                        group['humidity'] = humidity.mean()
                    else:
                        group['humidity'] = np.NaN
                    flags = group[group['flag'] != 4]['flag']
                    group['flag'] = flags.max()

                return group.iloc[0]
            else:
                return None

        days_data = days_data.groupby(pd.Grouper(key='datetime', freq='D')).apply(to_days)
        days_data.drop('datetime', axis=1, inplace=True)
        days_data.reset_index(drop=False, inplace=True)
        days_data = days_data.dropna(subset=['datetime'])

        days_data['concentration'] = days_data['concentration'].apply(lambda x: round(x, 3))
        days_data['std_concentration'] = days_data['std_concentration'].apply(lambda x: round(x, 6))

        days_data['humidity'] = days_data['humidity'].astype(float)
        days_data['humidity'] = days_data['humidity'].apply(lambda x: round(x, 1) if not pd.isnull(x) else x)
        days_data['flag'] = days_data['flag'].apply(lambda x: int(x) if not pd.isnull(x) else x)

        print(days_data)
        return days_data


def count_hours(month_data_path, target_path):
    worker = DataProcessor()
    lst = worker.collect_files(month_data_path, ".csv")
    for file in lst:
        print(file)
        frame = pd.read_csv(file, sep=',', names=['date', 'time', 'concentration', 'temperature', 'humidity'])
        processed_frame = worker.transform_data_hours(frame)
        key = file.split('\\')[-1]
        processed_frame.to_csv(f"{target_path}\\hourly_{key}",
                               sep=',', index=False)


def count_days(month_data_path, target_path):
    worker = DataProcessor()
    lst = worker.collect_files(month_data_path, ".csv")
    for file in lst:
        print(file)
        frame = pd.read_csv(file, sep=',')
        frame = frame[['datetime', 'concentration', 'temperature', 'humidity',
                       'std_concentration', 'flag']]
        processed_frame = worker.transform_data_fr_hours_to_days(frame)
        key = file.split('\\')[-1]
        key = key.replace('hourly_', '')
        processed_frame.to_csv(f"{target_path}\\days\\daily_{key}",
                               sep=',', index=False)




