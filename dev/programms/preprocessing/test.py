import sys

import pandas as pd
import os


main_dir = 'C:/AARI/GHG/dev/data/processing_data/ozone'
format_file = 'wri'
source_name = 'baranova'
processed_dir = 'C:/AARI/GHG/dev/data/processed_data/ozone'


def collect_files(main_directory, file_format, data_source_name):
    files_list = []
    for root, dirs, files in os.walk(main_directory + '/' + data_source_name):
        for file in files:
            file_path = os.path.join(root, file)
            if file_path.endswith(file_format):
                files_list.append(file_path)
            else:
                pass
    return files_list


def collect_data(files_list):
    print(files_list)
    file_data = pd.read_csv(files_list[0], sep='\s+', names=['date', 'time', 'o2_ppb', 'percentage', 'added_time'])
    for file in files_list[1:]:
        file_data = pd.concat([file_data, pd.read_csv(file, sep='\s+', names=['date', 'time', 'o2_ppb', 'percentage',
                                                                              'added_time'])],
                              ignore_index=True)
    return file_data


def transform_data_to_days_n_hours(dataframe):
    dataframe = dataframe.astype({'date': str, 'time': str})
    dataframe['date'] = dataframe['date'].str.replace('.', '-')
    dataframe['percentage'] = dataframe['percentage'].str.replace('%', '')
    dataframe = dataframe.astype({'percentage': int})
    print(dataframe['percentage'])
    dataframe['datetime'] = pd.to_datetime(dataframe['date'] + ' ' + dataframe['time'], format='%d-%m-%y %H:%M:%S')
    dataframe.drop(columns=['added_time', 'date', 'time'], inplace=True)
    dataframe.set_index('datetime', inplace=True)
    hour_data = dataframe.resample('H').mean()
    day_data = dataframe.resample('D').mean()

    hour_data['date'] = hour_data.index.date
    hour_data['time'] = hour_data.index.time
    hour_data.reset_index(drop=True, inplace=True)
    hour_data['o2_ppb'] = hour_data['o2_ppb'].apply(lambda x: round(x, 3))
    hour_data['percentage'] = hour_data['percentage'].apply(lambda x: round(x, 0))

    day_data['date'] = day_data.index.date
    day_data.reset_index(drop=True, inplace=True)
    day_data['o2_ppb'] = day_data['o2_ppb'].apply(lambda x: round(x, 3))
    day_data['percentage'] = day_data['percentage'].apply(lambda x: round(x, 0))

    print(hour_data.head())
    print(day_data.head())

    return {
            'hour_data': hour_data,
            'day_data': day_data
    }


def save_data(datadict, source):
    for table in datadict.keys():
        data_key = str(datadict[table]['date'][0])
        data_key = data_key.replace('-', '')
        data_key = data_key[2:]
        print(data_key)

        datadict[table].to_csv(f'{processed_dir}/{source}/{table}_{data_key}.csv', index=False)


files_lst = collect_files(main_dir, format_file, source_name)
data = collect_data(files_lst)

data_dicts = transform_data_to_days_n_hours(dataframe=data)
save_data(data_dicts, source_name)

