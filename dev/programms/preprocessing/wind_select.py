from collections import defaultdict

import pandas as pd
import os
import sys


def find_files(ghg_data_path, wind_data_path):
    months_lst = []
    for root, dirs, files in os.walk(ghg_data_path):
        for file in files:
            if file.endswith('.csv'):
                month_year = file.split('_')[-1]
                month_year = month_year.replace('.csv', '')
                months_lst.append(month_year)
            else:
                pass

    local_files_lst = []
    for root, dirs, files in os.walk(wind_data_path):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith('.txt'):
                month_year = file.split('_')[1][:6]
                if month_year in months_lst:
                    local_files_lst.append(file_path)
    return local_files_lst


def select_wind_only(files, wind_path):
    ym_data_lst = defaultdict(list)
    for file in files:
        year_month = file.split('\\')[-1].split('_')[1][:6]
        ym_data_lst[year_month].append(file)

    tail_frames = {}
    for key in sorted(ym_data_lst.keys()):
        print(key)
        key_files = []
        for file in ym_data_lst[key]:
            frame = pd.read_csv(file, sep='\s+')
            frame_cols = list(frame)
            frame_cols.remove('WS10A')
            frame_cols.remove('WDI')
            frame.drop(frame_cols, axis=1, inplace=True)
            frame.reset_index(drop=False, inplace=True)
            frame['key'] = frame['level_0'].apply(lambda x: x.replace('-', '')[:6])
            to_next = frame[frame['key'] != key]
            frame.drop('key', axis=1, inplace=True)
            frame.rename(columns={'level_0': 'date', 'level_1': 'time'}, inplace=True)

            if not to_next.empty:
                local_key = to_next['key'].iloc[0]
                tail_frames[local_key] = pd.DataFrame({'date': to_next['level_0'], 'time': to_next['level_1'],
                                                       'WS10A': to_next['WS10A'], 'WDI': to_next['WDI']})
                print(to_next)

            key_files.append(frame)

        if key in tail_frames.keys():
            print('\n\n')
            print(key)
            key_files.append(tail_frames[key])
            print(tail_frames[key])
            print('\n\n')

        all_month_data = pd.concat(key_files, ignore_index=True)
        all_month_data['datetime'] = all_month_data['date'] + ' ' + all_month_data['time']
        all_month_data = all_month_data[['datetime', 'WS10A', 'WDI']]
        all_month_data['datetime'] = pd.to_datetime(all_month_data['datetime'].str[:16], format='%Y-%m-%d %H:%M')
        all_month_data = all_month_data.sort_values('datetime')
        all_month_data.drop_duplicates(inplace=True)

        all_month_data.to_csv(f'{wind_path}/wind_{key}.csv', sep=',', index=False)

