import pandas
import os
import sys
import numpy as np

import pandas as pd
import adding_flags_pika


def to_hours(files_lst, hour_path):
    for file in files_lst:
        file_name = file.split('\\')[-1]
        frame = pd.read_csv(file, sep=';')
        frame = frame.replace(-999.9, np.NaN)
        frame['datetime'] = pd.to_datetime(frame['datetime'])

        def hours_grouper(group):
            group_size = len(group)
            if group_size < 30:
                group['std_CO2_dry'] = group['std_CO2_dry'].std()
                group['std_CH4_dry'] = group['std_CH4_dry'].std()
                group['flag'] = 8
                group = group.groupby(pd.Grouper(key='datetime', freq='H')).mean().reset_index(drop=True)
                return group
            else:
                correct_data = group[group['flag'] == 1]
                if len(correct_data) / group_size >= 0.5:
                    correct_data['std_CO2_dry'] = correct_data['std_CO2_dry'].std()
                    correct_data['std_CH4_dry'] = correct_data['std_CH4_dry'].std()

                    return correct_data.groupby(pd.Grouper(key='datetime', freq='H')).mean()
                else:
                    four_flag = len(group[group['flag'] == 4])
                    if four_flag / group_size > 0.5:
                        group['flag'] = 4

                    elif len(group[group['flag'] == 3]) / group_size > 0.5:
                        group = group[group['flag'] < 4]
                        group['flag'] = 3

                    elif len(group[group['flag'] == 2]) / group_size > 0.5:
                        group = group[group['flag'] < 3]
                        group['flag'] = 2

                    elif len(group[group['flag'] == 5]) / group_size > 0.5:
                        group = group[(group['flag'] != 4) & (group['flag'] != 3)]
                        group['flag'] = 5
                    else:
                        subgroup = group[group['flag'] < 2]
                        if not subgroup.empty:
                            group = subgroup
                            group['flag'] = 6
                            print('flag 6: ', group.iloc[0])
                        else:
                            group['flag'] = 7
                            print('flag 7: ', group.iloc[0])

                    group['std_CO2_dry'] = group['std_CO2_dry'].std()
                    group['std_CH4_dry'] = group['std_CH4_dry'].std()
                    group = group.groupby(pd.Grouper(key='datetime', freq='H')).mean().reset_index(drop=True)

                    return group

        frame = frame.groupby(pd.Grouper(key='datetime', freq='H')).apply(hours_grouper)
        print(frame)
        frame.reset_index(drop=False, inplace=True)
        frame = frame.drop('level_1', axis=1)

        frame['CO2_dry'] = frame['CO2_dry'].apply(lambda x: round(x, 3))
        frame['CH4_dry'] = frame['CH4_dry'].apply(lambda x: round(x, 3))
        frame['std_CO2_dry'] = frame['std_CO2_dry'].apply(lambda x: round(x, 6))
        frame['std_CH4_dry'] = frame['std_CH4_dry'].apply(lambda x: round(x, 6))

        frame['flag'] = frame['flag'].apply(lambda x: int(x) if not pd.isnull(x) else x)

        frame.to_csv(f'{hour_path}/{file_name}', index=False, sep=';')


def to_days(files_lst, day_path):
    for file in files_lst:
        file_name = file.split('\\')[-1]
        frame = pd.read_csv(file, sep=';')
        frame = frame.replace(-999.9, np.NaN)
        frame['datetime'] = pd.to_datetime(frame['datetime'])

        def hours_grouper(group):
            group_size = len(group)
            if group_size < 12:
                group['std_CO2_dry'] = group['std_CO2_dry'].std()
                group['std_CH4_dry'] = group['std_CH4_dry'].std()
                group['flag'] = 8
                group = group.groupby(pd.Grouper(key='datetime', freq='D')).mean().reset_index(drop=True)
                return group
            else:
                correct_data = group[group['flag'] == 1]
                if len(correct_data) / group_size >= 0.5:
                    correct_data['std_CO2_dry'] = correct_data['std_CO2_dry'].std()
                    correct_data['std_CH4_dry'] = correct_data['std_CH4_dry'].std()

                    return correct_data.groupby(pd.Grouper(key='datetime', freq='D')).mean()
                else:
                    four_flag = len(group[group['flag'] == 4])
                    if four_flag / group_size > 0.5:
                        group['flag'] = 4

                    elif len(group[group['flag'] == 3]) / group_size > 0.5:
                        group = group[group['flag'] < 4]
                        group['flag'] = 3

                    elif len(group[group['flag'] == 2]) / group_size > 0.5:
                        group = group[group['flag'] < 3]
                        group['flag'] = 2

                    elif len(group[group['flag'] == 5]) / group_size > 0.5:
                        group = group[(group['flag'] != 4) & (group['flag'] != 3)]
                        group['flag'] = 5
                    else:
                        subgroup = group[group['flag'] < 2]
                        if not subgroup.empty:
                            group = subgroup
                            group['flag'] = 6
                            print('flag 6: ', group.iloc[0])
                        else:
                            group['flag'] = 7
                            print('flag 7: ', group.iloc[0])

                    group['std_CO2_dry'] = group['std_CO2_dry'].std()
                    group['std_CH4_dry'] = group['std_CH4_dry'].std()
                    group = group.groupby(pd.Grouper(key='datetime', freq='D')).mean().reset_index(drop=True)

                    return group

        frame = frame.groupby(pd.Grouper(key='datetime', freq='D')).apply(hours_grouper)
        print(frame)
        frame.reset_index(drop=False, inplace=True)
        frame = frame.drop('level_1', axis=1)

        frame['CO2_dry'] = frame['CO2_dry'].apply(lambda x: round(x, 3))
        frame['CH4_dry'] = frame['CH4_dry'].apply(lambda x: round(x, 3))
        frame['std_CO2_dry'] = frame['std_CO2_dry'].apply(lambda x: round(x, 6))
        frame['std_CH4_dry'] = frame['std_CH4_dry'].apply(lambda x: round(x, 6))

        frame['flag'] = frame['flag'].apply(lambda x: int(x) if not pd.isnull(x) else x)

        frame.to_csv(f'{day_path}/{file_name}', index=False, sep=';')

