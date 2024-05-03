import pandas as pd
import os
import numpy as np
import logging
from path_config import amends_path, garbage_name, calib_lst_name, calib_journal_name, processed_path, processing_path


def clean_non_valid_data(nv_reason, data, flag):
    columns_lst = ['CO', 'CO2', 'CO2_dry', 'CH4', 'CH4_dry',
                   'H2O', 'h2o_reported', 'b_h2o_pct',
                   'peak_14', 'peak84_raw',
                   'std_CO', 'std_CO2', 'std_CO2_dry', 'std_CH4', 'std_CH4_dry',
                   'std_H2O', 'std_h2o_reported', 'std_b_h2o_pct']

    print(nv_reason.head())
    if 'datetime' in list(data):
        data['datetime'] = pd.to_datetime(data['datetime'])
    else:
        data['datetime'] = pd.to_datetime(data['DATE'] + ' ' + data['TIME'])

    nv_reason['start_time'] = pd.to_datetime(nv_reason['start_time'])

    if 'end_time' in list(nv_reason):
        nv_reason['end_time'] = pd.to_datetime(nv_reason['end_time'])
        nv_reason['end_time'] = nv_reason['end_time'] + pd.Timedelta(minutes=4)
    else:
        nv_reason['end_time'] = nv_reason['start_time'] + pd.Timedelta(hours=24)

    for index, row in data.iterrows():
        mask = (nv_reason['start_time'] <= row['datetime']) & (nv_reason['end_time'] >= row['datetime'])
        if mask.any():
            data.loc[index, columns_lst] = 999.9
            data.loc[index, 'flag'] = flag
            print(row['datetime'], flag)

    return data


def file_selector(data_path):
    local_files_lst = []
    for root, dirs, files in os.walk(data_path):
        for file in files:
            file_path = os.path.join(root, file)
            local_files_lst.append(file_path)

    return local_files_lst


def savior(file, path, target_path):
    print(target_path)
    file_name = path.split('\\')
    file_name = file_name[-1]
    print(file_name)
    file.to_csv(f'{target_path}/{file_name}', sep=';', index=False)
    logging.info(f"File {file_name} saved successfully in {target_path}")


def data_processor(files_lst, calibrations, garbage_df):
    for file in files_lst:
        frame = pd.read_csv(file, sep=';')
        frame['flag'] = 0
        data = clean_non_valid_data(garbage_df.copy(), frame, 1)
        data = clean_non_valid_data(calibrations.copy(), data, 2)
        savior(data, file, processed_path)


calibrations_lst_path = amends_path + '/' + calib_lst_name
calibrations_data = pd.read_csv(calibrations_lst_path)

garbage_path = amends_path + '/' + garbage_name
garbage_data = pd.read_csv(garbage_path)

files = file_selector(processing_path)
data_processor(files, calibrations_data, garbage_data)





