import pandas as pd
import os
import logging
import numpy as np
from scipy import stats
import sys

logging.basicConfig(
    format='%(asctime)s: %(threadName)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO
)


def file_selector(dpath, keys_lst):
    local_files_lst = []
    for root, dirs, files in os.walk(dpath):
        for file in files:
            if file.replace('baranova_', '').replace('.csv', '') in keys_lst:
                file_path = os.path.join(root, file)
                local_files_lst.append(file_path)

    return local_files_lst


def found_full_calibration(group):
    if len(group) == 4:
        return group
    else:
        return None


def find_coefficients(group):
    if not (group['CO2_dry'].isnull().any() or group['CH4_dry'].isnull().any()):
        slope, intercept, r_value, p_value, std_err = stats.linregress(sorted(group['CO2_dry']),
                                                                       sorted(group['CO2']))
        group['a_coefficient_CO2'] = slope
        group['b_coefficient_CO2'] = intercept

        slope, intercept, r_value, p_value, std_err = stats.linregress(sorted(group['CH4_dry']),
                                                                       sorted(group['CH4']))
        group['a_coefficient_CH4'] = slope
        group['b_coefficient_CH4'] = intercept

    return group.iloc[0]


def all_file_selector(dpath):
    local_files_lst = []
    for root, dirs, files in os.walk(dpath):
        for file in files:
            file_path = os.path.join(root, file)
            local_files_lst.append(file_path)

    return local_files_lst


def only_flag_file_selector(dpath, first_key):
    local_files_lst = []
    first_key = int(first_key)
    for root, dirs, files in os.walk(dpath):
        for file in files:
            if int(file.replace('baranova_', '').replace('.csv', '')) < first_key:
                file_path = os.path.join(root, file)
                local_files_lst.append(file_path)

    return local_files_lst


def find_measurements_mean(calibrations_path, data_path, calibration_means_path):
    calibrations = pd.read_csv(calibrations_path)
    calibrations = calibrations[~calibrations['start_time'].isnull()]
    calibrations = calibrations.groupby('date').apply(found_full_calibration)
    calibrations = calibrations.reset_index(drop=True)

    calibrations['start_timestamp'] = calibrations['date'] + ' ' + calibrations['start_time']
    calibrations['start_timestamp'] = pd.to_datetime(calibrations['start_timestamp'])

    calibrations['end_timestamp'] = calibrations['date'] + ' ' + calibrations['end_time']
    calibrations['end_timestamp'] = pd.to_datetime(calibrations['end_timestamp'])

    keys = calibrations['date'].str[:-3].str.replace('-', '')
    keys.drop_duplicates(inplace=True)
    keys = keys.to_list()

    path = data_path
    files_list = file_selector(path, keys)

    calibrations_lst = []
    pd.set_option('display.max_rows', None)
    for file in files_list:
        frame = pd.read_csv(file, sep=';')
        frame = frame[['DATE', 'TIME', 'CO2_dry', 'CH4_dry', 'flag']].copy()
        frame['datetime'] = frame['DATE'] + ' ' + frame['TIME']
        frame['datetime'] = pd.to_datetime(frame['datetime'])
        frame.loc[(frame['flag'] == 4), ['CO2_dry', 'CH4_dry']] = np.NaN
        frame.loc[(frame['CO2_dry'] == -999.9) | (frame['CH4_dry'] == -999.9), ['CO2_dry', 'CH4_dry']] = np.NaN

        selected_calibrations = calibrations[(calibrations['start_timestamp'] >= frame['datetime'].iloc[0]) &
                                             (calibrations['end_timestamp'] <= frame['datetime'].iloc[-1])]

        if selected_calibrations.empty:
            print("no data while calibration period")
        else:
            start_calibration_time = min(selected_calibrations['start_timestamp'])
            end_calibration_time = max(selected_calibrations['end_timestamp'])

            selected_calib_data = pd.DataFrame({'start_timestamp': start_calibration_time - pd.Timedelta(minutes=2),
                                                'end_timestamp': end_calibration_time + pd.Timedelta(minutes=5)}, index=[0])

            frame = frame[(frame['datetime'] >= selected_calib_data['start_timestamp'].iloc[0]) &
                          (frame['datetime'] <= selected_calib_data['end_timestamp'].iloc[0])]

            null_cnt = frame['CO2_dry'].isnull().sum()
            total_cnt = len(frame)

            percentage = null_cnt / total_cnt
            if percentage < 0.4:
                frame = frame.dropna(subset=['CO2_dry'])
                frame['null_percentage'] = round(percentage, 2)
                frame['int_part_co2'] = frame['CO2_dry'].astype(int)
                concentration_counts = frame['int_part_co2'].value_counts()
                top_concentrations = concentration_counts.nlargest(4).index

                if len(top_concentrations) == 4:
                    frame = frame[frame['int_part_co2'].isin(top_concentrations)]
                    frame.drop(['DATE', 'TIME', 'datetime'], axis=1, inplace=True)
                    frame = frame.groupby('int_part_co2').mean()
                    frame['start_time'] = start_calibration_time
                    frame['end_time'] = end_calibration_time

                    if not frame.empty:
                        calibrations_lst.append(frame)
                else:
                    print('\n\nALERT\n')

    calibration_means = pd.concat(calibrations_lst, ignore_index=True)
    calibration_means.drop(['flag'], axis=1, inplace=True)
    calibration_means.to_csv(calibration_means_path, index=False)


# date, multiply_coefficient, sum_coefficient
def calibration_coefficient_count(balloons_values_path, calibration_means_path, calibration_save_path):
    balloons_values = pd.read_csv(balloons_values_path, sep=';')
    calibration_means = pd.read_csv(calibration_means_path)

    calibration_times = calibration_means[['start_time', 'end_time']]
    calibration_times = calibration_times.drop_duplicates()
    calibration_times = calibration_times.reset_index(drop=True)

    calibration_means['calibration_time'] = pd.to_datetime(calibration_means['start_time']).dt.date

    calibration_means.drop(['start_time', 'end_time'], axis=1, inplace=True)
    balloons_values['date_from'] = pd.to_datetime(balloons_values['date_from']).dt.date

    calibration_means['a_coefficient_CO2'] = np.NaN
    calibration_means['b_coefficient_CO2'] = np.NaN

    calibration_means['a_coefficient_CH4'] = np.NaN
    calibration_means['b_coefficient_CH4'] = np.NaN

    balloons_values = balloons_values.sort_values(['date_from', 'CO2'])
    calibration_means = calibration_means.sort_values(['calibration_time', 'CO2_dry'])
    calibration_means['cond_balloon_num'] = calibration_means.groupby('calibration_time').cumcount() + 1
    calibration_means['balloon_date'] = calibration_means['calibration_time'].apply(lambda x: min(x, balloons_values['date_from'].max()))

    union_data = pd.merge(calibration_means, balloons_values, how='left', left_on=['cond_balloon_num', 'balloon_date'],
                          right_on=['balloon_number', 'date_from']).drop(['cond_balloon_num', 'CO2_delta', 'CH4_delta'], axis=1)

    union_data = union_data.groupby('calibration_time').apply(find_coefficients)
    union_data = union_data[['calibration_time', 'a_coefficient_CO2', 'b_coefficient_CO2',
                             'a_coefficient_CH4', 'b_coefficient_CH4']].dropna()
    pd.set_option('display.max_columns', None)

    calibration_times['date'] = pd.to_datetime(calibration_times['start_time']).dt.date
    calibration_times['date'] = pd.to_datetime(calibration_times['date'])
    union_data.rename(columns={'calibration_time': 'calibration_date'}, inplace=True)
    union_data = union_data.reset_index(drop=True)
    union_data['calibration_date'] = pd.to_datetime(union_data['calibration_date'])
    all_union = pd.merge(calibration_times, union_data, how='inner', left_on='date',
                         right_on='calibration_date') .drop(['calibration_date', 'date'], axis=1)
    all_union.to_csv(calibration_save_path, index=False)


def garbage_adding(frame, garbage_data):
    garbage_data['start_time'] = pd.to_datetime(garbage_data['start_time'])
    garbage_data['end_time'] = garbage_data['start_time'] + pd.Timedelta(hours=3)
    for index, row in garbage_data.iterrows():
        frame.loc[(frame['datetime'] >= row['start_time']) &
                  (frame['datetime'] <= row['end_time']) &
                  (frame['flag'] != 5), 'flag'] = 3


def wind_adding(frame, wind_data):
    wind_data = wind_data[(wind_data['WS10A'] < 3) | ((wind_data['WDI'] >= 95) & (wind_data['WDI'] <= 145))]
    frame.loc[(frame['datetime'].isin(wind_data['datetime'])) & (frame['flag'] <= 1), 'flag'] = 2
    print(frame[frame['flag'] == 2])


def add_calibrations(postcalibration_path, calibration_coefficients_path, data_path, garbage_file):
    calibration_coefficients = pd.read_csv(calibration_coefficients_path)
    files_lst = all_file_selector(data_path)

    calibration_coefficients['start_time'] = pd.to_datetime(calibration_coefficients['start_time'])
    calibration_coefficients['end_time'] = pd.to_datetime(calibration_coefficients['end_time'])

    garbage_frame = pd.read_csv(garbage_file)

    for file in files_lst:
        file_name = file.split('\\')[-1]
        print(file_name)
        frame = pd.read_csv(file, sep=';')
        frame['datetime'] = frame['DATE'] + ' ' + frame['TIME']
        frame['datetime'] = pd.to_datetime(frame['datetime'])

        frame['CO2_corrected'] = frame['CO2_dry']
        frame['CH4_corrected'] = frame['CH4_dry']

        max_i = len(calibration_coefficients) - 1

        for index, row in calibration_coefficients.iterrows():
            mask_add_flag = ((frame['datetime'] >= row['start_time']) &
                             (frame['datetime'] <= row['end_time']) &
                             (frame['flag'] != 4))
            if index < max_i:
                mask_add_coef = ((frame['datetime'] > row['end_time']) &
                                 (frame['datetime'] < calibration_coefficients['end_time'].iloc[index + 1]))
            else:
                mask_add_coef = (frame['datetime'] > row['end_time'])

            frame.loc[mask_add_flag, 'flag'] = 5
            frame.loc[mask_add_coef, 'CO2'] = row['a_coefficient_CO2'] * frame.loc[mask_add_coef, 'CO2_dry'] + row[
                'b_coefficient_CO2']
            frame.loc[mask_add_coef, 'CH4'] = row['a_coefficient_CH4'] * frame.loc[mask_add_coef, 'CH4_dry'] + row[
                'b_coefficient_CH4']

        frame = frame[['datetime', 'CO2_dry', 'std_CO2_dry', 'CH4_dry', 'std_CH4_dry', 'flag']]
        garbage_adding(frame, garbage_frame)
        frame = frame.fillna(-999.9)
        frame.to_csv(postcalibration_path + '\\' + file_name, sep=';', index=False)


def process_not_calibrated(processed_path, calibration_coefficients_file, garbage_file):
    processed_path = processed_path

    calibration_coefficients = pd.read_csv(calibration_coefficients_file)
    first_key = calibration_coefficients['start_time'].iloc[0][:7].replace('-', '')

    garbage_frame = pd.read_csv(garbage_file)

    files = only_flag_file_selector(processed_path, first_key)

    for file in files:
        file_name = file.split('\\')[-1]
        print(file_name)
        frame = pd.read_csv(file, sep=';')
        frame['datetime'] = frame['DATE'] + ' ' + frame['TIME']
        frame['datetime'] = pd.to_datetime(frame['datetime'])

        frame['CO2_corrected'] = np.NaN
        frame['CH4_corrected'] = np.NaN

        garbage_adding(frame, garbage_frame)













