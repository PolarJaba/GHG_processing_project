import pandas as pd
import os


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


def process_data(the_file_path, out_folder):
    print('\n\n', the_file_path)
    df = pd.read_csv(the_file_path, sep='\s+', header=None, encoding='cp1251')
    if len(df.columns) == 8:
        df.columns = ['date', 'time', 'concentration', 'units', 'temperature', 'humidity', 'smth', 'added_time']
        df.drop(columns={'smth', 'units'}, inplace=True)
    else:
        df.columns = ['date', 'time', 'concentration', 'humidity', 'added_time']
        df['temperature'] = None

    df['date'] = df['date'].astype(str)
    df['date'] = df['date'].replace(' ', '')
    df.loc[df['date'].str.len() > 8, 'date'] = df['date'].str[:6] + df['date'].str[-2:]
    print(df['date'])
    df['date'] = pd.to_datetime(df['date'], errors='coerce', format='%d.%m.%y')
    df = df.dropna(subset=['date'])

    print(df['date'])
    df['key'] = df['date'].astype(str)
    df['key'] = df['key'].str[:7].str.replace('-', '')

    keys_lst = list(df['key'].drop_duplicates())
    print(df.head(), keys_lst)

    for key in keys_lst:
        frame = df[df['key'] == key]
        frame.drop(['key', 'added_time'], axis=1, inplace=True)
        frame = frame[['date', 'time', 'concentration', 'temperature', 'humidity']]
        frame.to_csv(f'{out_folder}/{key}.csv', header=None, mode='a', index=False)
        print(f"Saved: {key}.csv")


def reorganization_data(files_lst, output_folder):
    output_folder = output_folder

    for file in files_lst:
        process_data(file, output_folder)


def control_n_mean(file_path):
    df = pd.read_csv(file_path, sep=',', header=None, encoding='cp1251')
    df.columns = ['date', 'time', 'concentration', 'temperature', 'humidity']
    print(df.head(1))

