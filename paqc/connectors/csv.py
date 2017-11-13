import pandas as pd


def read_csv_header(input_file_path):
    return pd.read_csv(input_file_path, nrows=0).columns


def read_csv(config, input_file_path):
    header = read_csv_header(input_file_path)
    general = config['general']
    if general['parse_all_dates'] == True:
        general['date_format']



pd.read_csv(input_file_path)