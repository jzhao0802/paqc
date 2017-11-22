import pandas as pd
from paqc.utils import utils


def read_csv_header(input_file_path):
    """
    Reads in the header of a csv file.

    :param input_file_path: Absolute path to the csv file.
    :return: pandas DataFrame without rows, but with columns.
    """
    return pd.read_csv(input_file_path, nrows=0)


def read_csv(config, input_file_path):
    """
    Reads in a csv file, and attempts to use the date format that is specified
    in the config file. The date columns it uses are the ones that are specified
    by the date_cols, first_exp_date_cols, last_exp_date_cols, index_date_col
    and lookback_date_col params in the general section of the YAML config.

    :param config: Parsed YAML config file.
    :param input_file_path: Absolute path to the csv file.
    :return: Tuple: first is a Boolean whether loading and parsing was
    successful, second is the pandas DataFrame if Bool=True.
    """
    header = read_csv_header(input_file_path)
    general = config['general']
    date_cols_types = ['date_cols',
                       'first_exp_date_cols',
                       'last_exp_date_cols',
                       'index_date_col',
                       'lookback_date_col']
    date_cols = utils.generate_list_columns(header, config, date_cols_types)
    # it turns out we should read the dates first in as strings
    date_cols_types = {date_col: str for date_col in date_cols}
    df = pd.read_csv(input_file_path, dtype=date_cols_types)
    # and then we can try to convert them using the user supplied format
    df[date_cols] = df[date_cols].apply(pd.to_datetime,
                                        format=general['date_format'])
    return True, df



