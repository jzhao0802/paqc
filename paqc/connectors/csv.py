import pandas as pd
from functools import partial
from multiprocessing import cpu_count, Pool
from paqc.utils import utils
from paqc.connectors import parse_utils


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
    Depending on the size of the dataset, and the number of date columns, we
    might use parallelisation to speed things up.

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
    # convert string dates to dates using the date format
    # Large dataset, conversion done in parallel
    if len(date_cols) > 50 or (df.shape[0] > 20000 and len(date_cols) > 1):
        print('parallel!')
        # we have to do this in parallel otherwise it takes forever
        df[date_cols] = parse_utils.apply_parallel(df[date_cols],
                                                   parse_utils.parse_dates,
                                                   format=general['date_format'])
    # Small dataset, faster to convert in non-parallel fashion
    elif len(date_cols) > 0:
        df[date_cols] = df[date_cols].apply(pd.to_datetime,
                                            format=general['date_format'])
    # No date columns to convert
    else:
        pass

    return df

