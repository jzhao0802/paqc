import pandas as pd
from functools import partial
from multiprocessing import cpu_count, Pool
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
    if df.shape[0] > 1000 or len(date_cols) > 50:
        # we have to do this in parallel otherwise it takes forever
        df[date_cols] = apply_parallel(df[date_cols], parse_dates,
                                       format=general['date_format'])
    else:
        df[date_cols] = df[date_cols].apply(pd.to_datetime,
                                            format=general['date_format'])
    return True, df


def parse_dates(column, **kwargs):
    """
    Helper function we'll apply to each date column in parallel to parse dates.
    Don't use this without apply_parallel.

    :param column: DataFrame column, with type str.
    :return: DataFrame column as datatime column with date_format applied.
    """

    return pd.to_datetime(column, **kwargs)


def apply_parallel(df, func, **kwargs):
    """
    Parallelizes the apply function of pandas. Works for columns only.

    :param df: DataFrame to use. Note, all columns are used.
    :param func: Function to apply to each column of df.
    :return: Transformed df.
    """

    # split DataFrame into columns
    df_cols = [df[col] for col in df]

    # use all cores and do parallel magic
    pool = Pool(cpu_count())
    df = pd.concat(pool.map(partial(func, **kwargs), df_cols), axis=1)
    pool.close()
    pool.join()

    return df


