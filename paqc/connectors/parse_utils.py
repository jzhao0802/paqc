import pandas as pd
import numpy as np
from functools import partial
from multiprocessing import cpu_count, Pool
from paqc.utils import utils


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


def check_dates(config, df):
    """
    Util function for certain connectors. It takes in a pandas DataFrame and
    the config file to check if there are any date columns that do not conform
    with the date_format field of the config file. If there are, it converts
    these two to the required date format either doing this in parallel or in
    series, depending on the size of the DataFrame.

    :param config: Parsed YAML config file.
    :param df: Input pandas DataFrame.
    :return: pandas DataFrame with all dates conforming the requested
             date_format of the config file.
    """

    general = config['general']
    date_cols_keys = ['date_cols',
                      'first_exp_date_cols',
                      'last_exp_date_cols',
                      'index_date_col',
                      'lookback_date_col']
    date_cols = utils.generate_list_columns(df, config, date_cols_keys)

    # Check if there are any columns that should be datetime, but aren't
    dtype_date_cols = df.dtypes[date_cols]
    dtype_date_cols = dtype_date_cols[dtype_date_cols.apply(lambda x: not
        np.issubdtype(x, np.datetime64))]

    # List of all date column names that are not in date format yet
    date_cols = dtype_date_cols.index.tolist()

    # Large dataset, conversion done in parallel
    if len(date_cols) > 50 or (df.shape[0] > 20000 and len(date_cols) > 1):
        # make copy of dataframe to do conversion on
        df = df.copy()
        # we have to do this in parallel otherwise it takes forever
        df[date_cols] = apply_parallel(df[date_cols], parse_dates,
                                       format=general['date_format'])

    # Small dataset, faster to convert in non-parallel fashion
    elif len(date_cols) > 0:
        # make copy of dataframe to do conversion on
        df = df.copy()
        df[date_cols] = df[date_cols].apply(pd.to_datetime,
                                            format=general['date_format'])
    return df
