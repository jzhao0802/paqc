import pandas as pd
from functools import partial
from multiprocessing import cpu_count, Pool


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
