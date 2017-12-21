import pandas as pd
import numpy as np

from paqc.connectors import parse_utils
from paqc.utils import utils


def parse_dataframe(config, df_input):
    """

    :param config:
    :param df_input:
    :return:
    """
    general = config['general']
    date_cols_keys = ['date_cols',
                       'first_exp_date_cols',
                       'last_exp_date_cols',
                       'index_date_col',
                       'lookback_date_col']
    date_cols = utils.generate_list_columns(df_input, config, date_cols_keys)
    # Check if there are any columns that should be datetime, but aren't
    dtype_date_cols = df_input.dtypes[date_cols]
    dtype_date_cols = dtype_date_cols[dtype_date_cols.apply(lambda x: not
                                                            np.issubdtype(x,
                                                            np.datetime64))]
    # List of all date column names that are not in date format yet
    date_cols = dtype_date_cols.index.tolist()
    # Large dataset, conversion done in parallel
    if len(date_cols) > 50 or (df_input.shape[0] > 20000 and len(date_cols) > 1):
        # make copy of dataframe to do conversion on
        df = df_input.copy()
        # we have to do this in parallel otherwise it takes forever
        df[date_cols] = parse_utils.apply_parallel(df_input[date_cols],
                                                   parse_util.parse_dates,
                                                   format=general['date_format'])
    # Small dataset, faster to convert in non-parallel fashion
    elif len(date_cols) > 0:
        # make copy of dataframe to do conversion on
        df = df_input.copy()
        df[date_cols] = df_input[date_cols].apply(pd.to_datetime,
                                                  format=general['date_format'])
    # No date columns to convert
    else:
        return df_input

    return df
