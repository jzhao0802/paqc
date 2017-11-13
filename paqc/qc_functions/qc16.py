import pandas as pd
import numpy as np

from paqc.report import report as rp


def qc16(df, dict_config):
    """
    Finds the columns for which the fraction of values that is missing or 0
    differs less over the two classes than the provided threshold, provided
    as max_fraction_diff.

    :param df:
    :param dict_config:
    :return:
    """

    colname_target = dict_config['general']['target_col']
    threshold = dict_config['qc']['qc_params']['max_fraction_diff']

    df_grouped = df.groupby(colname_target)
    df_size = df_grouped.size()
    df_sum = df_grouped.agg(lambda x: np.sum((x == 0) | pd.isnull(x)))
    df_fract = df_sum.div(np.array(df_size), axis='index')
    df_dif_high = abs(df_fract.iloc[0] - df_fract.iloc[1]) > threshold
    ls_high_dif = df_dif_high[df_dif_high].index.tolist()

    return rp.ReportItem.init_conditional(ls_high_dif, dict_config['qc'])
