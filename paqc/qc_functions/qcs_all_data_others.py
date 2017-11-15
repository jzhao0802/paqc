import pandas as pd
import numpy as np
import re

from paqc.report import report as rp
from paqc.utils import utils


def qc14(df, dict_config):
    """
    Checks for missing patient IDs.

    :param df:
    :param dict_config:
    :return: ReportItem:
                - self.extra=ls_idx_missing_id, the list of indices of rows
                that miss a patient ID.
    """

    patient_id_col = dict_config['general']['patient_id_col']
    ls_idx_missing_id = df[df[patient_id_col].isnull()].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_missing_id, dict_config['qc'])


def qc15(df, dict_config):
    """
    Tests if all columns expected to be numeric are actually numeric (
    according to the pd.read_csv function of pandas).

    :param df:
    :param dict_config:
    :return:
    """

    # yaml keys for all columns that are supposed to be numeric
    ls_keys = ['count_cols', 'freq_cols', 'flag_cols', 'age_col']

    ls_colnames = utils.generate_list_columns(df, dict_config, ls_keys)
    ls_cols_not_numeric = [colname for colname in ls_colnames if
                           not pd.api.types.is_numeric_dtype(df[colname])]

    return rp.ReportItem.init_conditional(ls_cols_not_numeric, dict_config['qc'])


def qc16(df, dict_config):
    """
    Finds the columns for which the fraction of values that is missing or 0
    differs less over the two classes than the provided threshold, provided
    as max_fraction_diff.

    :param df:
    :param dict_config:
                - max_fraction_diff: the parameter that decides how
                different the fraction of missing/zero values for the two
                different classes is allowed to be.
    :return: ReportItem:
                - self.extra=ls_cols_high_dif, the list of names of all
                columns that pass the previously explained threshold.
                - self.qc_params={max_fraction_diff:value}
    """

    colname_target = dict_config['general']['target_col']
    threshold = dict_config['qc']['qc_params']['max_fraction_diff']

    df_grouped = df.groupby(colname_target)
    df_size = df_grouped.size()
    df_sum = df_grouped.agg(lambda x: np.sum((x == 0) | pd.isnull(x)))
    df_fract = df_sum.div(np.array(df_size), axis='index')
    df_dif_high = abs(df_fract.iloc[0] - df_fract.iloc[1]) > threshold
    ls_cols_high_dif = df_dif_high[df_dif_high].index.tolist()

    return rp.ReportItem.init_conditional(ls_cols_high_dif, dict_config['qc'])
