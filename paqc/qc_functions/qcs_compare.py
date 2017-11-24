"""
All qcs for test suite 2. Suite 2 is responsible for acceptance of updated
data: these quality checks are designed to check updated data that we might
get from AA or BDF. This ensures that the updated file is as similar to the old
version as possible.
"""
import pandas as pd
import numpy as np

from paqc.report import report as rp
from paqc.utils import utils


def qc46(df_old, df_new, dict_config):
    """
    Tests if two dataframes have the same columns and if they are in the same
    order.

    :param df_old:
    :param df_new:
    :param dict_config:
    :return: ReportItem:
                -self.passed=False when df_old and df_new have do not have the
                same columns OR their columns are in a different order.
                -self.extra={'missing columns':ls_missing_cols,
                             'new columns': ls_new_cols}. A dictionary of
                             two lists when there is a difference in columns.
                           =None, when the columns are the same but in
                           different order.
    """

    ls_cols_1 = list(df_old)
    ls_cols_2 = list(df_new)

    # The dataframes have the same columns in the same order
    if ls_cols_1 == ls_cols_2:
        return rp.ReportItem(passed=True, **dict_config['qc'])
    set_cols_1, set_cols_2 = set(ls_cols_1), set(ls_cols_2)
    # The dataframes have the same columns, but in a different ordering
    if set_cols_1 == set_cols_2:
        return rp.ReportItem(passed=False,
                             text="No missing or new columns, but order changed.",
                             **dict_config['qc'])
    # The dataframes have different columns
    else:
        ls_missing_cols = list(set_cols_1 - set_cols_2)
        ls_new_cols = list(set_cols_2 - set_cols_1)
        return rp.ReportItem.init_conditional({'missing columns': ls_missing_cols,
                                               'new columns': ls_new_cols},
                                              dict_config['qc'])


def qc47(df_old, df_new, dict_config):
    """
    Tests if two dataframes have the same number of rows and patient IDs are in
    the same order.

    :param df_old:
    :param df_new:
    :param dict_config:
    :return: ReportItem:
                -self.passed=False when df_old and df_new have do not have the
                same rows OR their rows are in a different order.
                -self.extra={'missing IDs':ls_missing_ids,
                             'new IDs': ls_new_ids}. A dictionary of
                             two lists when there is a difference in patient IDs.
                           =None, when the rows are the same but in
                           different order.
    """
    ss_id_col_1 = df_old[dict_config['general']['patient_id_col']]
    ss_id_col_2 = df_new[dict_config['general']['patient_id_col']]

    # Dataframes have the same rows in the same order
    if list(ss_id_col_1) == list(ss_id_col_2):
        return rp.ReportItem(passed=True, **dict_config['qc'])
    ls_missing_ids = list(ss_id_col_1[~ss_id_col_1.isin(ss_id_col_2)])
    ls_new_ids = list(ss_id_col_2[~ss_id_col_2.isin(ss_id_col_1)])
    # No missing/new rows in new compared to old. Same rows, order changed.
    if not (ls_missing_ids or ls_new_ids):
        return rp.ReportItem(passed=False, text="No missing or new rows, "
                                                "but order changed.",
                                                **dict_config['qc'])
    # New or missing columns in df_new compared to df_old.
    else:
        return rp.ReportItem.init_conditional({'missing IDs': ls_missing_ids,
                                               'new IDs': ls_new_ids},
                                              dict_config['qc'])


def qc48(df_old, df_new, dict_config):
    """
    Tests if columns in df_new that should have stayed identical to df_old,
    are indeed identical.

    :param df_old:
    :param df_new:
    :param dict_config:
                -dict_config['qc']['qc_params']['list_columns']: list of
                columns to be tested on being identical in both dataframes.
    :return: ReportItem:
                -self.extra=ls_cols_faulty, list of columns of list_columns
                that are not identical over the two dataframes.
                
    """
    # Warns in the report if user forgot to provide needed parameters
    try:
        ls_colnames = dict_config['qc']['qc_params']['list_columns']
    except KeyError:
        return rp.ReportItem(passed=False,
                             text='QC needs extra parameter in qc_params: '
                                  'list_columns', **dict_config['qc'])
    # Part of the dataframes to be tested
    df1 = df_old[ls_colnames]
    df2 = df_new[ls_colnames]

    df_boolean = (df1[ls_colnames] != df2[ls_colnames]) & \
                 (~df1[ls_colnames].isnull() | ~df2[ls_colnames].isnull())
    ss_boolean = df_boolean.any()
    ls_cols_faulty = ss_boolean[ss_boolean].index.tolist()

    return rp.ReportItem.init_conditional(ls_cols_faulty, dict_config['qc'])


def qc49(df_old, df_new, dict_config):
    """
    DESCRIPTIVE STATS:    
    For each column that has changed, it calculate and reports for both
    dateframes:
        - min
        - max
        - median
        - mean
        - fraction of values being zero or missing
    
    Furthermore it adds two more columns that calculate the difference
    between these two dataframes for:
        - median
        - fraction of values being zero or missing

    :param df_old:
    :param df_new:
    :param dict_config:
    :return: ReportItem:
                -self.passed=True. This function never throws an error,
                only delivers descriptive statistics of the changes!
                -self.extra=df_summary, the dataframe with all the
                previously described statistics.
    """

    ss_cols_diff = ((df_old != df_new) & (~df_old.isnull() |
                                          ~df_new.isnull())).any()

    # Calculate the descriptive stats for both dataframes, only for columns
    # that are difference between the two and are datetime or np.number
    dict_dfs = {'old': df_old, 'new': df_new}
    dict_summ_dfs = {}
    for name, df in dict_dfs.items():
        df_diff = df.iloc[:, ss_cols_diff.values].select_dtypes(
                                            include=['datetime', np.number])
        df_summary = df_diff.apply([np.min, np.max, utils.mean_all_types,
                                    utils.median_all_types,
                                    utils.fraction_zeroes_or_null]).transpose()
        df_summary.columns = ['min', 'max', 'mean', 'median', 'null']
        dict_summ_dfs[name] = df_summary

    # Create the difference columns
    ss_diff_null = dict_summ_dfs['new']['null'] - dict_summ_dfs['old']['null']
    ss_diff_med = dict_summ_dfs['new']['median'] - dict_summ_dfs['old']['median']
    dict_summ_dfs['difference'] = pd.concat([ss_diff_med, ss_diff_null], axis=1)
    # Concatenate everything into one dataframe with keys
    df_summary = pd.concat(dict_summ_dfs, axis=1)

    return rp.ReportItem(passed=True, extra=df_summary, **dict_config['qc'])


def qc50(df_old, df_new, dict_config):
    """
    Check if the percentage of missing and zero values across the classes are
    the same in both files within an X% error rate.

    :param df_old:
    :param df_new:
    :param dict_config:
                -dict_config['qc']['qc_params']['max_fraction_diff']: the
                parameter that decides how large the difference in the fraction
                of missing/zero values for df_old and df_new is allowed to be.
    :return: ReportItem:
                -self.extra=ls_cols_high_diff, the list of columns where the
                fraction of 0/missing values changed more between df_old and
                df_new than the provided threshold in one or several classes.
    """
    # Warns in the report if user forgot to provide needed parameters
    try:
        threshold = dict_config['qc']['qc_params']['max_fraction_diff']
    except KeyError:
        return rp.ReportItem(passed=False,
                             text='QC needs extra parameter in qc_params: '
                                  'max_fraction_diff', **dict_config['qc'])

    colname_target = dict_config['general']['target_col']
    dict_summary_dfs = {}
    dict_dfs = {'orig': df_old, 'new': df_new}
    for name, df in dict_dfs.items():
        df_grouped = df.groupby(colname_target)
        dict_summary_dfs[name] = df_grouped.agg(utils.fraction_zeroes_or_null)

    df_diff = np.abs(dict_summary_dfs['new'] - dict_summary_dfs['orig'])
    ss_bool = (df_diff > threshold).any(axis=0)
    ls_cols_high_diff = ss_bool[ss_bool].index.tolist()

    return rp.ReportItem.init_conditional(ls_cols_high_diff, dict_config['qc'])
