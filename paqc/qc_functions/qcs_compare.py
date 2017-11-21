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
        return rp.ReportItem(passed=False, text="No missing or new columns, "
                                                  "but order changed.",
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
                -list_columns: list of columns to be tested on being
                identical in both dataframes.
    :return: ReportItem:
                -self.extra=ls_cols_faulty, list of columns of list_columns
                that are not identical over the two dataframes.
                
    """

    ls_colnames = dict_config['qc']['qc_params']['list_columns']

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
    For each column that has changed, it calculate and reports the min, 
    median, mean and max values and the percentage of zeros and missing for 
    both df_old.
    
    Furthermore it adds two more columns that compare 
    :param df_old:
    :param df_new:
    :param dict_config:
    :return: ReportItem:
                -self.passed=True. This function never throws an error,
                only delivers descriptive statistics of the changes!
                -self.extra=df_summary, the dataframe with all the
                previously described statistics.
    """
    # TODO: See what to do with median and the second difference function
    # (medianA - medianB)/(0.5*(medianA + medianB))

    ss_cols_diff = ((df_old != df_new) & (~df_old.isnull() |
                                          ~df_new.isnull())).any()
    dict_dfs = {'old': df_old, 'new': df_new}
    dict_summary_dfs = {}

    for name, df in dict_dfs.items():
        df_diff = df.iloc[:, ss_cols_diff.values].select_dtypes(
                                            include=['datetime', np.number])
        # Not done in one apply call due to problems with utils.mean_all_types
        # and utils.fraction_zeroes_or_null and reduce=False.
        # ss_min = df_diff.apply(np.min)
        # ss_max = df_diff.apply(np.max)
        ss_mean = df_diff.apply([utils.mean_all_types])
        ss_nulls = df_diff.apply([utils.fraction_zeroes_or_null]).astype(
                                                                    np.float64)
        df_summary = df_diff.apply([np.min, np.max,
                                    utils.fraction_zeroes_or_null])
    #     dict_summary_dfs[name] = pd.concat([ss_min, ss_max, ss_mean,
    #                                         ss_nulls], axis=1)
    #     dict_summary_dfs[name].columns = ['min', 'max', 'mean', 'missing']
    #
    # ss_diff_missing = dict_summary_dfs['new']['missing'] - dict_summary_dfs[
    #                                                         'old']['missing']
    # df_summary = pd.concat({'original': dict_summary_dfs['old'],
    #                         'new': dict_summary_dfs['new'],
    #                         'difference': ss_diff_missing},
    #                        axis=1)

    return rp.ReportItem(passed=True, extra=(df_summary, ss_mean),
                         **dict_config['qc'])


def qc50(df_old, df_new, dict_config):
    """
    Check if the percentage of missing and zero values across the classes are
    the same in both files within an X% error rate.

    :param df_old:
    :param df_new:
    :param dict_config:
                -max_fraction_diff: the parameter that decides how large the
                difference in the fraction of missing/zero values for df_old
                and df_new is allowed to be.
    :return: ReportItem:
                -self.extra=ls_cols_high_diff, the list of columns where the
                fraction of 0/missing values changed more between df_old and
                df_new than the provided threshold in one or several classes.
    """

    colname_target = dict_config['general']['target_col']
    threshold = dict_config['qc']['qc_params']['max_fraction_diff']

    dict_summary_dfs = {}
    dict_dfs = {'orig': df_old, 'new': df_new}
    for name, df in dict_dfs.items():
        df_grouped = df.groupby(colname_target)
        dict_summary_dfs[name] = df_grouped.agg(utils.fraction_zeroes_or_null)

    df_diff = np.abs(dict_summary_dfs['new'] - dict_summary_dfs['orig'])
    ss_bool = (df_diff > threshold).any(axis=0)
    ls_cols_high_diff = ss_bool[ss_bool].index.tolist()

    return rp.ReportItem.init_conditional(ls_cols_high_diff, dict_config['qc'])
