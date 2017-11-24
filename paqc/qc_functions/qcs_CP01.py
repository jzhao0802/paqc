import pandas as pd
import numpy as np
import re

from paqc.report import report as rp
from paqc.utils import utils


def qc22(df, dict_config):
    """
    Checks that dataset has only patients in it who meet at least one of the
    selection criteria listed in CC01_CP

    :param df:
    :param dict_config:
    :return: ReportItem:
                - self.extra=ls_idx_faulty, the indices of the rows that do
                not meet any of the criteria listed in CC01_CP
    """
    ls_cc01_cp = utils.generate_cc0_lists(dict_config)[1]
    prog = re.compile("(" + ")|(".join(ls_cc01_cp) + ")")

    dict_features = utils.generate_dict_grouped_columns(df, dict_config,
                                                        ['flag_cols'])
    ls_cc01_cp_flag_cols = [dict_feat['flag'] for key, dict_feat in
                           dict_features.items() if prog.search(key)]
    ls_idx_faulty = df[~df[ls_cc01_cp_flag_cols].any(axis=1)].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_faulty, dict_config['qc'])


def qc23(df, dict_config):
    """
    Checks that the index_date is always strictly before the disease_first_exp
    _date.

    :param df:
    :param dict_config:
                - dict_config['qc']['qc_params']['disease_first_exp_date']:
                the column name of the disease_first_exp_date column.
    :return: ReportItem:
                - self.extra=ls_idx_faulty, the list of indices of rows
                where index_date is not strictly before disease_first_exp_date
    """
    # Warns in the report if user forgot to provide needed parameters
    try:
        disease_date_col = dict_config['qc']['qc_params'][
                                             'disease_first_exp_date']
    except KeyError:
        return rp.ReportItem(passed=False,
                             text='QC needs extra parameter in qc_params: '
                                  'disease_first_exp_date')

    index_date_col = dict_config['general']['index_date_col']
    ls_idx_faulty = df[~(df[index_date_col] < df[disease_date_col])].index.\
                                                                     tolist()

    return rp.ReportItem.init_conditional(ls_idx_faulty, dict_config['qc'])


def qc24(df, dict_config):
    """
    Checks that all stratification and custom criteria dates are always between
    and including the INDEX_DATE and LOOKBACK_DATE.

    :param df:
    :param dict_config:
    :return: ReportItem:
                - self.extra=ls_idx_faulty, the list of indices of columns
                where at least one criteria has a date not between
                index_date and lookback_date.
    """
    ss_index_date = df[dict_config['general']['index_date_col']]
    ss_lookback_date = df[dict_config['general']['lookback_date_col']]
    dict_features = utils.generate_dict_grouped_columns(df, dict_config,
                                                        ['first_exp_date_cols',
                                                         'last_exp_date_cols'])
    ls_cc03_cp = utils.generate_cc0_lists(dict_config)[3]

    # Take the date columns of all features part of cc03_cp
    prog = re.compile("(" + ")|(".join(ls_cc03_cp) + ")")
    ls_cc03_cp_dt_cols = [dict_feat.values() for key, dict_feat in
                          dict_features.items() if prog.search(key)]
    # flatten the list
    ls_cc03_cp_dt_cols = [item for sublist in ls_cc03_cp_dt_cols for
                          item in sublist]
    # select the indices of rows where any of the _first_exp_date or
    # _last_exp_date is out of the range
    df_boolean = df[ls_cc03_cp_dt_cols].apply(lambda x: (x > ss_index_date)
                                                      | (x < ss_lookback_date))
    ls_idx_faulty = df_boolean[df_boolean.any(axis=1)].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_faulty, dict_config['qc'])
