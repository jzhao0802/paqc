import pandas as pd
import numpy as np
import re

from paqc.report import report as rp
from paqc.utils import utils


def qc20(df, dict_config):
    """
    Checks that dataset has only patients in it who meet at least one of the
    selection criteria listed in CC01_CP

    :param df:
    :param dict_config:
    :return:
    """
    ls_cc01_cp = utils.generate_cc0_lists(dict_config)[1]
    prog = re.compile("(" + ")|(".join(ls_cc01_cp) + ")")

    dict_features = utils.generate_dict_grouped_columns(df, dict_config,
                                                        ['flag_cols'])
    ls_cc01_cp_flag_cols = [dict_feat['flag'] for key, dict_feat in
                           dict_features.items() if prog.search(key)]
    ls_idx_faulty = df[~df[ls_cc01_cp_flag_cols].any(axis=1)].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_faulty, dict_config['qc'])


def qc21(df, dict_config):
    """
    Checks that the index_date is always strictly before the
    disease_first_exp_date.

    :param df:
    :param dict_config:
            - Disease_FRST_EXP_DT
    :return:
    """
    index_date_col = dict_config['general']['index_date_col']
    disease_date_col = dict_config['qc']['qc_params']['disease_first_exp_date']

    ls_idx_faulty = df[~(df[index_date_col] < df[disease_date_col])].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_faulty, dict_config['qc'])


def qc22(df, dict_config):
    """
    Checks that all stratification and custom criteria dates are always between
    and including the INDEX_DATE and LOOKBACK_DATE.

    :param df:
    :param dict_config:
    :return:
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
