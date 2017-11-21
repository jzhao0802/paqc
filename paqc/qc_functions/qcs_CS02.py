import pandas as pd
import numpy as np
import re

from paqc.report import report as rp
from paqc.utils import utils


def qc23(df, dict_config):
    """
    Checks that dataset has NO patients in it who meet at least one of the
    selection criteria listed in CC01_CP.

    :param df:
    :param dict_config:
    :return:
    """
    ls_cc01_cp = utils.generate_cc0_lists(dict_config)[1]
    dict_features = utils.generate_dict_grouped_columns(df, dict_config,
                                                        ['flag_cols',
                                                         'freq_cols',
                                                         'first_exp_date_cols',
                                                         'last_exp_date_cols',
                                                         'count_cols'])
    # List with all columns related to descriptions part of CC01_CP
    prog = re.compile("(" + ")|(".join(ls_cc01_cp) + ")")
    ls_cc01_cp_cols = [dict_feat.values() for key, dict_feat in
                            dict_features.items() if prog.search(key)]
    # Flatten list
    ls_cc01_cp_cols = [item for sublist in ls_cc01_cp_cols for item in sublist]
    ss_boolean = df[ls_cc01_cp_cols].apply(lambda x: ~utils.is_zero_or_null(
        x)).any(axis=1)
    ls_idx_faulty = ss_boolean[ss_boolean].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_faulty, dict_config['qc'])


def qc24(df, dict_config):
    """
    Checks that Disease_FRST_EXP_DT is always missing.

    :param df:
    :param dict_config:
    :return:
    """
    disease_date_col = dict_config['qc']['qc_params']['Disease_FRST_EXP_DT']
    ls_idx_faulty = df[~df[disease_date_col].isnull()].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_faulty, dict_config['qc'])


