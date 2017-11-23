import pandas as pd
import numpy as np
import re

from paqc.report import report as rp
from paqc.utils import utils


def qc25(df, dict_config):
    """
    Checks that dataset has NO patients in it who meet at least one of the
    selection criteria listed in CC01_CP.

    :param df:
    :param dict_config:
    :return: ReportItem:
                - self.extra=ls_idx_faulty, the list of indices of all rows
                of patients that have at least one non zero/null value at
                one of the columns related to the selection criteria in CC01_CP
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


def qc26(df, dict_config):
    """
    Checks that disease_first_exp_date is always missing.

    :param df:
    :param dict_config:
                - disease_first_exp_date: the column name of the
                disease_first_exp_date column.
    :return: ReportItem:
                - self.extra=ls_idx_faulty: the indices of rows that have a
                non-zero value for disease_first_exp_date
    """
    disease_date_col = dict_config['qc']['qc_params']['disease_first_exp_date']
    ls_idx_faulty = df[~df[disease_date_col].isnull()].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_faulty, dict_config['qc'])
