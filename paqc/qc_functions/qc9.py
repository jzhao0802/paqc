import pandas as pd
import re

from paqc.report import report as rp
from paqc.utils import utils


def qc9(df, dict_config):
    """
    All first and last exposure dates are before or on the index date.

    :param df: The qc-ed dataframe
    :return: True/False
    """

    ls_colnames = utils.generate_list_columns(df, dict_config,
                                              ['first_exp_date_cols',
                                               'last_exp_date_cols'])
    index_date_colname = dict_config['general']['index_date_col']
    ss_colnames_faulty = df[ls_colnames].apply(lambda x: x > df[
        index_date_colname]).any()
    ls_colnames_faulty = ss_colnames_faulty[ss_colnames_faulty].index.tolist()

    return rp.ReportItem.init_conditional(ls_colnames_faulty, dict_config[
        'qc'])
