import pandas as pd
import re

from os.path import join

from paqc.report import report as rp
from paqc.utils import utils

def qc3(df, dict_config):
    """
    All columns ending in FLAG, COUNT or FREQ should be 0 or positive, and
    never missing.

    :param df: The qc-ed dataframe
    :param dict_config: Meta-data in dictionary
    :return: ReportItem
    """

    ls_colnames = utils.generate_list_columns(df, dict_config,
                                                ['flag_cols', 'freq_cols',
                                                 'count_cols'])
    ss_col_is_faulty = (~(df[ls_colnames] >= 0)).any()
    ls_faulty_columns = ss_col_is_faulty[ss_col_is_faulty].index.tolist()

    return rp.ReportItem.init_conditional(ls_faulty_columns, dict_config['qc'])





