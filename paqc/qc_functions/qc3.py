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
    ls_faulty_columns = []
    for colname in ls_colnames:
        if pd.api.types.is_numeric_dtype(df[colname]):
            if (~(df[colname] >= 0)).any():
                ls_faulty_columns.append(colname)
            else:
                pass
        else:
            ls_faulty_columns.append(colname)

    return rp.ReportItem.init_conditional(ls_faulty_columns, dict_config['qc'])





