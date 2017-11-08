import pandas as pd
from os.path import join
from paqc.report import report as rp
from paqc.utils import utils



def qc6(df, dict_config):
    """
    Checks that no columns are a 100% empty.
    :param df: The qc-ed dataframe
    :param dict_config: Meta-data in dictionary
    :return: ReportItem
    """

    # List with names of all empty columns
    ls_empty_cols = df.columns[df.isnull().all(axis=0)].tolist()

    return rp.ReportItem.init_conditional(ls_empty_cols, dict_config['qc'])