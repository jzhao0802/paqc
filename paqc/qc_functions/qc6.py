import pandas as pd

from paqc.report import report as rp


def qc6(df, dict_config):
    """
    Checks for columns that are a 100% empty.

    :param df: The qc-ed dataframe
    :param dict_config: Meta-data in dictionary
    :return: ReportItem
    """

    # List with names of all empty columns
    ls_empty_cols = df.columns[df.isnull().all(axis=0)].tolist()

    return rp.ReportItem.init_conditional(ls_empty_cols, dict_config['qc'])
