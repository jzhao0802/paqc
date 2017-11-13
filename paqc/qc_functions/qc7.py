import pandas as pd

from paqc.report import report as rp


def qc7(df, dict_config):
    """
    Checks that no rows are a 100% empty.
    :param df: The qc-ed dataframe
    :param dict_config: Meta-data in dictionary
    :return: ReportItem
    """

    # List with index of all empty rows
    ls_empty_rows = df.index[df.isnull().all(axis=1)].tolist()

    return rp.ReportItem.init_conditional(ls_empty_rows, dict_config['qc'])






