import pandas as pd
from paqc.report import report as rp
from paqc.utils import utils


def qc7(df, dict_config):
    """
    Checks that no rows are a 100% empty.
    :param df: The qc-ed dataframe
    :param dict_config: Meta-data in dictionary
    :return: ReportItem
    """

    # List with index of all empty rows
    idx_empty_rows = df.index[df.isnull().all(axis=1)].tolist()

    if not idx_empty_rows:
        return rp.ReportItem(passed=True, **dict_config['qc'])
    else:
        return rp.ReportItem(passed=False, **dict_config['qc'],
                text=utils.generate_short_string(idx_empty_rows))






