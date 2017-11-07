import pandas as pd
import paqc.report.report as rp

def qc7(df, dict_config):
    """

    :param df:
    :return:
    """

    idx_empty_rows = df.index[df.isnull().all(1)].tolist()

    if not idx_empty_rows:
        return rp.ReportItem(passed=True, dict_config['qc'])

    return output
