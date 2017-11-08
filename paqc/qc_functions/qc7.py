import pandas as pd
from os.path import join
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
        path_csv = join(dict_config['general']['output_dir'], "qc7.csv")
        utils.write_list_to_csv(ls_items=idx_empty_rows, path_csv=path_csv)
        return rp.ReportItem(passed=False, **dict_config['qc'],
                text=utils.generate_short_string(idx_empty_rows))






