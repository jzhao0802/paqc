import pandas as pd
from paqc.report import report as rp
from paqc.utils import utils
from os.path import join


def qc6(df, dict_config):
    """
    Checks that no columns are a 100% empty.
    :param df: The qc-ed dataframe
    :param dict_config: Meta-data in dictionary
    :return: ReportItem
    """

    # List with names of all empty columns
    names_empty_cols = df.columns[df.isnull().all(axis=0)].tolist()

    if not names_empty_cols:
        return rp.ReportItem(passed=True, **dict_config['qc'])
    else:
        path_csv = join(dict_config['general']['output_dir'],"qc6.csv")
        utils.write_list_to_csv(ls_items=names_empty_cols, path_csv=path_csv)
        return rp.ReportItem(passed=False, **dict_config['qc'],
                text=utils.generate_short_string(names_empty_cols))