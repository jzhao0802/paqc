import pandas as pd
import re
from os.path import join
from paqc.report import report as rp
from paqc.utils import utils


def qc8(df, dict_config):
    """
    Checks that all columns end with one of the official suffixes in the config,
    or be equal to the gender, target, patient_id, matched_patient_id cols, or
    one of the special columns.
    :param df: The qc-ed dataframe
    :param dict_config: Meta-data in dictionary
    :return: ReportItem
    """

    keys_colnames = ['target_col', 'patient_id', 'matched_patient_id',
                     'gender_col', 'date_cols', 'count_cols', 'freq_cols',
                     'first_exp_date_cols', 'last_exp_date_cols',
                     'special_col', 'age_col']
    ls_allowed = []
    for key in keys_colnames:
        ls_allowed.append(dict_config['general'][key])

    list_regex = ["%s$" % colname for colname in ls_allowed]
    combined_regex = "(" + ")|(".join(list_regex) + ")"
    prog = re.compile(combined_regex)
    ls_nomatch = [colname for colname in df.columns if not prog.match(
        colname)]

    if not ls_nomatch:
        return rp.ReportItem(passed=True, **dict_config['qc'])
    else:
        path_csv = join(dict_config['general']['output_dir'],"qc8.csv")
        utils.write_list_to_csv(ls_items=ls_nomatch, path_csv=path_csv)
        return rp.ReportItem(passed=False, **dict_config['qc'],
                             text=utils.generate_short_string(ls_nomatch))










