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
                     'first_exp_date_cols', 'last_exp_date_cols', 'age_col']
    ls_allowed = dict_config['general']['special_cols']
    for key in keys_colnames:
        ls_allowed.append(dict_config['general'][key])

    # Creating a long regex that contains all possible column name structures
    # from ls_allowed
    list_regex = ["%s$" % colname for colname in ls_allowed]
    combined_regex = "(" + ")|(".join(list_regex) + ")"
    prog = re.compile(combined_regex)
    # Collects all column names that don't fit one of the structures
    ls_nomatch = [colname for colname in df.columns if not prog.search(
        colname)]

    return rp.ReportItem.init_conditional(ls_nomatch, dict_config['qc'])
