import pandas as pd
from paqc.report import report as rp
from paqc.utils import utils


def qc8(df, dict_config):
    """
    Checks that all columns end with one of the official suffixes in the config,
    or be equal to the gender, target, patient_id or matched_patient_id cols.
    :param df: The qc-ed dataframe
    :param dict_config: Meta-data in dictionary
    :return: ReportItem
    """

    dict_gen = dict_config['general'].copy()
    list_keys = ['date_cols', 'count_cols', 'freq_cols', 'first_exp_date_cols',
                 'last_exp_date_cols', 'target_col', 'patient_id',
                 'matched_patient_id']






