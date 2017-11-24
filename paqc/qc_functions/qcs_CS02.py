import pandas as pd
import numpy as np
import re

from paqc.report import report as rp
from paqc.utils import utils


def qc25(df, dict_config):
    """
    Checks that dataset has NO patients listed in CP01, this is checked by
    comparing the patient_id columns of both datasets.

    :param df:
    :param dict_config:
                - dict_config['qc']['qc_params']['path_file_cp01']:
                the absolute path to the cp01 file.
                - dict_config['qc']['qc_params']['patient_id_col_cp01']:
                the column name of patient_id column in the cp01 file.
    :return: ReportItem:
                - self.extra=ls_pat_ids_faulty: The list of patient ids of
                this dataframe that also show up in the cp01 file.
    """
    # Warns in the report if user forgot to provide needed parameters
    try:
        path_file_cp01 = dict_config['qc']['qc_params']['path_file_cp01']
        patient_id_col_cp01 = dict_config['qc']['qc_params'][
                                                'patient_id_col_cp01']
    except KeyError:
        return rp.ReportItem(passed=False,
                             text='QC needs extra parameters in qc_params: '
                                  'path_file_cp01 and patient_id_col_cp01',
                             **dict_config['qc'])
    ss_pat_ids = df[dict_config['general']['patient_id_col']].astype(str)
    # Only load the patient_col, make it a series and both have string type,
    # this avoids false negatives by two equal values being different types
    ss_pat_ids_cp01 = pd.read_csv(path_file_cp01,
                                  dtype={patient_id_col_cp01: str},
                                  usecols=[patient_id_col_cp01]).iloc[:, 0]
    ss_isin_cp01 = ss_pat_ids.isin(ss_pat_ids_cp01)
    ls_pat_ids_faulty = ss_pat_ids[ss_isin_cp01].values.tolist()

    return rp.ReportItem.init_conditional(ls_pat_ids_faulty, dict_config['qc'])


def qc26(df, dict_config):
    """
    Checks that disease_first_exp_date is always missing.

    :param df:
    :param dict_config:
                - dict_config['qc']['qc_params']['disease_first_exp_date']:
                the column name of the disease_first_exp_date column.
    :return: ReportItem:
                - self.extra=ls_idx_faulty: the indices of rows that have a
                non-zero value for disease_first_exp_date
    """
    # Warns in the report if user forgot to provide needed parameters
    try:
        disease_date_col = dict_config['qc']['qc_params'][
                                             'disease_first_exp_date']
    except KeyError:
        return rp.ReportItem(passed=False,
                             text='QC needs extra parameter in qc_params: '
                                  'disease_first_exp_date',
                             **dict_config['qc'])

    ls_idx_faulty = df[~df[disease_date_col].isnull()].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_faulty, dict_config['qc'])
