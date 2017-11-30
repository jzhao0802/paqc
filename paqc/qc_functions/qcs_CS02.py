import pandas as pd
import numpy as np
import re

from paqc.report import report as rp
from paqc.utils import utils


def qc25(df, dict_config, path_file_cp01='data/cp02.csv',
         pat_id_col_cp01='patient_id'):
    """
    Checks that dataset has NO patients listed in CP01, this is checked by
    comparing the patient_id columns of both datasets.

    :param df:
    :param dict_config:
    :param path_file_cp01: Absolute path to the cp01 file.
    :param pat_id_col_cp01: Column name of patient_id column in the cp01 file.
    :return: ReportItem:
                - self.extra=ls_pat_ids_faulty: The list of patient ids of
                this dataframe that also show up in the cp01 file.
    """
    ss_pat_ids_cs02 = df[dict_config['general']['patient_id_col']].astype(str)
    # Only load the patient_col, make it a series and both have string type,
    # this avoids false negatives by two equal values being different types
    ss_pat_ids_cp01 = pd.read_csv(path_file_cp01,
                                  dtype={pat_id_col_cp01: str},
                                  usecols=[pat_id_col_cp01]).iloc[:, 0]
    ss_isin_cp01 = ss_pat_ids_cs02.isin(ss_pat_ids_cp01)
    ls_pat_ids_faulty = ss_pat_ids_cs02[ss_isin_cp01].values.tolist()

    return rp.ReportItem.init_conditional(ls_pat_ids_faulty, dict_config['qc'])


def qc26(df, dict_config, diseasefirstexp_col='diseasefirstexp_dt'):
    """
    Checks that disease_first_exp_date is always missing.

    :param df:
    :param dict_config:
    :param diseasefirstexp_col: the column name of the disease_first_exp_date
    column.
    :return: ReportItem:
                - self.extra=ls_idx_faulty: the indices of rows that have a
                non-zero value for disease_first_exp_date
    """
    ls_idx_faulty = df[~df[diseasefirstexp_col].isnull()].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_faulty, dict_config['qc'])
