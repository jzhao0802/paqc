import pandas as pd
import numpy as np
import re

from paqc.report import report as rp
from paqc.utils import utils


def qc1(df, dict_config):
    """
    Testing if all column names of dataframe have no special characters and no
    spaces. They should only contain letters, numbers and underscores.

    :param df:
    :param dict_config:
    :return: ReportItem:
                - self.extra=ls_colnames_faulty, the list of column names
                that have special characters, spaces, numbers and underscores.
    """

    # Pattern matches any non \w character or fully empty string
    prog = re.compile(r'[^\w]+|^[\w]{0}$')
    it_match = (prog.search(colname) for colname in df.columns)
    ls_colnames_faulty = [el.string for el in it_match if el is not None]

    return rp.ReportItem.init_conditional(ls_colnames_faulty, dict_config['qc'])


def qc3(df, dict_config):
    """
    All columns ending in FLAG, COUNT or FREQ should be 0 or positive, and
    never missing.

    :param df: The qc-ed dataframe
    :param dict_config: Meta-data in dictionary
    :return: ReportItem:
                - self.extra=ls_cols_faulty, the list of column names of all
                FLAG/COUNT/FREQ columns that have missing or negative elements.
    """

    ls_colnames = utils.generate_list_columns(df, dict_config,
                                                ['flag_cols', 'freq_cols',
                                                 'count_cols'])
    ls_cols_faulty = []
    for colname in ls_colnames:
        if pd.api.types.is_numeric_dtype(df[colname]):
            if (~(df[colname] >= 0)).any():
                ls_cols_faulty.append(colname)
            else:
                pass
        else:
            ls_cols_faulty.append(colname)

    return rp.ReportItem.init_conditional(ls_cols_faulty, dict_config['qc'])


def qc4(df, dict_config):
    """
    No duplicate patient IDs within the same cohort file.

    In the case of several emtpy patient IDs (NaN according to pandas
    definition), these will NOT be considered as duplicates. Different qc
    for looking for NaN patient_id.

    :param df: The qc-ed dataframe
    :param dict_config: Meta-data in dictionary
    :return: ReportItem:
                - self.extra=ls_idx_duplicateID, the indices of
                the patient rows with duplicate patient IDs.
    """

    patient_id = dict_config['general']['patient_id_col']
    ls_idx_duplicateID = df[patient_id][df[patient_id].duplicated(
        keep=False) & ~df[patient_id].isnull()].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_duplicateID, dict_config['qc'])


def qc6(df, dict_config):
    """
    Checks for columns that are a 100% empty.

    :param df: The qc-ed dataframe
    :param dict_config: Meta-data in dictionary
    :return: ReportItem:
                - Self.extra=ls_cols_empty, the list of names of all columns
                that are completely empty.
    """

    # List with names of all empty columns
    ls_cols_empty = df.columns[df.isnull().all(axis=0)].tolist()

    return rp.ReportItem.init_conditional(ls_cols_empty, dict_config['qc'])


def qc7(df, dict_config):
    """
    Checks that for rows that are a 100% empty.

    :param df: The qc-ed dataframe
    :param dict_config: Meta-data in dictionary
    :return: ReportItem:
                - self.extra=ls_idx_empty, the list indices of all rows that
                are completely empty.
    """

    # List with index of all empty rows
    ls_idx_empty = df.index[df.isnull().all(axis=1)].tolist()

    return rp.ReportItem.init_conditional(ls_idx_empty, dict_config['qc'])


def qc8(df, dict_config):
    """
    Checks that all columns end with one of the official suffixes in the config,
    or are equal to the gender, target, patient_id, matched_patient_id cols, or
    one of the special columns.

    :param df: The qc-ed dataframe
    :param dict_config: Meta-data in dictionary
    :return: ReportItem:
                - self.extra=ls_colnames_nomatch, the list of column names that
                 do not fit one of the allowed column names or suffixes.
    """

    keys_colnames = ['target_col', 'patient_id_col', 'matched_patient_id_col',
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
    ls_colnames_nomatch = [colname for colname in df.columns if not
                                                        prog.search(colname)]

    return rp.ReportItem.init_conditional(ls_colnames_nomatch, dict_config[
        'qc'])


def qc9(df, dict_config):
    """
    Checks that all first and last exposure dates are before or on the index
    date.

    :param df:
    :param dict_config:
    :return: ReportItem:
                - self.extra=ls_cols_faulty, the list of names of all
                first_exp_date_cols and last_exp_date_cols that contain any
                date that is after the index date.
    """

    ls_colnames = utils.generate_list_columns(df, dict_config,
                                              ['first_exp_date_cols',
                                               'last_exp_date_cols'])
    index_date_colname = dict_config['general']['index_date_col']
    ss_cols_faulty = df[ls_colnames].apply(lambda x: x > df[
        index_date_colname]).any()
    ls_cols_faulty = ss_cols_faulty[ss_cols_faulty].index.tolist()

    return rp.ReportItem.init_conditional(ls_cols_faulty, dict_config[
        'qc'])


def qc10(df, dict_config):
    """
    Checks that all first and last exposure date columns only have dates that
    are after or on the lookback date. 

    :param df:
    :param dict_config:
    :return: ReportItem:
                - self.extra=ls_cols_faulty, the list of names of all
                first_exp_date_cols and last_exp_date_cols that contain any
                date that is before the lookback date.
    """

    ls_colnames = utils.generate_list_columns(df, dict_config,
                                              ['first_exp_date_cols',
                                               'last_exp_date_cols'])
    lookback_date_colname = dict_config['general']['lookback_date_col']
    ss_cols_faulty = df[ls_colnames].apply(lambda x: x < df[
        lookback_date_colname]).any()
    ls_cols_faulty = ss_cols_faulty[ss_cols_faulty].index.tolist()

    return rp.ReportItem.init_conditional(ls_cols_faulty, dict_config[
        'qc'])


def qc11(df, dict_config):
    """
    All first exposure dates are before their last exposure date, unless
    corresponding count=1 in which case they are equal.

    :param df:
    :param dict_config:
    :return: ReportItem:
                - self.extra=ls_features_faulty, the list of all features
                that have at least one row where last_exp_date is after
                first_exp_date or where first_exp_date == last_exp_date and
                count bigger than 1.
    """

    dict_grouped_cols = utils.generate_dict_grouped_columns(df, dict_config,
                                                    ['first_exp_date_cols',
                                                    'last_exp_date_cols',
                                                     'count_cols'])
    # Only run the test on predictors that have all those three columns
    dict_grouped_cols = {predictor: dict_grouped for
                         predictor, dict_grouped in dict_grouped_cols.items()
                         if (len(dict_grouped) == 3)}

    ls_features_faulty = []
    for feat, dict_feat in dict_grouped_cols.items():
        # Column contains at least one row where first_exp is after last_exp
        if (df[dict_feat['first_exp_date']] > df[dict_feat['last_exp_date']]).any():
            ls_features_faulty.append(feat)
        # Or at least one row where first_exp is at last_exp and count is not 1
        elif ((df[dict_feat['first_exp_date']] == df[dict_feat[
                'last_exp_date']]) & ~(df[dict_feat['count']] == 1)).any():
            ls_features_faulty.append(feat)

    return rp.ReportItem.init_conditional(ls_features_faulty, dict_config['qc'])
