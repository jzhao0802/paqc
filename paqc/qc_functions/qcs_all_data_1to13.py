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


def qc3(df, dict_config, missing_is_ok=False):
    """
    All columns ending in FLAG, COUNT or FREQ should be 0 or positive, and
    never missing.

    :param df:
    :param dict_config:
    :param missing_is_ok: sometimes missing should be accepted, set this to True
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
            if missing_is_ok:
                if (df[colname] < 0).any():
                    ls_cols_faulty.append(colname)
                else:
                    pass
            else:
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

    :param df:
    :param dict_config:
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

    :param df:
    :param dict_config:
    :return: ReportItem:
                - Self.extra=ls_cols_empty, the list of names of all columns
                that are completely empty.
    """
    # List with names of all empty columns
    ls_cols_empty = df.columns[df.isnull().all(axis=0)].tolist()

    return rp.ReportItem.init_conditional(ls_cols_empty, dict_config['qc'])


def qc7(df, dict_config):
    """
    Checks for rows that are a 100% empty.

    :param df:
    :param dict_config:
    :return: ReportItem:
                - self.extra=ls_idx_empty, the list indices of all rows that
                are completely empty.
    """
    # List with index of each empty row
    ls_idx_empty = df.index[df.isnull().all(axis=1)].tolist()

    return rp.ReportItem.init_conditional(ls_idx_empty, dict_config['qc'])


def qc8(df, dict_config):
    """
    Checks that all columns end with one of the official suffixes in the config,
    or are equal to the gender, target, patient_id, matched_patient_id cols, or
    one of the special columns.

    :param df:
    :param dict_config:
    :return: ReportItem:
                - self.extra=ls_colnames_nomatch, the list of column names that
                 do not fit one of the allowed column names or suffixes.
    """
    # The 13 main column keys
    keys_colnames = ['target_col', 'patient_id_col', 'matched_patient_id_col',
                     'gender_col', 'date_cols', 'count_cols', 'freq_cols',
                     'flag_cols', 'first_exp_date_cols',
                     'last_exp_date_cols', 'age_col', 'index_date_col',
                     'lookback_date_col']
    # Plus special columns
    if dict_config['general']['special_cols'] is not None:
        ls_allowed = dict_config['general']['special_cols']
    else:
        ls_allowed = []
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

    return rp.ReportItem.init_conditional(ls_colnames_nomatch, dict_config['qc'])


def qc9(df, dict_config,
        keys_columns_a=('first_exp_date_cols', 'last_exp_date_cols'),
        comparison='>', key_column_b='lookback_date_col', axis=0):
    """
    General function to compare chosen date columns with one other date
    column, it tests if columns_a are </<=/>=/> column_b, where the comparison
    operator is defined by the parameter comparison.

    The comparison itself is done by the paqc.utils.utils.compare_date_columns.

    Both columns_a and column_b are accessed by their key in the
    config_file, NOT the column name itself. keys_columns_a should be a
    list, not a single string, while key_column_b should be a key single name,
    not a list.

    The values that do not follow the expected date order can be returned in
    two ways, when axis=0 a list of all columns that have at least one wrong
    value is returned, when axis=1 a list of all row indices with at least
    one wrong value is returned.

    :param df:
    :param dict_config:
    :param keys_columns_a: The list of keys that point to column names that
           need to be tested.
    :param comparison: Comparison operator, possible values: '>', '>=',
           '<=', '<'. ALWAYS USE THE APOSTROPHES.
    :param key_column_b: The single key to the column that all the columns
           of keys_columns_a need to be compared with.
    :param axis: When 0, function returns faulty columns, when 1, faulty rows
    :return: ReportItem:
                - self.extra=ls_faulty, the list of column names/row indices
                that have at least one value that breaks the expected date
                order with column_b.
    """
    ls_colnames_a = utils.generate_list_columns(df, dict_config, keys_columns_a)
    colname_b = dict_config['general'][key_column_b]
    if colname_b in ls_colnames_a:
        ls_colnames_a.remove(colname_b)
    ls_faulty = utils.compare_date_columns(df, ls_colnames_a, comparison,
                                     colname_b, axis=axis)

    return rp.ReportItem.init_conditional(ls_faulty, dict_config['qc'])


def qc10(df, dict_config, lvl1_desc=1, comparison='>',
         key_column_b='lookback_date_col', axis=0):
    """
    General function to compare the first_exp_date and last_exp_date columns of
    chosen criteria (CC01_CP, CC02_CP, CC03_CP, chosen by putting lvl1_desc as
    1,2 or 3 respectively) with one other date column. It tests if the date
    columns belonging to CC0X are </<=/>=/> column_b, where the comparison
    operator is defined by the parameter comparison.

    The comparison itself is done by the paqc.utils.utils.compare_date_columns.

    lvl1_desc should be 1,2 or 3 or a list with several of these values,
    while key_column_b should be a single key name, not a list.

    The values that do not follow the expected date order can be returned in
    two ways, when axis=0 a list of all columns that have at least one wrong
    value is returned, when axis=1 a list of all row indices with at least
    one wrong value is returned.

    :param df:
    :param dict_config:
    :param lvl1_desc: the PROD_CUSTOM_LVL1_DESC, only values 1, 2, 3 or a
           list with several of these are allowed. Decides features
           belonging to which criteria (CC01, CC02 or CC03) are checked.
    :param comparison: Comparison operator, possible values: '>', '>=',
           '<=', '<'. ALWAYS USE THE APOSTROPHES.
    :param key_column_b: The single key to the column that all the columns
           of keys_columns_a need to be compared with.
    :param axis: When 0, function returns faulty columns, when 1, faulty rows
    :return: ReportItem:
                - self.extra=ls_faulty, the list of column names/row indices
                that have at least one value that breaks the expected date
                order with column_b.
    """
    # lvl1_desc can be both a list of levels, or just one level, in which
    # case we put it in a list to make the next part compatible.
    if isinstance(lvl1_desc, int):
        lvl1_desc = [lvl1_desc]

    ls_colnames_a = []
    for i in lvl1_desc:
        ls_colnames_a.extend(utils.generate_list_cc0x_columns(df, dict_config,
                             lvl1_desc=i, list_keys=['first_exp_date_cols',
                                                     'last_exp_date_cols']))
    colname_b = dict_config['general'][key_column_b]
    ls_faulty = utils.compare_date_columns(df, ls_colnames_a, comparison,
                                           colname_b, axis=axis)

    return rp.ReportItem.init_conditional(ls_faulty, dict_config['qc'])


def qc11(df, dict_config, multiple_a_day=True):
    """
    All first exposure dates are before or on their last exposure date.

    When first exposure date is on last exposure date, the qc allows counts
    to be higher than 1 or not, depending on multiple_a_day boolean parameter.

    :param df:
    :param dict_config:
    :param multiple_a_day: Boolean, if True: count is allowed to be higher than
           1 when first_exp_date and last_exp_date are the same.
           if False: rows where count is higher than 1 while first_exp_date and
           last_exp_date are the same, fail the qc. The relevant feature will
           be added to ls_features_faulty.
    :return: ReportItem:
                - self.extra=ls_features_faulty, the list of all features
                that have at least one row where last_exp_date is after
                first_exp_date or where first_exp_date == last_exp_date and
                count bigger than 1.
    """
    # multiple_a_day is provided, but (due to typo?) not a boolean, finish test
    if multiple_a_day not in [True, False]:
        return rp.ReportItem(passed=False,
                             text='multiple_a_day qc_parameter needs to be: '
                                  'True or False', **dict_config['qc'])

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
        # Columns contains at least one row where first_exp is after last_exp
        if (df[dict_feat['first_exp_date']] > df[dict_feat['last_exp_date']]).any():
            ls_features_faulty.append(feat)
        # Or at least one row where first_exp == last_exp but not count == 1,
        # only when multiple_a_day parameter is not True
        elif (~multiple_a_day &
                ((df[dict_feat['first_exp_date']] == df[dict_feat[
                'last_exp_date']]) & (df[dict_feat['count']] > 1)).any()):
            ls_features_faulty.append(feat)
        else:
            pass

    return rp.ReportItem.init_conditional(ls_features_faulty, dict_config['qc'])


def qc12(df, dict_config):
    """
    Checks that if a row has a non missing value for one of the variable types
    (flag, counts, freq, dates) for a feature/predictor, then all corresponding
    columns of that feature/predictor should have non-missing entries for that
    row.

    :param df:
    :param dict_config:
    :return: ReportItem:
                - self.extra=ls_feature_faulty, list of all the features that
                have at least one row where some variables are missing/0 and
                other not.
    """

    dict_grouped_cols = utils.generate_dict_grouped_columns(df, dict_config,
                                                        ['first_exp_date_cols',
                                                         'last_exp_date_cols',
                                                         'count_cols',
                                                         'freq_cols',
                                                         'flag_cols'])
    ls_features_faulty = []
    for feat, dict_feat in dict_grouped_cols.items():
        df_feat = df[list(dict_feat.values())].copy()
        df_feat = df_feat.apply(utils.is_zero_or_null, reduce=False)
        if df_feat.sum(axis=1).between(0, len(dict_feat),
                                       inclusive=False).any():
            ls_features_faulty.append(feat)

    return rp.ReportItem.init_conditional(ls_features_faulty, dict_config['qc'])


def qc13(df, dict_config):
    """
    Checks that when first exposure date is before last exposure date,
    the count for that feature is bigger than 1.

    :param df:
    :param dict_config:
    :return: ReportItem:
                - self.extra=ls_feature_faulty, list of all the features that
                have at least one row where first_exp_date is before
                last_exp_date but count is not bigger than 1.
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
        # Columns contains at least one row where first_exp is before last_exp
        # and count is not bigger than 1.
        if ((df[dict_feat['first_exp_date']] < df[dict_feat[
                'last_exp_date']]) & ~(df[dict_feat['count']] > 1)).any():
            ls_features_faulty.append(feat)

    return rp.ReportItem.init_conditional(ls_features_faulty, dict_config['qc'])
