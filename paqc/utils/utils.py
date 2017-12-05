"""
Various general helper functions for the QC package.
"""
from collections import defaultdict
import pandas as pd
import numpy as np
import re
import operator


def generate_hash(df):
    """
    Given a pandas DataFrame, this function will generate a unique hash value
    of it. This enables us to precisely identify which version of a data file
    the test was carried out on.

    :param df: Pandas DataFrame object.
    :return: Hash integer that uniquely maps to a certain DataFrame.
    """

    return hash(df.values.tobytes())


def write_list_to_csv(ls_items, path_csv):
    """
    Creates a csv file with the list items in a single column

    :param ls_items: input list
    :param path_csv: path of the csv file to be written to
    :return: None
    """
    df_tocsv = pd.DataFrame(ls_items)
    df_tocsv.to_csv(path_csv, index=False, header=False)


def compare_date_columns(df, ls_colnames_a, comparison, colname_b, axis):
    """
    Searches for cells in date columns where:
            df[ls_colnames_a]   comparison  df[colname_b]
                               >, >=, <=, <
    Is not followed.

    e.g. If you expect all lookback dates to be strictly smaller than the
    index dates, then compare_date_columns(df, ['lookback_dt'], <, 'index_dt',
    axis =0) will return an empty list if all lookback dates are strictly
    smaller than their index date, while it will return ['lookback_dt'] if
    at least one loockback date is greater or equal than the index date.

    :param df:
    :param ls_colnames_a: The list of columns that needed to be checked.
    :param comparison: The comparison operator the columns should follow.
    :param colname_b: The name (a string!) of the column that the columns in
           ls_colnames_a need to be checked against to.
    :param axis: When 0, function returns faulty columns, when 1, faulty rows
    :return: ls_faulty, a list with the row indices or column names
             containing values that do not follow the expected date order.
    """
    # Turning the operators around, as the aim is to find values that break
    # the expected ordering.
    dict_operator = {'>': operator.le,
                     '>=': operator.lt,
                     '<': operator.ge,
                     '<=': operator.gt}
    compare_op = dict_operator[comparison]
    # When ls_colnames_a consists of only one column name, df[ls_colnames_a]
    # becomes a Series, making .apply() unwanted.
    if isinstance(ls_colnames_a, str):
        ls_colnames_a = [ls_colnames_a]
    if len(ls_colnames_a) == 1:
        if compare_op(df[ls_colnames_a], df[colname_b]).any():
            ls_faulty = ls_colnames_a
        else:
            ls_faulty = []
    else:
        ss_faulty = df[ls_colnames_a].apply(lambda x: compare_op(x,
                                        df[colname_b])).any(axis=axis)
        ls_faulty = ss_faulty[ss_faulty].index.tolist()

    return ls_faulty


def generate_list_cc0x_columns(df, dict_config, lvl1_desc, list_keys=(
        'first_exp_date_cols', 'last_exp_date_cols')):
    """

    :param df:
    :param dict_config:
    :param lvl1_desc:
    :param list_keys:
    :return:
    """
    # Take all the column names based on the keys for the dict_config
    ls_cols = generate_list_columns(df, dict_config, list_keys)
    # Take all the feature names that belong to the right lvl1_desc and
    # clean string
    ls_cc0x_feats = generate_list_cc0x_feats(dict_config, lvl1_desc=lvl1_desc)
    ls_cc0x_feats = [clean_string(cc0x_feat) for cc0x_feat in ls_cc0x_feats]

    # Take all column names that belong both to right key and have the right
    # PROD_CUSTOM_LVL1_DESC
    prog = re.compile("(" + ")|(".join(ls_cc0x_feats) + ")")
    ls_cc0x_cols = [colname for colname in ls_cols if prog.search(
        clean_string(colname))]
    return ls_cc0x_cols


def generate_list_cc0x_feats(dict_config, lvl1_desc):
    """

    :param dict_config:
    :param lvl1_desc:
    :return:
    """
    code_lvl1_col = 'PROD_CUSTOM_LVL1_DESC'
    code_lvl2_col = 'PROD_CUSTOM_LVL2_DESC'
    ls_files = ['ICD_file', 'NCD_file', 'CPT_file', 'HCPC_file',
                'speciality_file']

    # Load the seperate CSV files
    dict_dfs = {}
    for file in ls_files:
        dict_dfs[file] = pd.read_csv(dict_config['general'][file])

    # Create the dict of 3 sets, each containing the level 2 descriptions that
    # have the level 1 description of the dictionary key.
    set_lvl2_desc = set()
    for df in dict_dfs.values():
        set_lvl2_desc.update(df[df[code_lvl1_col] == lvl1_desc][code_lvl2_col])
    # Changes set into list and make sure that each description string in
    # those lists is in the right format
    ls_lvl2_desc = [clean_string(desc) for desc in set_lvl2_desc]

    return ls_lvl2_desc


def generate_list_columns(df, dict_config, list_keys):
    """
    Generates list of all column names needed, based on the list of keys for
    the dict_config.

    If none of the keys exists in the dict_config, or none of the
    suffixes/column names is found in the list of column names, an empty
    list is returned.

    :param df:
    :param dict_config:
    :param list_keys:
    :return: List of all column names that match the values of the key-value
    pairs from list_keys.
    """
    list_regex = ["%s$" % dict_config['general'][key] for key in list_keys
                  if key in dict_config['general']]
    if list_regex:
        prog = re.compile("(" + ")|(".join(list_regex) + ")")
        return [colname for colname in df.columns if prog.search(colname)]
    else:
        return []


def generate_dict_grouped_columns(df, dict_config, list_keys):
    """
    Groups the columns of the dataframe df based on the feature, i.e. the
    prefix of the column names. This is done by creating a dictionary of
    dictionaries.

    Each key in the upper level is a general feature, the value is a
    dictionary in which all column names of the wanted columns of that
    feature are grouped,
    e.g:
        {'A': {'count': 'A_count', 'first_exp_date': 'A_first_exp_dt'},
         'B': {'count': 'B_count', 'first_exp_date': 'B_first_exp_dt'}}
    Features that do not have all the wanted columns just end up with
    shorter dictionaries.

    :param df:
    :param dict_config:
    :param list_keys: The list of keys in the YAML config file that select
            the wanted column types, so that point to the right column name
            suffixes.
    :return: A dictionary of dictionaries
    """
    dict_regex = {re.sub(r'_cols$', '', key): "%s$" % dict_config['general'][
        key] for key in list_keys if key in dict_config['general']}

    dict_grouped_cols = defaultdict(dict)
    for key, value in dict_regex.items():
        for colname in df.columns:
            if re.search(value, colname):
                dict_grouped_cols[re.sub(value, '', colname)][key] = colname
    dict_grouped_cols = {clean_string(key): dict_cols for key, dict_cols in
                         dict_grouped_cols.items()}
    return dict_grouped_cols


def clean_string(s):
    """
    Function that cleans up string, typically used for column names. Changes
    the incoming string s so that:
        - only lowercase
        - white spaces replaced by underscores
        - brackets replaced by underscores
        - multiple underscores in a row replaced by one
          underscore (e.g. ' (' -> '__' -> '_'
    :param s: string
    :return: new string
    """
    # only lowercase
    s = s.lower()
    # whites spaces and brackets replaced by underscores
    s = re.sub(r'( )|(\()|(\))', '_', s)
    # multiple underscores in a row replaced by one underscore
    s = re.sub(r'(_)\1+', r'\1', s)
    return s


def is_zero_or_null(ss):
    """
    pd.datetime or object throws error when compared to 0. This function
    avoids this problem by comparing values of series to 0 if values are
    numeric, otherwise only calls isnull()

    :param ss: Pandas series (column of dataframe)
    :return: Series of boolean values
    """
    if pd.api.types.is_numeric_dtype(ss):
        return (ss == 0) | ss.isnull()
    else:
        return ss.isnull()


def fraction_zeroes_or_null(ss):
    """
    Calculates the fraction of values of a pandas series that gave True in
    function is_zero_or_null.

    :param ss: Pandas series (column of dataframe)
    :return: Float, fraction of values in column being zero or null.
    """
    return is_zero_or_null(ss).sum()/len(ss)


def mean_all_types(ss):
    """
    Uses the correct mean function, based on the input type.
    :param ss: pandas series
    :return: Mean value of the series
    """
    # This seemingly useless line is used to avoid a bug in pandas'
    # DataFrame.apply, DO NOT REMOVE.
    ss.dtype
    if pd.api.types.is_numeric_dtype(ss):
        return np.mean(ss)
    elif pd.api.types.is_datetime64_any_dtype(ss):
        return mean_datetime(ss)
    else:
        return np.nan


def median_all_types(ss):
    """
    Uses the correct median function, based on the input type.
    :param ss: pandas series
    :return: Median value of the series
    """
    # This seemingly useless line is used to avoid a bug in pandas'
    # DataFrame.apply, DO NOT REMOVE.
    ss.dtype
    if pd.api.types.is_numeric_dtype(ss):
        return np.median(ss)
    elif pd.api.types.is_datetime64_any_dtype(ss):
        return median_datetime(ss)
    else:
        return np.nan


def mean_datetime(ss_datetime):
    """

    :param ss_datetime:
    :return:
    """
    ss_datetime_int = ss_datetime[~ss_datetime.isnull()].astype('int64')
    dt_mean_int = int(np.mean(ss_datetime_int))
    return transform_int_to_dt(dt_mean_int)


def median_datetime(ss_datetime):
    """

    :param ss_datetime:
    :return:
    """
    ss_datetime_int = ss_datetime[~ss_datetime.isnull()].astype('int64')
    dt_median_int = int(np.median(ss_datetime_int))
    return transform_int_to_dt(dt_median_int)


def transform_int_to_dt(date_int):
    return pd.to_datetime(np.datetime64(date_int, 'ns'))


def get_qcs_desc():
    """
    Loads the qc list file from data, which has to be exported and downloaded
    from the online tracker manually every time it's updated.

    :return: Dictionary of qc_num: qc description pairs.
    """
    df = pd.read_excel("paqc/data/Status_QC.xlsx")
    qc_dict = dict()

    for i in df.index:
        row = df.loc[i]
        id = row.ID
        if not pd.isnull(id) and id not in qc_dict:
            key = 'qc%d' % id
            qc_dict[key] = row['Test description']
    return qc_dict


def get_qcs_compare():
    """
    Loads the qc list file from data, which has to be exported and downloaded
    from the online tracker manually every time it's updated. For each QC it
    checks if it's a comparison type, i.e. it requires two dataframes as input.

    :return: Dictionary of qc_num: boolean, whether the QC is a compare.
    """
    df = pd.read_excel("paqc/data/Status_QC.xlsx")
    qc_dict = dict()

    for i in df.index:
        row = df.loc[i]
        id = row.ID
        if not pd.isnull(id) and id not in qc_dict:
            key = 'qc%d' % id
            if row['Test data'] == "Compare":
                qc_dict[key] = True
            else:
                qc_dict[key] = False
    return qc_dict
