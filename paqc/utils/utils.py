"""
Various general helper functions for the QC package.
"""
from collections import defaultdict
import pandas as pd
import numpy as np
import re


def generate_hash(df):
    """
    Given a pandas DataFrame, this function will generate a unique hash value
    of it. This enables us to precisely identify which version of a data file
    the test was carried out on.

    :param df: Pandas DataFrame object.
    :return: Hash integer that uniquely maps to a certain DataFrame.
    """

    return hash(df.values.tobytes())


def generate_cc0_lists(dict_config):
    """

    :param dict_config:
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
    dict_cc0_sets = {1: set(), 2: set(), 3: set()}
    for df in dict_dfs.values():
        for i in range(1, 4):
            dict_cc0_sets[i].update(df[df[code_lvl1_col] == i][code_lvl2_col])
    # Changes sets into lists and make sure that each description in those
    # lists is clean, i.e. only lowercase, white spaces replaced by underscores
    dict_cc0_lists = {key: [description.lower().replace(' ', '_') for
                            description in set_descriptions] for key,
                            set_descriptions in dict_cc0_sets.items()}
    return dict_cc0_lists


def write_list_to_csv(ls_items, path_csv):
    """
    Creates a csv file with the list items in a single column

    :param ls_items: input list
    :param path_csv: path of the csv file to be written to
    :return: None
    """

    df_tocsv = pd.DataFrame(ls_items)
    df_tocsv.to_csv(path_csv, index=False, header=False)


def generate_list_columns(df, dict_config, list_keys):
    """
    Generates list of all column names needed, based on the list of keys for
    the dict_config.

    :param df:
    :param dict_config:
    :param list_keys:
    :return: List of all column names that match the values of the key-value
    pairs from list_keys.
    """

    list_regex = ["%s$" % dict_config['general'][key] for key in list_keys]
    prog = re.compile("(" + ")|(".join(list_regex) + ")")
    return [colname for colname in df.columns if prog.search(colname)]


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
        key] for key in list_keys}

    dict_grouped_cols = defaultdict(dict)
    for key, value in dict_regex.items():
        for colname in df.columns:
            if re.search(value, colname):
                dict_grouped_cols[re.sub(value, '', colname)][key] = colname
    # Cleaning up the spelling of the features, i.e. no white spaces and
    # only lower case.
    dict_grouped_cols = {key.lower().replace(' ', '_'): dict_cols for key,
                         dict_cols in dict_grouped_cols.items()}
    return dict_grouped_cols


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
