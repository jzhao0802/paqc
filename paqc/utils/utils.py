"""
Various general helper functions for the QC package.
"""
from itertools import islice
import pandas as pd
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


def generate_short_string(ls_items, n_items_printed = 20):
    """
    Generate short string of a list of items (column names, indices etc),
    typically for the ReportItem.text field.
    :param ls_items: list of values that needs to be turned into printable string.
    :param n_items_printed: First n numbers of the list that are put into the
    string format.
    :return: Comma delimited string
    """

    string_list = ", ".join(str(item) for item in islice(ls_items,
                                                          n_items_printed))
    string_list += ", ..."
    return string_list


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
    pairs from list_keys
    """

    list_regex = ["%s$" % dict_config['general'][key] for key in list_keys]
    prog = re.compile("(" + ")|(".join(list_regex) + ")")
    return [colname for colname in df.columns if prog.search(colname)]
