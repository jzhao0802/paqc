"""
Various general helper functions for the QC package.
"""
from itertools import islice
import pandas as pd


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

    string_list =  ", ".join(str(item) for item in islice(ls_items,
                                                          n_items_printed))
    string_list = string_list + ", ..."
    return string_list

def write_list_to_csv(ls_items, path_csv):
    """

    :param ls_items:
    :return: None
    """
    df_tocsv = pd.DataFrame(ls_items)
    df_tocsv.to_csv(path_csv, index=False, header=False)



