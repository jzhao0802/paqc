import pandas as pd
from paqc.connectors import parse_utils


def read_feather(config, input_file_path):
    """
    Reads in .feather files directly into pandas. You need to have
    feather-format installed in python.

    :param config: Parsed YAML config file.
    :param input_file_path: Absolute path to the csv file.
    :return: Tuple: first is a Boolean whether loading and parsing was
             successful, second is the pandas DataFrame if Bool=True.
    """

    df = pd.read_feather(input_file_path)
    return parse_utils.check_dates(config, df)
