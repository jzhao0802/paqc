from paqc.connectors import parse_utils


def parse_dataframe(config, df):
    """
    Accepts an already loaded pandas DataFrame. This is useful if you want to
    paqc in an interactive session, or for development. For this, you need to
    pass the pandas DataFrame to the constructor of the Driver class, set the
    source param in the general section of the config to dataframe. The input
    param of the config file can be anything as it will be ignored.

    :param config: Parsed YAML config file.
    :param df: Input pandas DataFrame.
    :return: Tuple: first is a Boolean whether loading and parsing was
             successful, second is the pandas DataFrame if Bool=True.
    """

    df = parse_utils.check_dates(config, df)
    return df
