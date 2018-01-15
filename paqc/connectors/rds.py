import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from paqc.connectors import parse_utils

# activate pandas - R bridge
pandas2ri.activate()
readRDS = robjects.r['readRDS']
robjects.r('memory.limit(64000)')


def read_rds(config, input_file_path):
    """
    Reads in .rds files directly into pandas. You need to have rpy2 installed
    in python.

    :param config: Parsed YAML config file.
    :param input_file_path: Absolute path to the csv file.
    :return: Tuple: first is a Boolean whether loading and parsing was
             successful, second is the pandas DataFrame if Bool=True.
    """

    # TODO: check why is this so slow on large files.
    df = pandas2ri.ri2py(readRDS(input_file_path))
    return parse_utils.check_dates(config, df)
