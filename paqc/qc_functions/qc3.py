import pandas as pd
import re


def qc3(df, level = "error"):
    """

    :param df:
    :param level:
    :return:
    """

    prog = re.compile(r'COUNT$|FLAG$|FREQ$')
