import pandas as pd
import re

def qc1(df, level = "error"):
    """
    Testing if all column names of dataframe have no special characters and no
    spaces. They should only contain letters, numbers and underscores.

    :param df: The qc-ed dataframe
    :param level:
    :return: True/Fasle
    """

    prog = re.compile(r'[\w]+$')
    if all(prog.match(colname) for colname in df.columns):
        output  = True
    else:
        output = False

    return output
