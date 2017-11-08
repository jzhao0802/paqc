import pandas as pd
import re

from paqc.report import report as rp
from paqc.utils import utils

def qc1(df, dict_config):
    """
    Testing if all column names of dataframe have no special characters and no
    spaces. They should only contain letters, numbers and underscores.

    :param df: The qc-ed dataframe
    :param level:
    :return: True/False
    """

    # Pattern matches any non \w character or fully empty string
    prog = re.compile(r'[^\w]+|^[\w]{0}$')
    it_match = (prog.search(colname) for colname in df.columns)
    ls_match = [el.string for el in it_match if el is not None]
    if not ls_match:
        return rp.ReportItem(passed=True, **dict_config['qc'])
    else:
        return rp.ReportItem(passed=False, **dict_config['qc'],
                             text=utils.generate_short_string(ls_match))


