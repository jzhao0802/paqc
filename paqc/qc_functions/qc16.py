import pandas as pd
import re
from os.path import join
from paqc.report import report as rp
from paqc.utils import utils


def qc16(df, dict_config):
    """

    :param df:
    :param dict_config:
    :return:
    """

    

    return rp.ReportItem.init_conditional(ls_nomatch, dict_config['qc'])