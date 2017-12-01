import pandas as pd
import re

from paqc.report import report as rp
from paqc.utils import utils


def qc40(df, dict_config, set_metrictypes={'FLAG', 'FREQ', 'COUNT'}):
    """
    Predictor names should follow the naming convention in Table 1.10 2.

    Naming conventions for predictors:
    Category of predictor	Naming convention of predictor
    GPI-6	                G_[Code]_[MetricType]
    GPI-10	                G_[Code]_[MetricType]
    ICD-9 Level 4	        D_[Code]_ [MetricType]
    CPT	                    C_[Code]_ [MetricType]
    HCPCS	                H_[Code]_ [MetricType]
    Specialty	            S_[SpecialtyCode]_MetricType]

    :param df:
    :param dict_config:
    :param set_metrictypes:
    :return:
    """
    dict_prefix = {'CPT': 'C', 'GPI6': 'G', 'GPI-10': 'G', 'ICD9': 'D',
                   'HCPC': 'H', 'Specialty': 'S', 'ICD10': 'D'}

    def is_not_wellnamed(code, cat):
        is_not = (not (len(code) == 3) or
                  not (code[2] in set_metrictypes) or
                  not (dict_prefix[cat] == code[0]))
        return is_not

    ss_code_split = df[dict_config['code']].apply(lambda x: re.split('_', x))
    ss_bool = pd.concat([ss_code_split, df['category']], axis=1).apply(
                        lambda x: is_not_wellnamed(*x), axis=1)
    ls_codes_faulty = df.loc[ss_bool, 'code']

    return rp.ReportItem.init_conditional(ls_codes_faulty, dict_config['qc'])


def qc41(df, dict_config):
    """
    There should be no missing values in the entire table. 

    :param df:
    :param dict_config:
    :return:
    """
    ss_rows_with_nulls = df.isnull().any(axis=1)
    ls_idx_faulty = ss_rows_with_nulls[ss_rows_with_nulls].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_faulty, dict_config['qc'])


def qc42(df, dict_config):
    """
    Every row should have a unique description.

    :param df:
    :param dict_config:
    :return:
    """
    ss_boolean = df.duplicated(subset=dict_config['description'], keep=False)
    ls_idx_duplicate = ss_boolean[ss_boolean].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_duplicate, dict_config['qc'])