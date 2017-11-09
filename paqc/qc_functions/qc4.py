import pandas as pd
from os.path import join
from paqc.report import report as rp
from paqc.utils import utils


def qc4(df, dict_config):
    """
    No duplicate patient IDs within the same cohort file.

    In the case of several emtpy patient IDs (NaN according to pandas
    definition), these will NOT be considered as duplicates. Different qc
    for looking for NaN patient_id.

    :param df: The qc-ed dataframe
    :param dict_config: Meta-data in dictionary
    :return: ReportItem
    """

    patient_id = dict_config['general']['patient_id']
    ls_idx_duplicateID = df[patient_id][df[patient_id].duplicated(
        keep=False) & ~df[patient_id].isnull()].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_duplicateID, dict_config[
        'qc'])