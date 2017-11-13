from paqc.report import report as rp


def qc52(df, dict_config):
    """
    Checks for missing patient IDs. Generates a list of the indices of the
    rows with a missing patient ID.

    :param df:
    :param dict_config:
    :return:
    """

    colname_patientid = dict_config['general']['patient_id_col']
    ls_missing_ids = df[df[colname_patientid].isnull()].index.tolist()

    return rp.ReportItem.init_conditional(ls_missing_ids, dict_config['qc'])
