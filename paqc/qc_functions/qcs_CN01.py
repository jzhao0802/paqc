import pandas as pd
import re

from paqc.report import report as rp
from paqc.utils import utils


# Todo: Rewrite so the test checks that each CP02 patient has the right
# amount of CN01 patients
def qc27(df, dict_config, path_file_cp02='data/cp02.csv',
         pat_id_col_cp02='patient_id', n01_match=100):
    """
    Number of patients in CN01 = N01_MATCH * Number of patients in CP02.

    :param df:
    :param dict_config:
    :param path_file_cp02: Absolute path to the CP02 file.
    :param pat_id_col_cp02: Column name of patient_id column in the CP02 file.
    :param n01_match: The number of matched patients in CN01 for each
           patient in CP02.
    :return: ReportItem:
                -self.extra=str_extra: A description clarifying printing the
                number of patients in CN01 and the number of patients it
                should be according to the N01_MATCH number.
    """
    n_cp02 = pd.read_csv(path_file_cp02, usecols=[pat_id_col_cp02]).shape[0]
    n_cn01 = df.shape[0]

    if n_cp02*n01_match == n_cn01:
        return rp.ReportItem(passed=True, **dict_config['qc'])
    else:
        str_extra = str("Number of patients in CN01: %d. \n" \
                        "Number of patients in CP02 * N01_MATCH: %d." \
                        % (n_cn01, n_cp02*n01_match))
        return rp.ReportItem(passed=False, extra=str_extra, **dict_config['qc'])


# Todo: Make the one month a default parameter
def qc28(df, dict_config, path_file_cp02='data/cp02.csv',
         lookback_col_cn01='lookback_dys', lookback_col_cp02='lookback_dys'):
    """
    Mean lookback length is the same (within +/- one month) between positive
    and matched negative patients.

    :param df:
    :param dict_config:
    :param path_file_cp02: Absolute path to the CP02 file.
    :param lookback_col_cn01: Column name of the lookback length in days column
    :param lookback_col_cp02: Column name of the lookback length in days
           column for the CP02 file.
    :return: ReportItem:
                -self.extra=str_extra: A description of the difference in
                the mean lookback period in days between CN01 and CP02.
    """
    ss_lookback_cp02 = pd.read_csv(path_file_cp02, usecols=[lookback_col_cp02])
    ss_lookback_cn01 = df[lookback_col_cn01]
    diff_mean = abs(ss_lookback_cn01.mean() - ss_lookback_cp02.mean())
    if diff_mean.values[0] < 31:
        return rp.ReportItem(passed=True, **dict_config['qc'])
    else:
        str_extra = str('Difference in avg lookback is %d days.' % diff_mean)
        return rp.ReportItem(passed=False, extra=str_extra, **dict_config['qc'])

# todo: Make the 90 days a default parameter
def qc29(df, dict_config, path_file_cp02='data/cp02.csv',
         pat_id_col_cp02='patient_id', lookback_col_cn01='lookback_dys',
         lookback_col_cp02='lookback_dys'):
    """
    Negative patients matched to positive patients must not have a lookback
    length more than 90 days different.

    :param df:
    :param dict_config:
    :param path_file_cp02: Absolute path to the CP02 file.
    :param pat_id_col_cp02: Column name of patient_id column in the CP02 file.
    :param lookback_col_cn01: Column name of the lookback length in days column
    :param lookback_col_cp02: Column name of the lookback length in days column
           for the CP02 file.
    :return: ReportItem:
                -self.extra=ls_idx_faulty: A list of indices of rows in the
                dataframe (CN01 files) where the negative patients have a
                lookback length more than 90 days different from their
                matched CP02 patient.
    """

    # Warns in the report if user forgot to provide needed parameters
    matched_pat_id_col = dict_config['general']['matched_patient_id_col']

    # Load the needed columns of the CP02 file
    df_cp02 = pd.read_csv(path_file_cp02,
                          dtype={pat_id_col_cp02: df[matched_pat_id_col].dtype,
                                 lookback_col_cp02: df[lookback_col_cn01].dtype},
                          usecols=[pat_id_col_cp02, lookback_col_cp02])
    # Pandas .merge gives suffixes _x and _y when the colnames are identical
    if lookback_col_cn01 == lookback_col_cp02:
        lookback_col_x = lookback_col_cn01 + "_x"
        lookback_col_y = lookback_col_cp02 + "_y"
    else:
        lookback_col_x = lookback_col_cn01
        lookback_col_y = lookback_col_cp02
    # Merge the two dataframes on the matched_pat_id_col
    df_merged = pd.merge(df, df_cp02, how='left',
                         left_on=matched_pat_id_col,
                         right_on=pat_id_col_cp02)[[lookback_col_x,
                                                    lookback_col_y]]
    # Create list of indices of the CN01 dataframe of rows where the
    # lookback time of the patient is more than 90 days different from the
    # matched patient in CP02 dataframe or where the lookback_dys value is
    # missing in one of the two columns
    ss_bool = df_merged.isnull().any(axis=1) |\
              (abs(df_merged[lookback_col_x] - df_merged[lookback_col_y]) > 90)
    ls_idx_faulty = ss_bool[ss_bool].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_faulty, dict_config['qc'])


def qc30(df, dict_config):
    """
    Every patient in CN01 should meet the stratification criteria, CC01_CS.

    This is done by checking the _flag values for each feature part of
    CC01_CS. 

    :param df:
    :param dict_config:
    :return: ReportItem:
                -self.extra=ls_idx_faulty, a list of indices of rows of
                patients that do not meet any of the stratification criteria
    """
    ls_cc01_cs = utils.generate_list_cc0x_feats(dict_config, lvl1_desc=3)
    prog = re.compile("(" + ")|(".join(ls_cc01_cs) + ")")
    dict_features = utils.generate_dict_grouped_columns(df, dict_config,
                                                        ['flag_cols'])
    ls_cc01_cs_flag_cols = [dict_feat['flag'] for key, dict_feat in
                            dict_features.items() if prog.search(key)]
    ls_idx_faulty = df[~df[ls_cc01_cs_flag_cols].any(axis=1)].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_faulty, dict_config['qc'])
