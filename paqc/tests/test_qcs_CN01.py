import pytest

from paqc.connectors import csv
from paqc.qc_functions.qcs_CN01 import *
from paqc.utils.config_utils import config_open

DICT_CONFIG_CN01 = config_open("paqc/tests/data/driver_dict_output_CN01.yml")[1]


# 27
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_CN01])
@pytest.mark.parametrize("df, expected", [
    # Original subset from data/initial_negative.csv
    (csv.read_csv(DICT_CONFIG_CN01, "paqc/tests/data/qc27_check1.csv"),
     True),
    # Deleted one row
    (csv.read_csv(DICT_CONFIG_CN01, "paqc/tests/data/qc27_check2.csv"),
     False),
])
def test_qc27(df, expected, dict_config):
    qc_params = dict_config['qc']['qc_params']
    rpi = qc27(df, dict_config, qc_params['path_file_cp02'], qc_params[
        'pat_id_col_cp02'], qc_params['n01_match'])
    assert (rpi.passed == expected)


# 28
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_CN01])
@pytest.mark.parametrize("df, expected", [
    # Original subset from data/initial_negative.csv
    (csv.read_csv(DICT_CONFIG_CN01, "paqc/tests/data/qc28_check1.csv"),
     True),
    # Added 50 days to each lookback_dys_cn value
    (csv.read_csv(DICT_CONFIG_CN01, "paqc/tests/data/qc28_check2.csv"),
     False),
])
def test_qc28(df, expected, dict_config):
    qc_params = dict_config['qc']['qc_params']
    rpi = qc28(df, dict_config, qc_params['path_file_cp02'], qc_params[
        'lookback_col_cn01'], qc_params['lookback_col_cp02'])
    assert (rpi.passed == expected)


# 29
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_CN01])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Original subset from data/random_sample_scoring.csv
    (csv.read_csv(DICT_CONFIG_CN01, "paqc/tests/data/qc29_check1.csv"),
     True, None),
    # Added 200 days to lookback_dys_cn row 1, and deleted the value for row 2
    (csv.read_csv(DICT_CONFIG_CN01, "paqc/tests/data/qc29_check2.csv"),
     False, [1, 2]),
    # Row 7 has a matched_patient_id that does not exist in the cp02 file
    (csv.read_csv(DICT_CONFIG_CN01, "paqc/tests/data/qc29_check3.csv"),
     False, [7])
])
def test_qc29(df, expected, ls_faults, dict_config):
    qc_params = dict_config['qc']['qc_params']
    rpi = qc29(df, dict_config, qc_params['path_file_cp02'],
               qc_params['pat_id_col_cp02'], qc_params['lookback_col_cn01'],
               qc_params['lookback_col_cp02'])
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)

# 30
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_CN01])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Original subset from data/random_sample_scoring.csv
    (csv.read_csv(DICT_CONFIG_CN01, "paqc/tests/data/qc30_check1.csv"),
     True, None),
    # Deleted the values for the stratification criteria row 2 met
    (csv.read_csv(DICT_CONFIG_CN01, "paqc/tests/data/qc30_check2.csv"),
     False, [2]),
])
def test_qc30(df, expected, ls_faults, dict_config):
    rpi = qc30(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)
