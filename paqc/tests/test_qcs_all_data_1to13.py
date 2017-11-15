import pytest

from paqc.connectors import csv
from paqc.qc_functions.qcs_all_data_1to13 import *
from paqc.utils.config_utils import config_open

DICT_CONFIG_1TO8 = config_open("paqc/tests/data/driver_dict_output.yml")[1]
DICT_CONFIG_9TO13 = config_open(
    "paqc/tests/data/qc9to13_driver_dict_output.yml")[1]


# 1
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_1TO8])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Original column names from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc1_check1.csv")[1],
     True, None),
    # GENDER has trailing space, D_7931_AVG_CLAIM_CNT leading space
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc1_check2.csv")[1],
     False, ['GENDER ', ' D_7931_AVG_CLAIM_CNT']),
    # Second column name is empty
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc1_check3.csv")[1],
     False, ['Unnamed: 1']),
    # Created column name with single $
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc1_check4.csv")[1],
     False, ['$']),
    # First column name is lab*el
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc1_check5.csv")[1],
     False, ['lab*el'])
])
def test_qc1(df, expected, ls_faults, dict_config):
    rpi = qc1(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 3
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_1TO8])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc3_check1.csv")[1],
     True, None),
    # column test_FLAG has negative float
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc3_check2.csv")[1],
     False, ["test_FLAG"]),
    # One empty cell, one cell with special character (;)
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc3_check3.csv")[1],
     False, ["D_7245_AVG_CLAIM_FREQ", "D_V048_AVG_CLAIM_CNT"]),
    # empty cell and special character (.). Also one non-relevant column
    # with negative integers, should not be picked up.
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc3_check4.csv")[1],
     False, ["test_FLAG", "D_7245_AVG_CLAIM_FREQ", "D_V048_AVG_CLAIM_CNT"])
])
def test_qc3(df, expected, ls_faults, dict_config):
    rpi = qc3(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 4
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_1TO8])
@pytest.mark.parametrize("df, expected, ls_duplicates", [
    # Subset from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc4_check1.csv")[1],
     True, None),
    # Made two patient_ids equal, in row indices 1 and 2
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc4_check2.csv")[1],
     False, [1, 2]),
    # Two empty patient_id cells, not considered as duplicate
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc4_check3.csv")[1],
     True, None),
    # Row with indices 3 and 4 have a dot (.) as patient_id, considered
    # valid input and thus flagged as duplicates
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc4_check4.csv")[1],
     False, [3, 4])
])
def test_qc4(df, expected, ls_duplicates, dict_config):
    rpi = qc4(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_duplicates)


# 6
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_1TO8])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc6_check1.csv")[1],
     True, None),
    # GENDER is empty
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc6_check2.csv")[1],
     False, ['GENDER']),
    # Sparse dataframe, GENDER only one field with 'F', but no empty columns
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc6_check3.csv")[1],
     True, None),
])
def test_qc6(df, expected, ls_faults, dict_config):
    rpi = qc6(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 7
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_1TO8])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc7_check1.csv")[1],
     True, None),
    # Row 4 is empty
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc7_check2.csv")[1],
     False, [3]),
    # Sparse dataframe, but no empty rows
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc7_check3.csv")[1],
     True, None),
])
def test_qc7(df, expected, ls_faults, dict_config):
    rpi = qc7(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 8
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_1TO8])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc8_check1.csv")[1],
     True, None),
    # Column name G has trailing s
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc8_check2.csv")[1],
     False, ['D_7245_AVG_CLAIM_FREQs']),
    # Added some special columns
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc8_check3.csv")[1],
     True, None),
])
def test_qc8(df, expected, ls_faults, dict_config):
    rpi = qc8(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 9
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_9TO13])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Modified subset from paqc/data/initial_pos.csv
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc9_check1.csv")[1],
     True, None),
    # Two columns have respectively 1 and 5 dates after the index date
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc9_check2.csv")[1],
     False, ['A_first_exp_dt', 'A_last_exp_dt']),
    # Some dates are on the index date, which is allowed.
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc9_check3.csv")[1],
     True, None)
])
def test_qc9(df, expected, ls_faults, dict_config):
    rpi = qc9(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 10
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_9TO13])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Modified subset from paqc/data/initial_pos.csv
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc10_check1.csv")[1],
     True, None),
    # Two columns have 1 date before the lookback date
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc10_check2.csv")[1],
     False, ['B_first_exp_dt', 'C_last_exp_dt'])
])
def test_qc10(df, expected, ls_faults, dict_config):
    rpi = qc10(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)
