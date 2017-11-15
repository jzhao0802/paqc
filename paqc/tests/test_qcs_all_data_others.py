import pytest

from paqc.connectors import csv
from paqc.qc_functions.qcs_all_data_others import *
from paqc.utils.config_utils import config_open

DICT_CONFIG = config_open("paqc/tests/data/driver_dict_output.yml")[1]
DICT_CONFIG_16 = config_open("paqc/tests/data/qc16_driver_dict_output.yml")[1]


# 14
@pytest.mark.parametrize("dict_config", [DICT_CONFIG])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG, "paqc/tests/data/qc14_check1.csv")[1],
     True, None),
    # row 3 is missing
    (csv.read_csv(DICT_CONFIG, "paqc/tests/data/qc14_check2.csv")[1],
     False, [2])
])
def test_qc14(df, expected, ls_faults, dict_config):
    rpi = qc14(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 15
@pytest.mark.parametrize("dict_config", [DICT_CONFIG])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG, "paqc/tests/data/qc15_check1.csv")[1],
     True, None),
    # One column with word, other with a non-numeric character ;
    (csv.read_csv(DICT_CONFIG, "paqc/tests/data/qc15_check2.csv")[1],
     False, ['D_7373_AVG_CLAIM_FLAG', 'D_7803_AVG_CLAIM_CNT'])
])
def test_qc15(df, expected, ls_faults, dict_config):
    rpi = qc15(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 16
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_16])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG_16, "paqc/tests/data/qc16_check1.csv")[1],
     True, None),
    # Gender is 1 for label 0 samples and 0 for label 1 samples
    (csv.read_csv(DICT_CONFIG_16, "paqc/tests/data/qc16_check2.csv")[1],
     False, ['GENDER']),
    # Same, + D_7373_AVG_CLAIM_CNT is missing more often for label 0 samples
    (csv.read_csv(DICT_CONFIG_16, "paqc/tests/data/qc16_check3.csv")[1],
     False, ['GENDER', 'D_7373_AVG_CLAIM_CNT'])
])
def test_qc16(df, expected, ls_faults, dict_config):
    rpi = qc16(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)
