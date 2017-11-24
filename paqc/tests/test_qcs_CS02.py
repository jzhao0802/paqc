import pytest

from paqc.connectors import csv
from paqc.qc_functions.qcs_CS02 import *
from paqc.utils.config_utils import config_open

DICT_CONFIG_CS02 = config_open("paqc/tests/data/driver_dict_output_CS02.yml")[1]


# 25
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_CS02])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Original subset from data/random_sample_scoring.csv
    (csv.read_csv(DICT_CONFIG_CS02, "paqc/tests/data/qc25_check1.csv"),
     True, None),
    # Copied two patient IDs from the cp01 file into check2
    (csv.read_csv(DICT_CONFIG_CS02, "paqc/tests/data/qc25_check2.csv"),
     False, ['57616631', '81744431'])
])
def test_qc25(df, expected, ls_faults, dict_config):
    rpi = qc25(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 26
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_CS02])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Original subset from data/random_sample_scoring.csv
    (csv.read_csv(DICT_CONFIG_CS02, "paqc/tests/data/qc26_check1.csv"),
     True, None),
    # Row 1 has a date value for diseasefirstexp
    (csv.read_csv(DICT_CONFIG_CS02, "paqc/tests/data/qc26_check2.csv"),
     False, [1])
])
def test_qc26(df, expected, ls_faults, dict_config):
    rpi = qc26(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)