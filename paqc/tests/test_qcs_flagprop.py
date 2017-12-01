import pytest

from paqc.connectors import csv
from paqc.qc_functions.qcs_flagprop import *
from paqc.utils.config_utils import config_open

DICT_CONFIG_FLAGPROP = config_open(
    "paqc/tests/data/driver_dict_output_flagprop.yml")[1]


# 40
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_FLAGPROP])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Altered subset from v02.csv
    (csv.read_csv(DICT_CONFIG_FLAGPROP, "paqc/tests/data/qc40_check1.csv"),
     True, None),
    #
    (csv.read_csv(DICT_CONFIG_FLAGPROP, "paqc/tests/data/qc40_check2.csv"),
     False, ['C_11402COUNT', 'J_27370_DATE_FIRST_INDEX']),
])
def test_qc40(df, expected, ls_faults, dict_config):
    qc_params = dict_config['qc']['qc_params']
    rpi = qc40(df, dict_config, qc_params['ls_metrictypes'])
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 41
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_FLAGPROP])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Altered subset from v02.csv
    (csv.read_csv(DICT_CONFIG_FLAGPROP, "paqc/tests/data/qc41_check1.csv"),
     True, None),
    #
    (csv.read_csv(DICT_CONFIG_FLAGPROP, "paqc/tests/data/qc41_check2.csv"),
     False, [2, 3]),
])
def test_qc41(df, expected, ls_faults, dict_config):
    rpi = qc41(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 42
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_FLAGPROP])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Altered subset from v02.csv
    (csv.read_csv(DICT_CONFIG_FLAGPROP, "paqc/tests/data/qc42_check1.csv"),
     True, None),
    #
    (csv.read_csv(DICT_CONFIG_FLAGPROP, "paqc/tests/data/qc42_check2.csv"),
     False, [1, 2, 3]),
])
def test_qc42(df, expected, ls_faults, dict_config):
    rpi = qc42(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)
