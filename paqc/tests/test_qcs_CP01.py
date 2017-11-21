import pytest

from paqc.connectors import csv
from paqc.qc_functions.qcs_CP01 import *
from paqc.utils.config_utils import config_open

DICT_CONFIG_CP01 = config_open("paqc/tests/data/driver_dict_output_CP01.yml")[1]


# 20
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_CP01])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Original column names from data/initial_pos.csv
    (csv.read_csv(DICT_CONFIG_CP01, "paqc/tests/data/qc20_check1.csv")[1],
     True, None),
    # Row 0, 1 and 2 have respectively NaN or 0 for the flag column.
    # flag columns are the only columns used to assess if a row has any
    # predictor part of CP01
    (csv.read_csv(DICT_CONFIG_CP01, "paqc/tests/data/qc20_check2.csv")[1],
     False, [0, 1, 2])
])
def test_qc20(df, expected, ls_faults, dict_config):
    rpi = qc20(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 21
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_CP01])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Original column names from data/initial_pos.csv
    (csv.read_csv(DICT_CONFIG_CP01, "paqc/tests/data/qc21_check1.csv")[1],
     True, None),
    # index_dt has one missing value, one the same as diseasefirstexp (has
    # to be strictly before!) and one after
    (csv.read_csv(DICT_CONFIG_CP01, "paqc/tests/data/qc21_check2.csv")[1],
     False, [0, 1, 4])
])
def test_qc21(df, expected, ls_faults, dict_config):
    rpi = qc21(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 22
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_CP01])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Original column names from data/initial_pos.csv
    (csv.read_csv(DICT_CONFIG_CP01, "paqc/tests/data/qc22_check1.csv")[1],
     True, None),
    # Row 0 has a _first_exp_date before lookback_dt,
    # Row 6 has _first_exp_dt and _last_exp_dt on lookback and index
    # respectively, not a problem!
    (csv.read_csv(DICT_CONFIG_CP01, "paqc/tests/data/qc22_check2.csv")[1],
     False, [0])
])
def test_qc22(df, expected, ls_faults, dict_config):
    rpi = qc22(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)
