import pytest

from paqc.connectors import csv
from paqc.utils.config_utils import config_open

DICT_CONFIG_CP01 = config_open("paqc/tests/data/driver_dict_output_CP01.yml")[1]


# 22
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_CP01])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Original column names from data/initial_pos.csv
    (csv.read_csv(DICT_CONFIG_CP01, "paqc/tests/data/qc22_check1.csv"),
     True, None),
    # Row 0, 1 and 2 have respectively NaN or 0 for the flag column.
    # flag columns are the only columns used to assess if a row has any
    # predictor part of CP01
    (csv.read_csv(DICT_CONFIG_CP01, "paqc/tests/data/qc22_check2.csv"),
     False, [0, 1, 2])
])
def test_qc22(df, expected, ls_faults, dict_config):
    rpi = qc22(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 23
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_CP01])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Original column names from data/initial_pos.csv
    (csv.read_csv(DICT_CONFIG_CP01, "paqc/tests/data/qc23_check1.csv"),
     True, None),
    # index_dt has one missing value, one the same as diseasefirstexp (has
    # to be strictly before!) and one after
    (csv.read_csv(DICT_CONFIG_CP01, "paqc/tests/data/qc23_check2.csv"),
     False, [0, 1, 4])
])
def test_qc23(df, expected, ls_faults, dict_config):
    rpi = qc23(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 24
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_CP01])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Original column names from data/initial_pos.csv
    (csv.read_csv(DICT_CONFIG_CP01, "paqc/tests/data/qc24_check1.csv"),
     True, None),
    # Row 0 has a _first_exp_date before lookback_dt,
    # Row 6 has _first_exp_dt and _last_exp_dt on lookback and index
    # respectively, not a problem!
    (csv.read_csv(DICT_CONFIG_CP01, "paqc/tests/data/qc24_check2.csv"),
     False, [0])
])
def test_qc24(df, expected, ls_faults, dict_config):
    rpi = qc24(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)
