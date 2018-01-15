import pytest

from paqc.connectors import csv
from paqc.utils.config_utils import config_open

DICT_CONFIG = config_open("paqc/tests/data/driver_dict_output.yml")[1]
DICT_CONFIG_16 = config_open("paqc/tests/data/qc16_driver_dict_output.yml")[1]
DICT_CONFIG_17TO19 = config_open(
                        "paqc/tests/data/qc17to19_driver_dict_output.yml")[1]
DICT_CONFIG_20TO21 = config_open(
                        "paqc/tests/data/qc20to21_driver_dict_output.yml")[1]


# 14
@pytest.mark.parametrize("dict_config", [DICT_CONFIG])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG, "paqc/tests/data/qc14_check1.csv"),
     True, None),
    # row 3 is missing
    (csv.read_csv(DICT_CONFIG, "paqc/tests/data/qc14_check2.csv"),
     False, [2])
])
def test_qc14(df, expected, ls_faults, dict_config):
    rpi = qc14(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 15
@pytest.mark.parametrize("dict_config", [DICT_CONFIG])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG, "paqc/tests/data/qc15_check1.csv"),
     True, None),
    # One column with word, other with a non-numeric character ;
    (csv.read_csv(DICT_CONFIG, "paqc/tests/data/qc15_check2.csv"),
     False, ['D_7373_AVG_CLAIM_FLAG', 'D_7803_AVG_CLAIM_CNT'])
])
def test_qc15(df, expected, ls_faults, dict_config):
    rpi = qc15(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 16
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_16])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG_16, "paqc/tests/data/qc16_check1.csv"),
     True, None),
    # Gender is 1 for label 0 samples and 0 for label 1 samples
    (csv.read_csv(DICT_CONFIG_16, "paqc/tests/data/qc16_check2.csv"),
     False, ['GENDER']),
    # Same, + D_7373_AVG_CLAIM_CNT is missing more often for label 0 samples
    (csv.read_csv(DICT_CONFIG_16, "paqc/tests/data/qc16_check3.csv"),
     False, ['GENDER', 'D_7373_AVG_CLAIM_CNT'])
])
def test_qc16(df, expected, ls_faults, dict_config):
    rpi = qc16(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 17
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_17TO19])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/initial_pos.csv
    (csv.read_csv(DICT_CONFIG_17TO19, "paqc/tests/data/qc17_check1.csv"),
     True, None),
    # One row has U for gender, another an empty cell
    (csv.read_csv(DICT_CONFIG_17TO19, "paqc/tests/data/qc17_check2.csv"),
     False, [0, 1]),
])
def test_qc17(df, expected, ls_faults, dict_config):
    rpi = qc17(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 18
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_17TO19])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/initial_pos.csv
    (csv.read_csv(DICT_CONFIG_17TO19, "paqc/tests/data/qc18_check1.csv"),
     True, None),
    # One row has a missing age, another is 86
    (csv.read_csv(DICT_CONFIG_17TO19, "paqc/tests/data/qc18_check2.csv"),
     False, [1, 2]),
])
def test_qc18(df, expected, ls_faults, dict_config):
    rpi = qc18(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 19
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_17TO19])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/initial_pos.csv, given minimum index_dt is 01/02/2009
    # 05:00.
    (csv.read_csv(DICT_CONFIG_17TO19, "paqc/tests/data/qc19_check1.csv"),
     True, None),
    # One row has a missing index_dt, another is in 2008, before the given
    # minimum of 01/02/2009 05:00
    (csv.read_csv(DICT_CONFIG_17TO19, "paqc/tests/data/qc19_check2.csv"),
     False, [0, 7]),
])
def test_qc19(df, expected, ls_faults, dict_config):
    rpi = qc19(df, dict_config, dict_config['qc']['qc_params']['date_limit'])
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 20
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_20TO21])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from date_diff_vars.csv
    (csv.read_csv(DICT_CONFIG_20TO21, "paqc/tests/data/qc20_check1.csv"),
     True, None),
    # Recalculated A_FREQ by dividing count by lookback length in days
    (csv.read_csv(DICT_CONFIG_20TO21, "paqc/tests/data/qc20_check2.csv"),
     False, ['a']),
])
def test_qc20(df, expected, ls_faults, dict_config):
    qc_params = dict_config['qc']['qc_params']
    rpi = qc20(df, dict_config, qc_params['lookback_days_col'], qc_params[
        'days_months_years'])
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 21
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_20TO21])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from date_diff_vars.csv
    (csv.read_csv(DICT_CONFIG_20TO21, "paqc/tests/data/qc21_check1.csv"),
     True, None),
    #
    (csv.read_csv(DICT_CONFIG_20TO21, "paqc/tests/data/qc21_check2.csv"),
     False, ['A_FIRST_EXP_DD', 'A_LAST_EXP_DD']),
])
def test_qc21(df, expected, ls_faults, dict_config):
    qc_params = dict_config['qc']['qc_params']
    rpi = qc21(df, dict_config, qc_params['lookback_days_col'], qc_params[
        'ls_dd_columns'])
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)
