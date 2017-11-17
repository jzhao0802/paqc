import pytest

from paqc.connectors import csv
from paqc.qc_functions.qcs_compare import *
from paqc.utils.config_utils import config_open

DICT_CONFIG_9TO13 = config_open(
    "paqc/tests/data/qc9to13_driver_dict_output.yml")[1]
DICT_CONFIG_48 = config_open(
    "paqc/tests/data/qc48_driver_dict_output.yml")[1]

# 46
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_9TO13])
@pytest.mark.parametrize("df_old", [csv.read_csv(DICT_CONFIG_9TO13,
                                    "paqc/tests/data/suite2_df_old.csv")[1]])
@pytest.mark.parametrize("df_new, expected, ls_faults", [
    # identical to suite2_df_old.csv
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc46_check1.csv")[1],
     True, None),
    # A_last_exp_dt and A_first_exp_dt are changed in position
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc46_check2.csv")[1],
     False, None),
    # column C_count is gone in the new dataframe, column D_count is new
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc46_check3.csv")[1],
     False, {'missing columns': ['C_count'], 'new columns': ['D_count']})
])
def test_qc46(df_old, df_new, expected, ls_faults, dict_config):
    rpi = qc46(df_old, df_new, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 47
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_9TO13])
@pytest.mark.parametrize("df_old", [csv.read_csv(DICT_CONFIG_9TO13,
                                    "paqc/tests/data/suite2_df_old.csv")[1]])
@pytest.mark.parametrize("df_new, expected, ls_faults", [
    # identical to suite2_df_old.csv
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc47_check1.csv")[1],
     True, None),
    # Two rows changed position
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc47_check2.csv")[1],
     False, None),
    # One row is gone
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc47_check3.csv")[1],
     False, {'missing IDs': [71062831], 'new IDs': []})
])
def test_qc47(df_old, df_new, expected, ls_faults, dict_config):
    rpi = qc47(df_old, df_new, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 48
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_48])
@pytest.mark.parametrize("df_old", [csv.read_csv(DICT_CONFIG_48,
                                    "paqc/tests/data/suite2_df_old.csv")[1]])
@pytest.mark.parametrize("df_new, expected, ls_faults", [
    # identical to suite2_df_old.csv
    (csv.read_csv(DICT_CONFIG_48, "paqc/tests/data/qc48_check1.csv")[1],
     True, None),
    # A_first_exp_dt has extra value, corresping A_count changed to 1
    (csv.read_csv(DICT_CONFIG_48, "paqc/tests/data/qc48_check2.csv")[1],
     False, ['A_first_exp_dt', 'A_count']),
    # A value of A_count is deleted, some columns not part of list_columns
    # also changed, are not picked up.
    (csv.read_csv(DICT_CONFIG_48, "paqc/tests/data/qc48_check3.csv")[1],
     False, ['A_count'])
])
def test_qc48(df_old, df_new, expected, ls_faults, dict_config):
    rpi = qc48(df_old, df_new, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)

#TODO: Write test for qc50