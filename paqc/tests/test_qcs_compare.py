import pytest

from paqc.connectors import csv
from paqc.utils.config_utils import config_open

DICT_CONFIG_9TO13 = config_open(
    "paqc/tests/data/qc9to13_driver_dict_output.yml")[1]
DICT_CONFIG_48 = config_open(
    "paqc/tests/data/qc48_driver_dict_output.yml")[1]
DICT_CONFIG_50 = config_open(
    "paqc/tests/data/qc50_driver_dict_output.yml")[1]

# 46
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_9TO13])
@pytest.mark.parametrize("df_old", [csv.read_csv(DICT_CONFIG_9TO13,
                                    "paqc/tests/data/suite2_df_old.csv")])
@pytest.mark.parametrize("df_new, expected, ls_faults", [
    # identical to suite2_df_old.csv
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc46_check1.csv"),
     True, None),
    # A_last_exp_dt and A_first_exp_dt are changed in position
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc46_check2.csv"),
     False, None),
    # column C_count is gone in the new dataframe, column D_count is new
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc46_check3.csv"),
     False, {'missing columns': ['C_count'], 'new columns': ['D_count']})
])
def test_qc46(df_old, df_new, expected, ls_faults, dict_config):
    rpi = qc46(df_old, df_new, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 47
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_9TO13])
@pytest.mark.parametrize("df_old", [csv.read_csv(DICT_CONFIG_9TO13,
                                    "paqc/tests/data/suite2_df_old.csv")])
@pytest.mark.parametrize("df_new, expected, ls_faults", [
    # identical to suite2_df_old.csv
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc47_check1.csv"),
     True, None),
    # Two rows changed position
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc47_check2.csv"),
     False, None),
    # One row is gone
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc47_check3.csv"),
     False, {'missing IDs': [71062831], 'new IDs': []})
])
def test_qc47(df_old, df_new, expected, ls_faults, dict_config):
    rpi = qc47(df_old, df_new, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 48
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_48])
@pytest.mark.parametrize("df_old", [csv.read_csv(DICT_CONFIG_48,
                                    "paqc/tests/data/suite2_df_old.csv")])
@pytest.mark.parametrize("df_new, expected, ls_faults", [
    # identical to suite2_df_old.csv
    (csv.read_csv(DICT_CONFIG_48, "paqc/tests/data/qc48_check1.csv"),
     True, None),
    # A_first_exp_dt has extra value, corresping A_count changed to 1
    (csv.read_csv(DICT_CONFIG_48, "paqc/tests/data/qc48_check2.csv"),
     False, ['A_first_exp_dt', 'A_count']),
    # A value of A_count is deleted, some columns not part of list_columns
    # also changed, are not picked up.
    (csv.read_csv(DICT_CONFIG_48, "paqc/tests/data/qc48_check3.csv"),
     False, ['A_count'])
])
def test_qc48(df_old, df_new, expected, ls_faults, dict_config):
    rpi = qc48(df_old, df_new, dict_config, dict_config['qc']['qc_params'][
        'ls_colnames'])
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 50
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_50])
@pytest.mark.parametrize("df_old", [csv.read_csv(DICT_CONFIG_50,
                                    "paqc/tests/data/suite2_df_old.csv")])
@pytest.mark.parametrize("df_new, expected, ls_faults", [
    # identical to suite2_df_old.csv
    (csv.read_csv(DICT_CONFIG_50, "paqc/tests/data/qc50_check1.csv"),
     True, None),
    # Deleted rows for both feature A and B, only in the number is high enough
    # to make the difference in fraction of empty columns high enough
    (csv.read_csv(DICT_CONFIG_50, "paqc/tests/data/qc50_check2.csv"),
     False, ['B_flag', 'B_count', 'B_first_exp_dt',	'B_last_exp_dt',
             'B_hcp_spec']),
    # Added values in two rows for feature C in class 1, the change with
    # original file is bigger than the threshold of 0.3.
    (csv.read_csv(DICT_CONFIG_50, "paqc/tests/data/qc50_check3.csv"),
     False, ['C_flag', 'C_count', 'C_first_exp_dt', 'C_last_exp_dt',
             'C_hcp_spec'])
])
def test_qc50(df_old, df_new, expected, ls_faults, dict_config):
    rpi = qc50(df_old, df_new, dict_config, dict_config['qc']['qc_params'][
        'max_fraction_diff'])
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)
