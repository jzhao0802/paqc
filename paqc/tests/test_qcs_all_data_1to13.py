import pytest

from paqc.connectors import csv
from paqc.utils.config_utils import config_open

DICT_CONFIG_1TO8 = config_open("paqc/tests/data/driver_dict_output.yml")[1]
DICT_CONFIG_9TO13 = config_open(
    "paqc/tests/data/qc9to13_driver_dict_output.yml")[1]


# 1
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_1TO8])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Original column names from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc1_check1.csv"),
     True, None),
    # GENDER has trailing space, D_7931_AVG_CLAIM_CNT leading space
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc1_check2.csv"),
     False, ['GENDER ', ' D_7931_AVG_CLAIM_CNT']),
    # Second column name is empty
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc1_check3.csv"),
     False, ['Unnamed: 1']),
    # Created column name with single $
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc1_check4.csv"),
     False, ['$']),
    # First column name is lab*el
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc1_check5.csv"),
     False, ['lab*el'])
])
def test_qc1(df, expected, ls_faults, dict_config):
    rpi = qc1(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 3
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_1TO8])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc3_check1.csv"),
     True, None),
    # column test_FLAG has negative float
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc3_check2.csv"),
     False, ["test_FLAG"]),
    # One empty cell, one cell with special character (;)
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc3_check3.csv"),
     False, ["D_7245_AVG_CLAIM_FREQ", "D_V048_AVG_CLAIM_CNT"]),
    # empty cell and special character (.). Also one non-relevant column
    # with negative integers, should not be picked up.
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc3_check4.csv"),
     False, ["test_FLAG", "D_7245_AVG_CLAIM_FREQ", "D_V048_AVG_CLAIM_CNT"])
])
def test_qc3(df, expected, ls_faults, dict_config):
    rpi = qc3(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 4
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_1TO8])
@pytest.mark.parametrize("df, expected, ls_duplicates", [
    # Subset from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc4_check1.csv"),
     True, None),
    # Made two patient_ids equal, in row indices 1 and 2
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc4_check2.csv"),
     False, [1, 2]),
    # Two empty patient_id cells, not considered as duplicate
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc4_check3.csv"),
     True, None),
    # Row with indices 3 and 4 have a dot (.) as patient_id, considered
    # valid input and thus flagged as duplicates
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc4_check4.csv"),
     False, [3, 4])
])
def test_qc4(df, expected, ls_duplicates, dict_config):
    rpi = qc4(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_duplicates)


# 6
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_1TO8])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc6_check1.csv"),
     True, None),
    # GENDER is empty
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc6_check2.csv"),
     False, ['GENDER']),
    # Sparse dataframe, GENDER only one field with 'F', but no empty columns
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc6_check3.csv"),
     True, None),
])
def test_qc6(df, expected, ls_faults, dict_config):
    rpi = qc6(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 7
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_1TO8])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc7_check1.csv"),
     True, None),
    # Row 4 is empty
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc7_check2.csv"),
     False, [3]),
    # Sparse dataframe, but no empty rows
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc7_check3.csv"),
     True, None),
])
def test_qc7(df, expected, ls_faults, dict_config):
    rpi = qc7(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 8
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_1TO8])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc8_check1.csv"),
     True, None),
    # Column name G has trailing s
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc8_check2.csv"),
     False, ['D_7245_AVG_CLAIM_FREQs']),
    # Added some special columns
    (csv.read_csv(DICT_CONFIG_1TO8, "paqc/tests/data/qc8_check3.csv"),
     True, None),
])
def test_qc8(df, expected, ls_faults, dict_config):
    rpi = qc8(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 9
dict_config_91 = config_open("paqc/tests/data/qc9_check1.yml")[1]
dict_config_92 = config_open("paqc/tests/data/qc9_check2.yml")[1]
dict_config_93 = config_open("paqc/tests/data/qc9_check3.yml")[1]
dict_config_94 = config_open("paqc/tests/data/qc9_check4.yml")[1]
dict_config_95 = config_open("paqc/tests/data/qc9_check5.yml")[1]
dict_config_96 = config_open("paqc/tests/data/qc9_check6.yml")[1]


@pytest.mark.parametrize("df, dict_config, expected, ls_faults", [
    # Testing if all date columns are after the lookback_date_col
    (csv.read_csv(dict_config_91, "paqc/tests/data/qc9_check1.csv"),
     dict_config_91, False, ['frst_rx_clm_dt', 'frst_dx_clm_dt']),
    # Deleted frst_rx_clm_dt and frst_dx_clm_dt
    (csv.read_csv(dict_config_92, "paqc/tests/data/qc9_check2.csv"),
     dict_config_92, True, None),
    # frst_rx_clm_dt and frst_dx_clm_dt are <= lookback_dt, index_dt and
    # diseasefirstexp_dt aren't
    (csv.read_csv(dict_config_93, "paqc/tests/data/qc9_check3.csv"),
     dict_config_93, False, ['index_dt', 'diseasefirstexp_dt']),
    # None of them are strictly < than lookback_dt
    (csv.read_csv(dict_config_94, "paqc/tests/data/qc9_check4.csv"),
     dict_config_94, False, ['frst_rx_clm_dt', 'frst_dx_clm_dt', 'index_dt',
                             'diseasefirstexp_dt']),
    # Only looking at index_dt and diseasefirstexp_dt, all are >= compared
    # to lookback
    (csv.read_csv(dict_config_95, "paqc/tests/data/qc9_check5.csv"),
     dict_config_95, True, None),
    # But with slightly altered data, row 0 and 1 are not strictly >
    # compared to lookback date
    (csv.read_csv(dict_config_96, "paqc/tests/data/qc9_check6.csv"),
     dict_config_96, False, [0, 1]),
])
def test_qc9(df, expected, ls_faults, dict_config):
    rpi = qc9(df, dict_config, **dict_config['qc']['qc_params'])
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 10
dict_config_101 = config_open("paqc/tests/data/qc10_check1.yml")[1]
dict_config_102 = config_open("paqc/tests/data/qc10_check2.yml")[1]
dict_config_103 = config_open("paqc/tests/data/qc10_check3.yml")[1]
dict_config_104 = config_open("paqc/tests/data/qc10_check4.yml")[1]
dict_config_105 = config_open("paqc/tests/data/qc10_check5.yml")[1]


@pytest.mark.parametrize("df, dict_config, expected, ls_faults", [
    # lvl1_desc = 1
    (csv.read_csv(dict_config_101, "paqc/tests/data/qc10_check1.csv"),
     dict_config_101, True, None),
    # lvl1_desc = [1,2,3]
    (csv.read_csv(dict_config_102, "paqc/tests/data/qc10_check2.csv"),
     dict_config_102, False, ['albuterolcc02_cp_first_exp_dt',
                              'albuterolcc02_cp_last_exp_dt',
                              'antihistaminescc03_cp_first_exp_dt',
                              'antihistaminescc03_cp_last_exp_dt']),
    # Deleted some index dates, none of these rows can violate the comparison
    (csv.read_csv(dict_config_103, "paqc/tests/data/qc10_check3.csv"),
     dict_config_103, False, ['albuterolcc02_cp_first_exp_dt',
                              'albuterolcc02_cp_last_exp_dt']),
    # Testing lvl1_desc = [2, 3] on being smaller or equal than index_dt
    (csv.read_csv(dict_config_104, "paqc/tests/data/qc10_check4.csv"),
     dict_config_104, True, None),
    # Testing lvl1_desc = [2, 3] on being smaller or equal than index_dt,
    # axis=1 this time, so it returns indices instead of column names
    (csv.read_csv(dict_config_105, "paqc/tests/data/qc10_check5.csv"),
     dict_config_105, False, [0, 1]),
])
def test_qc10(df, expected, ls_faults, dict_config):
    rpi = qc10(df, dict_config, **dict_config['qc']['qc_params'])
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 11
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_9TO13])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Modified subset from paqc/data/initial_pos.csv
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc11_check1.csv"),
     True, None),
    # predictorA has first_exp_date after last_exp_date, predictorB has
    # first_exp_date == last_exp_date, but with count > 1
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc11_check2.csv"),
     False, ['predictora', 'predictorb']),
    # predictorA has a first_exp_date missing for row with count 5, THIS IS
    # NOT PICKED UP BY THIS QC, QC12 responsible for this.
    # predictorB has row with count == 1, but first_exp_date before
    # last_exp_date.
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc11_check3.csv"),
     False, ['predictorb'])
])
def test_qc11(df, expected, ls_faults, dict_config):
    rpi = qc11(df, dict_config, dict_config['qc']['qc_params'][
        'multiple_a_day'])
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 12
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_9TO13])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Modified subset from paqc/data/initial_pos.csv
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc12_check1.csv"),
     True, None),
    # predictorA has first_exp_date and last_exp_date, but no _count or _flag,
    # predictorB lacks first_exp_date on rows with counts and flags
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc12_check2.csv"),
     False, ['predictora', 'predictorb']),
    # predictorB lacks first_exp_date and last_exp_date for row with counts
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc12_check3.csv"),
     False, ['predictorb'])
])
def test_qc12(df, expected, ls_faults, dict_config):
    rpi = qc12(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)


# 13
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_9TO13])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Modified subset from paqc/data/initial_pos.csv
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc13_check1.csv"),
     True, None),
    # predictorA and B have row with first_exp != last_exp, but count 1
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc13_check2.csv"),
     False, ['predictora', 'predictorb']),
    # predictorA has row that misses _first_exp_date, not flagged here!
    # predictorB has row with first_exp != last_exp, but count 1
    (csv.read_csv(DICT_CONFIG_9TO13, "paqc/tests/data/qc13_check3.csv"),
     False, ['predictorb'])
])
def test_qc13(df, expected, ls_faults, dict_config):
    rpi = qc13(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)
