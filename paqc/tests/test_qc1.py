import pytest
import pandas as pd
from paqc.qc_functions.qc1 import qc1
from paqc.utils.config_utils import config_open


@pytest.mark.parametrize("dict_config", [
    config_open("paqc/tests/data/driver_dict_output.yml")[1]
])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Original column names from data/qc_data.csv
    (pd.read_csv("paqc/tests/data/qc1_check1.csv"), True, None),
    # GENDER has trailing space, D_7931_AVG_CLAIM_CNT leading space
    (pd.read_csv("paqc/tests/data/qc1_check2.csv"), False, ['GENDER ',
                                                          ' D_7931_AVG_CLAIM_CNT']),
    # Second column name is empty
    (pd.read_csv("paqc/tests/data/qc1_check3.csv"), False, ['Unnamed: 1']),
    # Created column name with single $
    (pd.read_csv("paqc/tests/data/qc1_check4.csv"), False, ['$']),
    # First column name is lab*el
    (pd.read_csv("paqc/tests/data/qc1_check5.csv"), False, ['lab*el'])
])
def test_qc1(df, expected, ls_faults, dict_config):
    rpi = qc1(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)
