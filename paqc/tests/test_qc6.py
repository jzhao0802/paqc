import pytest
import pandas as pd
from paqc.qc_functions.qc6 import qc6
from paqc.utils.config_utils import config_open


@pytest.mark.parametrize("dict_config", [
    config_open("paqc/data/driver_dict_output.yml")[1]
])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (pd.read_csv("paqc/tests/data/qc6_check1.csv"), True, None),
    # GENDER is empty
    (pd.read_csv("paqc/tests/data/qc6_check2.csv"), False, ['GENDER']),
    # Sparse dataframe, GENDER only one field with 'F', but no empty columns
    (pd.read_csv("paqc/tests/data/qc6_check3.csv"), True, None),
])
def test_qc6(df, expected, ls_faults, dict_config):
    rpi = qc6(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)
