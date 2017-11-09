import pytest
import pandas as pd

from paqc.qc_functions.qc3 import qc3
from paqc.utils.config_utils import config_open

@pytest.mark.parametrize("dict_config", [
    config_open("paqc/data/driver_dict_output.yml")[1]
])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (pd.read_csv("paqc/tests/data/qc3_check1.csv"), True, None),
    # column test_FLAG has negative float
    (pd.read_csv("paqc/tests/data/qc3_check2.csv"), False, ["test_FLAG"]),
    # Added some special columns
    (pd.read_csv("paqc/tests/data/qc8_check3.csv"), False,
     ["D_7245_AVG_CLAIM_FREQ", "D_V048_AVG_CLAIM_CNT"])
])
def test_qc3(df, expected, ls_faults, dict_config):
    rpi = qc3(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)
