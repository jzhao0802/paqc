import pytest
import pandas as pd

from paqc.qc_functions.qc52 import qc52
from paqc.utils.config_utils import config_open


@pytest.mark.parametrize("dict_config", [
    config_open("paqc/tests/data/driver_dict_output.yml")[1]
])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (pd.read_csv("paqc/tests/data/qc52_check1.csv"), True, None),
    # row 3 is missing
    (pd.read_csv("paqc/tests/data/qc52_check2.csv"), False, [2])
])
def test_qc52(df, expected, ls_faults, dict_config):
    rpi = qc52(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)
