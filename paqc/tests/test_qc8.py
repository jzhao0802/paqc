import pytest
import pandas as pd
from paqc.qc_functions.qc8 import qc8
from paqc.utils.config_utils import config_open

@pytest.mark.parametrize("dict_config", [
    config_open("paqc/data/driver_dict_output.yml")[1]
])
@pytest.mark.parametrize("df, expected", [
    # Subset from data/qc_data.csv
    (pd.read_csv("paqc/tests/data/qc8_check1.csv"), True),
    # Column name G has trailing s
    (pd.read_csv("paqc/tests/data/qc8_check2.csv"), False),
    # Added some special columns
    (pd.read_csv("paqc/tests/data/qc8_check3.csv"), True),
])
def test_qc8(df, expected, dict_config):
    assert qc8(df, dict_config).passed == expected
