import pytest
import pandas as pd
from paqc.qc_functions.qc7 import qc7
from paqc.utils.config_utils import config_open

@pytest.mark.parametrize("dict_config", [
    config_open("paqc/data/driver_dict_output.yml")[1]
])
@pytest.mark.parametrize("df, expected", [
    # Original column names from data/qc_data.csv
    (pd.read_csv("paqc/tests/data/qc7_check1.csv"), True),
    # Row 4 is empty
    (pd.read_csv("paqc/tests/data/qc7_check2.csv"), False),
    # Sparse dataframe
    (pd.read_csv("paqc/tests/data/qc7_check3.csv"), True),
])
def test_qc7(df, expected, dict_config):
    assert qc7(df, dict_config).passed == expected



