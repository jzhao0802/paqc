import pytest
import pandas as pd
from paqc.qc_functions.qc6 import qc6
from paqc.utils.config_utils import config_open

@pytest.mark.parametrize("dict_config", [
    config_open("paqc/data/driver_dict_output.yml")[1]
])
@pytest.mark.parametrize("df, expected", [
    # Subset from data/qc_data.csv
    (pd.read_csv("paqc/tests/data/qc6_check1.csv"), True),
    # GENDER is empty
    (pd.read_csv("paqc/tests/data/qc6_check2.csv"), False),
    # Sparse dataframe, GENDER only one field with 'F', but no empty columns
    (pd.read_csv("paqc/tests/data/qc6_check3.csv"), True),
])
def test_qc6(df, expected, dict_config):
    assert qc6(df, dict_config).passed == expected