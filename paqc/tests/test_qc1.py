import pytest
import pandas as pd
from paqc.qc_functions.qc1 import qc1

@pytest.mark.parametrize("df, expected", [
    # Original column names from data/qc_data.csv
    (pd.read_csv("paqc/tests/data/qc1_check1.csv"), True),
    # GENDER has trailing space, D_7931_AVG_CLAIM_CNT leading space
    (pd.read_csv("paqc/tests/data/qc1_check2.csv"), False),
    # Second column name is empty
    (pd.read_csv("paqc/tests/data/qc1_check3.csv"), False),
    # Created column name with single $
    (pd.read_csv("paqc/tests/data/qc1_check4.csv"), False),
    # First column name is lab*el
    (pd.read_csv("paqc/tests/data/qc1_check5.csv"), False)
])
def test_qc1(df, expected):
    assert qc1(df) == expected