import pytest
import pandas as pd

from paqc.qc_functions.qc4 import qc4
from paqc.utils.config_utils import config_open


@pytest.mark.parametrize("dict_config", [
    config_open("paqc/data/driver_dict_output.yml")[1]
])
@pytest.mark.parametrize("df, expected, ls_duplicates", [
    # Subset from data/qc_data.csv
    (pd.read_csv("paqc/tests/data/qc4_check1.csv"), True, None),
    # Made two patient_ids equal, in row indices 1 and 2
    (pd.read_csv("paqc/tests/data/qc4_check2.csv"), False, [1, 2]),
    # Two empty patient_id cells, not considered as duplicate
    (pd.read_csv("paqc/tests/data/qc4_check3.csv"), True, None),
    # Row with indices 3 and 4 have a dot (.) as patient_id, considered
    # valid input and thus flagged as duplicates
     (pd.read_csv("paqc/tests/data/qc4_check4.csv"), False, [3, 4])
])
def test_qc4(df, expected, ls_duplicates, dict_config):
    rpi = qc4(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_duplicates)