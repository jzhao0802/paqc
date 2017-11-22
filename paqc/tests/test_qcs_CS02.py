import pytest

from paqc.connectors import csv
from paqc.qc_functions.qcs_CS02 import *
from paqc.utils.config_utils import config_open

DICT_CONFIG_CS02 = config_open("paqc/tests/data/driver_dict_output_CS02.yml")[1]

# 23

# 24
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_CS02])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Original column names from data/initial_pos.csv
    (csv.read_csv(DICT_CONFIG_CS02, "paqc/tests/data/qc24_check1.csv")[1],
     True, None),
    # Row 1 has a date value for diseasefirstexp
    (csv.read_csv(DICT_CONFIG_CS02, "paqc/tests/data/qc24_check2.csv")[1],
     False, [1])
])
def test_qc24(df, expected, ls_faults, dict_config):
    rpi = qc24(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)