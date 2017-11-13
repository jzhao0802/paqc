import pytest
import pandas as pd

from paqc.qc_functions.qc16 import qc16
from paqc.utils.config_utils import config_open


@pytest.mark.parametrize("dict_config", [
    config_open("paqc/tests/data/qc16_driver_dict_output.yml")[1]
])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Subset from data/qc_data.csv
    (pd.read_csv("paqc/tests/data/qc16_check1.csv"), True, None),
    # Gender is 1 for label 0 samples and 0 for label 1 samples
    (pd.read_csv("paqc/tests/data/qc16_check2.csv"), False, ['GENDER']),
    # Same, + D_7373_AVG_CLAIM_CNT is missing more often for label 0 samples
    (pd.read_csv("paqc/tests/data/qc16_check3.csv"), False, ['GENDER',
                                                             'D_7373_AVG_CLAIM_CNT'])
])
def test_qc16(df, expected, ls_faults, dict_config):
    rpi = qc16(df, dict_config)
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)
