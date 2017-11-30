import pytest

from paqc.connectors import csv
from paqc.qc_functions.qcs_CS04 import *
from paqc.utils.config_utils import config_open

DICT_CONFIG_CS02 = config_open("paqc/tests/data/driver_dict_output_CS02.yml")[1]


# 35
@pytest.mark.parametrize("dict_config", [DICT_CONFIG_CS02])
@pytest.mark.parametrize("df, expected, ls_faults", [
    # Original subset from data/random_sample_scoring.csv
    (csv.read_csv(DICT_CONFIG_CS02, "paqc/tests/data/qc25_check1.csv"),
     True, None),
    # Copied two patient IDs from the cp01 file into check2
    (csv.read_csv(DICT_CONFIG_CS02, "paqc/tests/data/qc25_check2.csv"),
     False, ['57616631', '81744431'])
])
def test_qc35(df, expected, ls_faults, dict_config):
    qc_params = dict_config['qc']['qc_params']
    rpi = qc35(df, dict_config, qc_params['path_file_cp01'], qc_params[
        'pat_id_col_cp01'])
    assert (rpi.passed == expected) & (rpi.extra == ls_faults)