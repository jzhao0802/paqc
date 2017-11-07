import pytest

from paqc.utils.config_utils import config_open, config_checker


@pytest.mark.parametrize("path_to_file, expected", [
    # properly formatted YAML file
    ("paqc/tests/data/config_test_open1.yml", True),
    # misformatted YAML file
    ("paqc/tests/data/config_test_open2.yml", False)
])
def test_config_open(path_to_file, expected):
    assert config_open(path_to_file)[0] == expected


@pytest.mark.parametrize("path_to_file, expected", [
    # no general section
    ("paqc/tests/data/config_test_check1.yml", False),
    # no input file in general section
    ("paqc/tests/data/config_test_check2.yml", False),
    # no output dir in general section
    ("paqc/tests/data/config_test_check3.yml", False),
    # no source in general section
    ("paqc/tests/data/config_test_check4.yml", False),
    # misformatted source in general section
    ("paqc/tests/data/config_test_check5.yml", False),
    # missing column definitions #1
    ("paqc/tests/data/config_test_check6.yml", False),
    # missing column definitions #1
    ("paqc/tests/data/config_test_check7.yml", False),
    # missing column definitions #1
    ("paqc/tests/data/config_test_check8.yml", False),
    # no qcs section
    ("paqc/tests/data/config_test_check9.yml", False),
    # not all of the mandatory qcs fields (input, level, order) are present
    ("paqc/tests/data/config_test_check10.yml", False),
    # misformatted level field in qc
    ("paqc/tests/data/config_test_check11.yml", False),
    # bad input file referenced for qc
    ("paqc/tests/data/config_test_check12.yml", False),
    # non-integer order specified for qc
    ("paqc/tests/data/config_test_check13.yml", False),
    # none unique order for test (each should have a different)
    ("paqc/tests/data/config_test_check14.yml", False),
    # properly formatted config YAML
    ("paqc/tests/data/config_test_check15.yml", True)
])
def test_config_checker(path_to_file, expected):
    assert config_checker(config_open(path_to_file)[1]) == expected


