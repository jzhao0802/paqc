"""
This submodule contains the main class that takes in the config file and
executes the QC scripts in the requested order and collates their findings in a
report.
"""

from paqc.utils import config_utils as cu
from paqc.utils import utils as u

class QCDriver:
    """
    Main executor class of the the QC pipeline. It reads in a YAML config
    file, checks it, then executes the tests on the specified input files one by
    one, while collecting their output and generating a report from it.
    """

    def __init__(self, config_path, verbose=True):
        self.config = cu.config_open(config_path)
        self.verbose = verbose

        # load, parse and check config file
        self.printer("Loading config file...")
        if self.config[0]:
            self.printer("Parsing and checking config file...")
            self.config = self.config[1]
            if cu.config_checker(self.config):
                self.printer("Config file checked, starting testing..")
            else:
                self.printer("Check the config file, some mandatory fields "
                             "are either missing or misformatted.")
        else:
            raise ValueError("Couldn't load config file. It's badly formatted.")

        # load input files


        # generate report object

    def printer(self, to_print):
        if self.verbose:
            print(to_print)

