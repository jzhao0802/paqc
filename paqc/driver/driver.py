"""
This submodule contains the main class that takes in the config file and
executes the QC scripts in the requested order and collates their findings in a
report.
"""

import time
from paqc.connectors import csv
from paqc.utils import config_utils
from paqc.utils import utils
from paqc.report import report
import paqc.qc_functions as qcs_main


class Driver:
    """
    Main executor class of the the QC pipeline. It reads in a YAML config
    file, checks it, then executes the tests on the specified input files one by
    one, while collecting their output and generating a report from it.
    """

    def __init__(self, config_path, verbose=True, debug=True):
        self.config = config_path
        self.general = None
        self.report = None
        self.verbose = verbose
        self.debug = debug
        # load the QC functions into a single dict
        self.qc_functions = qcs_main.import_submodules(qcs_main)
        self.config_loader()

    def config_loader(self):
        """
        This will load, check and parse the config file for the driver.

        :return: Nothing. Updates the internal config object of the driver.
        """
        self.printer("Loading config file...")
        self.config = config_utils.config_open(self.config)
        if self.config[0]:
            self.printer("Checking and parsing config file...")
            self.config = self.config[1]
            if config_utils.config_checker(self.config):
                self.config = config_utils.config_parser(self.config)
                self.general = self.config['general']
                self.report = report.Report(self.config)
                self.printer("Config file checked and parsed. "
                             "Starting QC pipeline...")
            else:
                self.printer("Check the config file, some mandatory fields "
                             "are either missing or misformatted.")
                return
        else:
            self.printer("Couldn't load config file. It's badly formatted.")
            return

        # parsed config successfully, let's execute qc functions
        self.main()

    def main(self):
        """
        This is the main function or the 'brain' of the Driver class, which
        will execute the qc functions one by one of the desired input files
        even if those have multiple chunked data files within them.

        :return: Nothing, calls the :func:`~driver.driver.do_qc` on each
                 data_file and executes the required qc functions, then
                 generates the .csv and HTML report
        """

        # loop through the data files
        for k, v in self.config.items():
            if k != 'general':
                # check if input data has multiple file paths
                if not isinstance(self.general[k], str):
                    for input_file_path in self.general[k]:
                        self.printer("Starting QCs on %s, with file path: %s" %
                                     (k, input_file_path), True)
                        self.do_qc(k, input_file_path, v)
                else:
                    self.printer("Starting QCs on %s, file path: %s" %
                                 (k, self.general[k]), True)
                    self.do_qc(k, self.general[k], v)

        # generate final report
        self.printer("Generating HTML and CSV report...", True, True)
        self.report.generate_report()

    def do_qc(self, input_file, input_file_path, qcs):
        """
        This is the function that actually takes an input data file with a
        path, loads it, then calls all required qc functions on it.

        :param input_file: input1,...,input_n
        :param input_file_path: actual file path to the data
        :param qcs: dictionary of qcs to execute on a given data file.
        :return: Nothing, updates Driver's internal report object.
        """

        df = self.data_loader(input_file_path)
        df_hash = utils.generate_hash(df)
        print("Unique generated hash: %d" % df_hash)
        for qc in qcs:
            # generate mini config object for the QC function
            qc_config = {'general': self.general, 'qc': qcs[qc]}
            qc_config['qc']['input_file_path'] = input_file_path
            qc_config['qc']['qc_num'] = qc
            # extract the specific QC object from the qc_functions module
            qc_function = self.qc_functions[qc]

            # execute and time it on the data file
            self.printer("Executing test %s on %s: %s" %
                         (qc, input_file, input_file_path))
            ts = time.time()
            if self.debug:
                rpi = qc_function(df, qc_config)
            # if we're not in debug mode, don't stop at bugs, log them as errors
            else:
                try:
                    rpi = qc_function(df, qc_config)
                except:
                    text = "QC failed due to internal bug, report it to admins!"
                    rpi = report.ReportItem(passed=False, level="error",
                                            order=1, qc_num=qc,
                                            input_file=input_file, text=text,
                                            input_file_path=input_file_path)
            te = time.time()
            rpi.exec_time = te - ts
            self.report.add_item(rpi)

    def report_generator(self):
        """
        Generates an HTML and .txt report from the Driver's
        :obj:`~report.report.Report`

        :return: Nothing, generates report to the output folder instead.
        """

        self.report.print_items()
        return True

    def data_loader(self, input_file_path):
        """
        Loads an input data file using its path and the source argument of
        the config file.

        :param input_file_path: path to the data.
        :return: pandas DataFrame object of the fully loaded datafile.
        """

        source = self.config['general']['source']
        if source != 'csv':
            raise ValueError("We only support .csv input files currently.")

        if source == 'csv':
            loaded, df = csv.read_csv(self.config, input_file_path)
            if loaded:
                return df
            else:
                raise ValueError("We couldn't load the following file: %s. "
                                 "It's most likely, that your dates are "
                                 "formatted inconsistently. Make sure to visit "
                                 "http://strftime.org/ for the correct date "
                                 "format specs." % input_file_path)

    def printer(self, to_print, hline_before=False, hline_after=False):
        """
        Simple wrapper function, that prints messages to users if the driver
        object was instantiated with verbose=True.

        :param to_print: Message to print.
        :param hline_before: Boolean, should a horizontal like be printed
               before the to_print string?
        :param hline_after: Boolean, should a horizontal like be printed
               after the to_print string?
        :return: Nothing, prints to standard out.
        """
        if self.verbose:
            if hline_before:
                print("-------------------------------------------------------")
            print(to_print)
            if hline_after:
                print("-------------------------------------------------------")
