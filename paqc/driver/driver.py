"""
This submodule contains the main class that takes in the config file and
executes the QC scripts in the requested order and collates their findings in a
report.
"""

import time
import re
import traceback
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
        self.config_path = config_path
        self.config = None
        self.general = None
        self.report = None
        self.verbose = verbose
        self.debug = debug
        # load the QC functions into a single dict
        self.qc_functions = qcs_main.import_submodules(qcs_main)
        # load list of comparison qc functions
        self.qcs_compare = utils.get_qcs_compare()
        # list of dataframes we load in to run comparison type QCs on
        self.compare_dfs = dict()
        self.compare_dfs_hash = dict()

    def run(self, generate_report=True):
        """
        Runs the QC pipeline by executing the various methods of the Driver
        class in succession.

        :param generate_report: Boolean, if set to False Driver will not
                                generate report, but simply run the QCs.
        :return: None
        """

        # load, check, parse config
        self.config_loader()
        # parsed config successfully, let's execute qc functions
        self.main()
        # generate report
        if generate_report:
            self.printer("Generating HTML and CSV report...", True, True)
            self.report.generate_report()

    def config_loader(self):
        """
        This will load, check and parse the config file for the driver.

        :return: Nothing. Updates the internal config object of the driver.
        """
        self.printer("Loading config file...")
        self.config = config_utils.config_open(self.config_path)
        if self.config[0]:
            self.printer("Checking and parsing config file...")
            self.config = self.config[1]
            if config_utils.config_checker(self.config):
                self.config = config_utils.config_parser(self.config)
                self.general = self.config['general']
                # init the report object
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

    def main(self):
        """
        This is the main function or the 'brain' of the Driver class, which
        will execute the qc functions one by one of the desired input files
        even if those have multiple chunked data files within them.
        If we encounter QCs that are comparing two dataframes, we load both
        required files and perform the qc.

        :return: Nothing, calls the :func:`~driver.driver.do_qc` on each
                 data_file and executes the required qc functions, then
                 generates the .csv and HTML report
        """

        # loop through the config file
        for k, v in self.config.items():
            # if we find a data file, load it and execute all of its QCs
            if bool(re.match(r"^input\d{1,2}$", k)):
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

            # if qc, it has to be a comparison one that we  handle differently
            elif bool(re.match(r"^qc\d{1,3}$", k)):
                input1 = self.config[k]['input_file'][0]
                input2 = self.config[k]['input_file'][1]
                self.do_compare_qc(input1, input2, k)

    def do_qc(self, input_file, input_file_path, qcs):
        """
        This is the function that actually takes an input data file with a
        path, loads it, then calls all required qc functions on it.

        :param input_file: input1,...,input_n in general part of config
        :param input_file_path: actual file path to the data
        :param qcs: dictionary of qcs to execute on a given data file.
        :return: Nothing, updates Driver's internal report object.
        """

        df = self.data_loader(input_file_path)
        df_hash = utils.generate_hash(df)
        for qc in qcs:
            # generate mini config object for the QC function
            qc_config = {'general': self.general, 'qc': qcs[qc]}
            qc_config['qc']['input_file_path'] = input_file_path
            qc_config['qc']['qc_num'] = qc
            qc_config['qc']['data_hash'] = df_hash

            # extract the specific QC object from the qc_functions module
            qc_function = self.qc_functions[qc]

            # execute and time it on the data file
            self.printer("Executing test %s on %s: %s" %
                         (qc, input_file, input_file_path))
            ts = time.time()

            # check if we have params for this qc function
            if "qc_params" in qc_config['qc']:
                if qc_config['qc']['qc_params'] is None:
                    qc_params = dict()
                else:
                    qc_params = qc_config['qc']['qc_params']
            else:
                qc_params = dict()

            if self.debug:
                rpi = qc_function(df, qc_config, **qc_params)
            # if we're not in debug mode, don't stop at bugs, log them as errors
            else:
                try:
                    rpi = qc_function(df, qc_config, **qc_params)
                # Some qcs need to load an extra csv with path given in config,
                # this error is raised when the file does not exist.
                except FileNotFoundError as e:
                    text = str(e)
                    rpi = report.ReportItem(passed=False, level="error",
                                            order=1, qc_num=qc,
                                            input_file=input_file, text=text,
                                            input_file_path=input_file_path)
                except:
                    text = ("QC failed due to internal bug, report it to "
                            "admins with this error:\n%s"
                            % traceback.format_exc())
                    rpi = report.ReportItem(passed=False, level="error",
                                            order=1, qc_num=qc,
                                            input_file=input_file, text=text,
                                            input_file_path=input_file_path)
            te = time.time()
            rpi.exec_time = te - ts
            self.report.add_item(rpi)

    def do_compare_qc(self, input_file1, input_file2, qc):
        """
        Function for executing QCs on two input dataframes at once. If the
        dataframes are already loaded, we use them, otherwise we load them too.

        :param input_file1: input1,...,input_n in general part of config
        :param input_file2: input1,...,input_n in general part of config
        :param qc: string, name of qc to execute on the two dataframes.
        :return: Nothing, updates Driver's internal report object.
        """

        # check if dataset is already loaded, if not load it, hash it
        if input_file1 not in self.compare_dfs:
            input_file_path = self.config['general'][input_file1]
            self.compare_dfs[input_file1] = self.data_loader(input_file_path)
            self.compare_dfs_hash[input_file1] = utils.generate_hash(
                self.compare_dfs[input_file1])
        if input_file2 not in self.compare_dfs:
            input_file_path = self.config['general'][input_file2]
            self.compare_dfs[input_file2] = self.data_loader(input_file_path)
            self.compare_dfs_hash[input_file2] = utils.generate_hash(
                self.compare_dfs[input_file2])

        # variables to shorten lines hereafter
        df1 = self.compare_dfs[input_file1]
        df2 = self.compare_dfs[input_file2]
        input_file_path1 = self.config['general'][input_file1]
        input_file_path2 = self.config['general'][input_file2]
        input_files = ("%s and %s" % (input_file1, input_file2))
        input_file_paths = ("%s and %s" % (input_file_path1, input_file_path2))
        hash1 = self.compare_dfs_hash[input_file1]
        hash2 = self.compare_dfs_hash[input_file2]

        # generate mini config object for the QC function
        qc_config = {'general': self.general, 'qc': self.config[qc]}
        qc_config['qc']['input_file_path'] = input_file_paths
        qc_config['qc']['qc_num'] = qc
        qc_config['qc']['data_hash'] = ("%s: %d\n%s: %d" % (input_file1, hash1,
                                                            input_file2, hash2))

        # extract the specific QC object from the qc_functions module
        qc_function = self.qc_functions[qc]

        # execute and time it on the data file
        self.printer("Executing test %s on %s: %s \nand %s: %s" %
                     (qc, input_file1, input_file_path1,
                      input_file2, input_file_path2))
        ts = time.time()

        # check if we have params for this qc function
        if "qc_params" in qc_config:
            qc_params = qc_config["qc_params"]
        else:
            qc_params = dict()

        if self.debug:
            rpi = qc_function(df1, df2, qc_config, **qc_params)
        # if we're not in debug mode, don't stop at bugs, log them as errors
        else:
            try:
                rpi = qc_function(df1, df2, qc_config, **qc_params)
            # Some qcs need to load an extra csv with path given in config,
            # this error is raised when the file does not exist.
            except FileNotFoundError as e:
                text = str(e)
                rpi = report.ReportItem(passed=False, level="error",
                                        order=1, qc_num=qc,
                                        input_file=input_files, text=text,
                                        input_file_path=input_file_paths)
            except:
                text = ("QC failed due to internal bug, report it to "
                        "admins with this error:\n%s"
                        % traceback.format_exc())
                rpi = report.ReportItem(passed=False, level="error",
                                        order=1, qc_num=qc,
                                        input_file=input_files, text=text,
                                        input_file_path=input_file_paths)
        te = time.time()
        rpi.exec_time = te - ts
        self.report.add_item(rpi)

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
            try:
                return csv.read_csv(self.config, input_file_path)
            except:
                self.printer("We couldn't load the following file: %s. It's "
                             "most likely, that your dates are formatted "
                             "inconsistently. Make sure to visit "
                             "http://strftime.org/ for the correct date format"
                             " specs.\n\nTRACEBACK:\n\n%s"
                             % (input_file_path, traceback.format_exc()))

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
