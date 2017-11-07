"""
Functions for reading, parsing and sanity checking the YAML config files of the
QC pipelines.
"""


import yaml
import re
import numpy as np


def config_open(path):
    """
    Reads in the YAML config file and  displays any parsing or formatting
    errors. If the parsing fails the user is presented with the line that
    failed. Usually the formatting error is right above this line.

    :param path: absolute path to the YAML formatted config file.
    :return: Tuple of Boolean and parsed YAML file. The boolean is indicating
             whether the opening and parsing of the YAML file was successful,
             and the YAML config object is a dict of dicts and lists.
    """

    config_text = open(path).read()

    try:
        yml = yaml.load(config_text, yaml.SafeLoader)
        return True, yml
    except yaml.YAMLError as exc:
        print("Error while parsing YAML file:")
        if hasattr(exc, 'problem_mark'):
            if exc.context is not None:
                print('  parser says\n' + str(exc.problem_mark) + '\n  ' +
                      str(exc.problem) + ' ' + str(exc.context) +
                      '\nPlease correct data and retry.')
            else:
                print('  parser says\n' + str(exc.problem_mark) + '\n  ' +
                      str(exc.problem) + '\nPlease correct data and retry.')
        else:
            print("Something went wrong while parsing yaml file")

        return False, None


def config_checker(yml):
    """
    Parses the YAML config file and checks that the minimum required fields for
    the general setup of the test suite and for each test are present. If the
    config checking fails at any point, the user is informed about it.

    :param yml: Output of :func:`~utils.config_utils.config_open`.
    :return: Boolean.
    """

    # --------------------------------------------------------------------------
    # test general params
    if 'general' not in yml:
        print("ConfigError: You need to specify a 'general' section.")
        return False
    elif yml['general'] is None:
        print("ConfigError: General section is empty.")
        return False
    else:
        general = yml['general']

        # check if we have at least one input
        inputs = []
        for k, v in general.items():
            if bool(re.match(r"^input\d{1,2}$", k)):
                inputs.append(k)
        if len(inputs) == 0:
            print("ConfigError: You need to specify at least one input file.")
            return False

        # check if we have output dir specified
        if 'output_dir' not in general:
            print("ConfigError: You need to specify the output_dir.")
            return False

        # check source of data
        if 'source' not in general:
            print("ConfigError: You need to specify the source of your data. "
                  "Possible values: csv, bdf, sql.")
            return False
        else:
            if general['source'] not in ['csv', 'sql', 'bdf']:
                print("ConfigError: Source must be one of: csv, bdf, sql.")
                return False

        # test mandatory column name fields
        mandatory_general_fields = {'date_cols', 'count_cols', 'freq_cols',
                                    'first_exp_date_cols', 'last_exp_date_cols',
                                    'target_col', 'patient_id',
                                    'matched_patient_id'}
        if not mandatory_general_fields <= set(general.keys()):
            print("ConfigError: The general section must have these fields: "
                  "%s." % ', '.join(list(mandatory_general_fields)))
            return False

    # --------------------------------------------------------------------------
    # go through test params
    if 'tests' not in yml:
        print("ConfigError: You need to specify a 'tests' section.")
        return False
    else:
        tests = yml['tests']

        # check if we have at least one test and if they are well spec-ed
        counter = 0
        non_tests = []
        order_nums = []
        mandatory_test_fields = {'order', 'input', 'level'}
        severitity_levels = ['error', 'warning', 'info']
        for k, v in tests.items():
            if bool(re.match(r"^test\d{1,3}$", k)):
                counter += 1
                # check if the test has all necessary fields
                if not mandatory_test_fields <= set(tests[k].keys()):
                    print("ConfigError: Each test must have these fields: "
                          "%s." % ', '.join(list(mandatory_test_fields)))
                    return False
                # check if the severity level is specified as supoosed to
                elif tests[k]['level'] not in severitity_levels:
                    print("ConfigError: Test severity level has to be one "
                          "of %s." % ', '.join(severitity_levels))
                    return False
                # check if the input is one of ones listed in general
                elif tests[k]['input'] not in inputs:
                    print("ConfigError: %s has an input that is not "
                          "listed in the general section." % k)
                    return False
                # check order is an int and add it to the list of order
                elif not isinstance(tests[k]['order'], int):
                    print("ConfigError: %s has a non-integer order." % k)
                    return False
                else:
                    order_nums.append(tests[k]['order'])
            else:
                # we'll warn the user about mis-formatted tags later
                non_tests.append(k)
        if counter == 0:
            print("ConfigError: You need to specify at least one test.")
            return False
        elif len(non_tests) > 0:
            print("ConfigWarning: The following tags within the tests "
                  "section will be ignored %s." % ', '.join(non_tests))
        if len(np.unique(order_nums)) != len(order_nums):
            print("ConfigError: No two tests should have the same order.")
            return False
    return True
