"""
Functions for reading, parsing and sanity checking the YAML config files of the
QC pipelines.
"""

import os
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
    the general setup of the QC suite and for each QC are present. If the
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
        if not mandatory_general_fields.issubset(general.keys()):
            print("ConfigError: The general section must have these fields: "
                  "%s." % ', '.join(list(mandatory_general_fields)))
            return False

    # --------------------------------------------------------------------------
    # go through qc params
    if 'qcs' not in yml:
        print("ConfigError: You need to specify a 'qcs' section.")
        return False
    else:
        qcs = yml['qcs']

        # check if we have at least one QC and if they are well spec-ed
        counter = 0
        non_qcs = []
        order_nums = []
        mandatory_qc_fields = {'order', 'input_file', 'level'}
        severity_levels = ['error', 'warning', 'info']
        for k, v in qcs.items():
            if bool(re.match(r"^qc\d{1,3}$", k)):
                counter += 1
                # check if the qc has all necessary fields
                if not mandatory_qc_fields.issubset(qcs[k].keys()):
                    print("ConfigError: Each QC must have these fields: "
                          "%s." % ', '.join(list(mandatory_qc_fields)))
                    return False
                # check if the severity level is specified as supposed to
                elif qcs[k]['level'] not in severity_levels:
                    print("ConfigError: QC severity level has to be one "
                          "of %s." % ', '.join(severity_levels))
                    return False
                # check order is an int and add it to the list of order
                elif not isinstance(qcs[k]['order'], int):
                    print("ConfigError: %s has a non-integer order." % k)
                    return False
                else:
                    order_nums.append(qcs[k]['order'])
                # check if the input_file is one of ones listed in general
                input_file = qcs[k]['input_file']
                multiple_inputs = not isinstance(input_file, str)
                cond1 = not multiple_inputs and input_file not in inputs
                cond2 = multiple_inputs and not set(input_file).issubset(inputs)
                if cond1 or cond2:
                    print("ConfigError: %s has an input_file that is not "
                          "listed in the general section as an input." % k)
                    return False
            else:
                # we'll warn the user about mis-formatted tags later
                non_qcs.append(k)
        if counter == 0:
            print("ConfigError: You need to specify at least one qc.")
            return False
        elif len(non_qcs) > 0:
            print("ConfigWarning: The following tags within the QCs "
                  "section will be ignored %s." % ', '.join(non_qcs))
        if len(np.unique(order_nums)) != len(order_nums):
            print("ConfigError: No two qcs should have the same order.")
            return False
    return True

def config_extractor(yml):
    """
    Once the :func:`~utils.config_utils.config_checker` checked the config file
    this function extracts some important bits from it for the driver and also
    reformats the structure of the QCs dict part to make it indexed by the
    input_files.

    :param yml: Output of :func:`~utils.config_utils.config_open`.
    :return: reformatted YML as a dict of dict.
    """

    general = yml['general']

    # extract inputs, and multi inputs
    inputs = []
    multi_inputs = []
    for k, v in general.items():
        if bool(re.match(r"^input\d{1,2}$", k)):
            inputs.append(k)
            # input files that have a * in their name are read in piece by
            # piece and treated as separate data files.
            if bool(re.search(r"\*", v)):
                # get files in dict
                dir_name = os.path.dirname(v)
                files = os.listdir(dir_name)
                # search for the files we need, make regex pattern
                pattern = os.path.basename(v).replace('*', '\d{1,3}')
                for file in files:
                    if bool(re.match(pattern, file)):
                        multi_inputs.append('/'.join([dir_name, file]))





