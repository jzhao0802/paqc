"""
Functions for reading, parsing and sanity checking the YAML config files of the
QC pipelines.
"""

import os
import yaml
import re
import numpy as np
import copy
from paqc.utils import utils


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
        mandatory_general_fields = {'flag_cols', 'count_cols', 'freq_cols',
                                    'first_exp_date_cols', 'last_exp_date_cols',
                                    'index_date_col', 'lookback_date_col',
                                    'date_format', 'date_cols', 'target_col',
                                    'patient_id_col', 'gender_col', 'age_col',
                                    'special_cols', 'matched_patient_id_col'}
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
        qcs_compare = utils.get_qcs_compare()
        mandatory_qc_fields = {'order', 'input_file', 'level'}
        severity_levels = ['error', 'warning', 'info']
        for k, v in qcs.items():
            if bool(re.match(r"^qc\d{1,3}$", k)):
                counter += 1
                # check if qc is one of the known ones
                if k not in qcs_compare:
                    print("ConfigError: Unknown QC function: %s." % k)
                    return False
                # check if the qc has all necessary fields
                if not mandatory_qc_fields.issubset(v.keys()):
                    print("ConfigError: Each QC must have these fields: "
                          "%s." % ', '.join(list(mandatory_qc_fields)))
                    return False
                # check if the severity level is specified as supposed to
                elif v['level'] not in severity_levels:
                    print("ConfigError: QC severity level has to be one "
                          "of %s." % ', '.join(severity_levels))
                    return False
                # check order is an int and add it to the list of order
                elif not isinstance(v['order'], int):
                    print("ConfigError: %s has a non-integer order." % k)
                    return False
                else:
                    order_nums.append(v['order'])

                # check if the input_file is one that's listed in general
                input_file = v['input_file']
                multiple_inputs = not isinstance(input_file, str)
                cond1 = not multiple_inputs and input_file not in inputs
                cond2 = multiple_inputs and not set(input_file).issubset(inputs)
                if cond1 or cond2:
                    print("ConfigError: %s has an input_file (%s) that is not "
                          "listed in the general section as an input." %
                          (k, input_file))
                    return False

                # check if the qc is a comparison one and it has two inputs
                if ((qcs_compare[k] and not multiple_inputs) or
                    (qcs_compare[k] and len(input_file) != 2)):
                    print("ConfigError: Comparison QC (%s) has to have exactly "
                          "two input files." % k)
                    return False
                # check if the two input files are the same
                elif qcs_compare[k] and input_file[0] == input_file[1]:
                    print("ConfigError: Comparison QC (%s) has to have two"
                          " different input files." % k)
                    return False
                # check if any of the input files are multi-files
                if qcs_compare[k]:
                    for input_file_key in input_file:
                        input_file_path = yml['general'][input_file_key ]
                        if bool(re.search(r"\*", input_file_path )):
                            print(
                                "ConfigError: Comparison QC (%s) has to have "
                                "two full datafiles, and not multi-file input"
                                " path: %s." % (k, input_file_path))
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


def config_parser(yml):
    """
    Once the :func:`~utils.config_utils.config_checker` checked the config file
    this function extracts some important bits from it for the driver and also
    reformats the structure of the QCs dict part to make it indexed by the
    input_files.

    :param yml: Output of :func:`~utils.config_utils.config_open`.
    :return: Reformatted YAML config object (dict of dicts), where the files
             with multiple inputs (* in their name) are converted to lists and
             the qcs section is replaced and reformatted to be indexed by input
             files.
    """

    # extract multi inputs and put all their files into a list
    general = yml['general']
    input_paths = {}
    for k, v in general.items():
        # input files that have a * in their name are read in piece by piece
        # and treated as separate data files.
        if bool(re.match(r"^input\d{1,2}$", k)) and bool(re.search(r"\*", v)):
            if k not in input_paths:
                input_paths[k] = []
            # get files in dict
            dir_name = os.path.dirname(v)
            files = os.listdir(dir_name)
            # search for the files we need, make regex pattern for it
            pattern = os.path.basename(v).replace('*', '\d{1,3}')
            for file in files:
                if bool(re.match(pattern, file)):
                    input_paths[k].append('/'.join([dir_name, file]))
        else:
            input_paths[k] = v
        # update original yml object
        general[k] = input_paths[k]

    # refactor structure of yml, so that we can load each input file separately
    # do all tests on it then proceed with the next data file
    qcs = yml['qcs']
    input_qc = dict()
    qcs_compare = utils.get_qcs_compare()
    for input_file in input_paths.keys():
        for qc in qcs:
            # does this qc have to be applied to multiple input_files?
            multiple_inputs = not isinstance(qcs[qc]['input_file'], str)
            # is this a qc that is for comparing two dataframes?
            if not qcs_compare[qc]:
                if multiple_inputs and input_file in qcs[qc]['input_file']:
                    add_qc_to_input_qc_dict(input_qc, input_file, qc)
                elif not multiple_inputs and input_file == qcs[qc]['input_file']:
                    add_qc_to_input_qc_dict(input_qc, input_file, qc)
            else:
                # add comparison qc to the root of the config file
                yml[qc] = qcs[qc]

    # replace qc part of yml with new structure
    for k, v in input_qc.items():
        # add a new node to the config object for the input file
        yml[k] = dict()
        # fill up the new node with qcs that we need to carry out on this input
        for qc in v:
            yml[k][qc] = copy.deepcopy(yml['qcs'][qc])
            # update the input_file param to ensure it points to a single file
            yml[k][qc]['input_file'] = k

    # delete old qcs section
    yml.pop('qcs', None)

    return yml


def add_qc_to_input_qc_dict(input_qc, input_file, qc):
    """
    Helper function that adds qc functions to the input_qc dictionary, mapping
    each qc to one (or multiple) input files.
    :param input_qc: Dictionary to be updated with input1,...,input_n as keys
    :param input_file: Name of the key to update or create: input1,...,input_n
    :param qc: name of the qc function: qc1,...,qc_n
    :return: updated input_qc dictionary
    """
    if input_file not in input_qc:
        input_qc[input_file] = [qc]
    else:
        input_qc[input_file].append(qc)
