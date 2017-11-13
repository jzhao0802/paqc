"""
This submodule contains classes that build up, store and handle the report that
is generated during the run of the series of tests.
"""

import os
import numpy as np
import pandas as pd
from time import gmtime, strftime


class ReportItem:
    """
    Simple lass for the output of an individual test. Each item has to have a
    level, and input file, and a text the holds the info, warning or error
    message.
    """

    def __init__(self, passed, level, order, input_file, input_file_path,
                 extra=None, text=None, exec_time=0, qc_params={}):
        self.level = level
        self.passed = passed
        self.order = order
        self.input_file = input_file
        self.input_file_path = input_file_path
        self.text = text
        self.extra = extra
        self.exec_time = exec_time
        self.qc_params = qc_params

    @classmethod
    def init_conditional(cls, list_failures, dict_config):
        if not list_failures:
            return cls(passed=True, **dict_config)
        else:
            return cls(passed=False, extra=list_failures, **dict_config)

    def summarise_report_item(self):
        return ("Test item #%d (level: %s) was carried out on %s: %s. \nIt "
                "has "
                "passed: %s, in %d seconds, with the following message:\n%s" %
                (self.order, self.level, self.input_file, self.input_file_path,
                 self.passed, self.exec_time, self.text))

    def print_report_item(self):
        print(self.summarise_report_item())

    def update_level(self, level):
        self.level = level

    def update_text(self, text):
        self.text = text

    def update_exec_time(self, exec_time):
        self.exec_time = exec_time


class Report:
    """
    Class for building up, storing, ordering and managing the report items of
    individual tests.
    """

    def __init__(self, config):
        self.config = config
        self.items = []
        self.datetime = strftime("%Y-%m-%d %H:%M:%S", gmtime())

    def add_item(self, report_item):
        """
        Adds a new ReportItem to the Report.

        :param report_item: :obj:`~report.report.ReportItem`
        :return: None.
        """
        self.items.append(report_item)

    def order_items(self, most_severe_first=True):
        """
        This method will order the report items based on their severity,
        where error > warning > info.

        :param most_severe_first: If set to False, info items are the first
        in the generated report.
        :return: None. This simply reorders the internal state of the Report.
        """
        levels = []
        for item in self.items:
            if item.level == "error":
                levels.append(1)
            elif item.level == "warning":
                levels.append(2)
            else:
                levels.append(3)
        self.items = np.array(self.items)
        ordering = np.argsort(levels)
        if not most_severe_first:
            ordering = ordering[::-1]
        self.items = self.items[ordering]

    def print_items(self):
        """
        Simply prints the Report object's ReportItems to the terminal.

        :return: Nothing, prints to terminal.
        """
        for i, item in enumerate(self.items):
            print("%d.   %s" % (i, item.summarise_report_item()))

    def get_total_exec_time(self):
        """
        Return total execution time of the QC suite.

        :return: Number of seconds it took to execute all QC functions.
        """
        total_exec_time = 0
        for item in self.items:
            total_exec_time += item.exec_time
        return total_exec_time

    def get_report_table(self):
        """
        Turns the internal list of ReportItem obj representation of the
        Report object into a pandas DataFrame.

        :return: pandas DataFrame where each row is a ReportItem.
        """

        if len(self.items) == 0:
            print('Use the add_item() method to add ReportItem to the Report '
                  'object first, then we can turn it into a pandas DataFrame.')
            return None
        else:
            # extract class attributes from ReportItem so we don't hardcode them
            item_attrs = list(self.items[0].__dict__.keys())
            report_d = {i: [] for i in item_attrs if i[:1] != '_'}

        for item in self.items:
            for item_attr in item_attrs:
                value = getattr(item, item_attr)
                # add severity level numerically, so ordering in HTML is easier
                if item_attr == 'level' and value == 'error':
                    value = "1_error"
                elif item_attr == 'level' and value == 'warning':
                    value = "2_warning"
                else:
                    value = "3_info"
                report_d[item_attr].append(value)

        return pd.DataFrame.from_dict(report_d)

    def generate_report(self):
        """
        This function simply generates the final report HTML and the JSON
        that feeds the JavaScript tables. It also saves the tables as csv
        :return: Nothing.
        """

        output_dir = self.config['general']['output_dir']
        # order ReportItems, and get report table
        self.order_items()
        report_df = self.get_report_table()

        # save CSV
        report_df.to_csv(os.path.join(output_dir, 'qc_report.csv'))

        # table to JSON, but make it legible by breaking lines and adding tabs
        json_str = report_df.to_json(None, orient='records')
        n1 = '    '
        n2 = n1 * 2
        json_str = json_str.replace('","', '",\n' + n2 + '"')
        json_str = json_str.replace('},{', '},\n' + n2 + '{')
        # add prefix and suffix so Data-tables (JS library) accepts it
        json_str = '{\n' + n1 + '"data":\n' + n2 + json_str + '\n}'

        # save JSON
        out_file = os.path.join(output_dir, 'qc_report.json')
        f = open(out_file, 'w')
        f.write(json_str)
        f.close()

        # write HTML that uses the JSON with Data-tables JS library



