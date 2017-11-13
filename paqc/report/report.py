"""
This submodule contains classes that build up, store and handle the report that
is generated during the run of the series of tests.
"""

import numpy as np
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
        for i, item in enumerate(self.items):
            print("%d.   %s" % (i, item.summarise_report_item()))
