"""
This submodule contains classes that build up, store and handle the report that
is generated during the run of the series of tests.
"""

import os
import numpy as np
import pandas as pd
from paqc.utils import utils
from time import gmtime, strftime, time


class ReportItem:
    """
    Simple lass for the output of an individual test. Each item has to have a
    level, and input file, and a text the holds the info, warning or error
    message.
    """

    def __init__(self, passed, level, order, qc_num, input_file,
                 input_file_path, extra=None, text=None, exec_time=0,
                 qc_params=None, data_hash=None):
        self.level = level
        self.passed = passed
        self.order = order
        self.qc_num = qc_num
        self.input_file = input_file
        self.input_file_path = input_file_path
        self.text = text
        self.extra = extra
        self.exec_time = exec_time
        self.qc_params = qc_params
        self.data_hash = data_hash

    @classmethod
    def init_conditional(cls, list_failures, dict_config):
        if not list_failures:
            return cls(passed=True, **dict_config)
        else:
            return cls(passed=False, extra=list_failures, **dict_config)

    def summarise_report_item(self):
        return ("Test item #%d (level: %s) was carried out on %s: %s. \nIt "
                "has passed: %s, in %s seconds, with the following message:\n%s"
                %(self.qc_num, self.level, self.input_file, self.input_file_path,
                  self.passed, "{0:.4f}".format(self.exec_time), self.text))

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
        self.datetime = strftime("%Y-%m-%d_%H_%M_%S", gmtime())
        self.ts = time()
        self.qcs_desc = utils.get_qcs_desc()

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

    def get_summary_stats(self):
        """
        This gathers a few simple summary stats about the Report object. The
        keys of the returned dict are self-explanatory, so see those.

        :return: Dict, with the following keys: qc_sum, data_file_sum,
                 passed_sum, failed_sum, total_exec_time.
        """

        to_return = dict()
        to_return['qc_sum'] = len(self.items)
        file_counter = {}
        to_return['data_file_sum'] = 0
        to_return['total_exec_time'] = 0
        to_return['passed_sum'] = 0
        to_return['failed_sum'] = 0
        for item in self.items:
            to_return['total_exec_time'] += item.exec_time
            if item.passed:
                to_return['passed_sum'] += 1
            else:
                to_return['failed_sum'] += 1
            if item.input_file_path not in file_counter:
                file_counter[item.input_file_path] = 1
        to_return['data_file_sum'] = len(list(file_counter.keys()))

        return to_return

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

        # add new key so we can sort by level in the HTML table
        report_d['level_int'] = []
        report_d['qc_desc'] = []
        for item in self.items:
            for item_attr in item_attrs:
                value = getattr(item, item_attr)
                # add severity level numerically, so ordering in HTML is easier
                if item_attr == 'level' and value == 'error':
                    report_d['level_int'].append(1)
                elif item_attr == 'level' and value == 'warning':
                    report_d['level_int'].append(2)
                elif item_attr == 'level' and value == 'info':
                    report_d['level_int'].append(3)
                report_d[item_attr].append(value)
            # add description to qc
            report_d['qc_desc'].append(self.qcs_desc[item.qc_num])

        return pd.DataFrame.from_dict(report_d)

    def generate_report(self):
        """
        This function simply generates the final report HTML and the JSON
        that feeds the JavaScript tables. It also saves the tables as csv

        :return: Nothing.
        """

        # extract output dir and create it if necessary (win compatible)
        output_dir = self.config['general']['output_dir']
        output_dir = os.path.expanduser(output_dir)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # order ReportItems, and get report table
        self.order_items()
        report_df = self.get_report_table()

        # reorder columns of report table
        col_order = ['qc_num', 'qc_desc', 'passed', 'level', 'level_int',
                     'order', 'extra', 'input_file', 'input_file_path',
                     'data_hash', 'exec_time', 'text']
        report_df_filtered = report_df[col_order]

        # format exec_times to be nicer
        str_exec = report_df_filtered.loc[:, 'exec_time'].map('{:,.4f}s'.format)
        report_df_filtered.loc[:, 'exec_time'] = str_exec

        # add in the extra file names and save them as separate csvs and JS vars
        extra_counter = 1
        extra_js = ''
        n1 = '    '
        n2 = n1 * 2
        n3 = n1 * 3
        n4 = n1 * 4
        for i in report_df_filtered.index:
            extra = report_df_filtered.loc[i, 'extra']
            if extra is not None:
                extra_name = 'extra_%d' % extra_counter
                extra_counter += 1
                extra_file = 'extra%d_%s.csv' % (i, self.datetime)
                out_file = os.path.join(output_dir, extra_file)
                report_df_filtered.loc[i, 'extra'] = extra_name

                # depending on what's in extra we need to proceed differently
                if isinstance(extra, list):
                    # save list as csv
                    pd.Series(extra).to_csv(out_file)
                    # save list as js var
                    list_to_str = '\\n'.join(map(str, extra))
                    extra_js += '%svar %s = "%s";\n' % (n1, extra_name,
                                                        list_to_str)
                elif isinstance(extra, set):
                    # save list as csv
                    extra = list(extra)
                    pd.Series(extra).to_csv(out_file)
                    # save list as js var
                    list_to_str = '\\n'.join(map(str, extra))
                    extra_js += '%svar %s = "%s";\n' % (n1, extra_name,
                                                        list_to_str)
                elif isinstance(extra, str):
                    # save str as csv
                    f = open(out_file, 'w')
                    f.write(extra)
                    f.close()
                    # save str as js var
                    extra_js += '%svar %s = "%s";\n' % (n1, extra_name, extra)
                elif isinstance(extra, dict):
                    # save dict as csv
                    pd.DataFrame().from_dict(extra).to_csv(out_file)
                    # save dict as js var
                    extra_js += ('%s var %s = "Please check the %s csv file in '
                                 'the output_dir that contains further info."\n'
                                 % (n1, extra_name, extra_name))
                elif isinstance(extra, pd.DataFrame):
                    # save DataFrame as csv
                    extra.to_csv(out_file)
                    # save DataFrame as js var
                    extra_js += ('%s var %s = "Please check the %s csv file in '
                                 'the output_dir that contains further info."\n'
                                 % (n1, extra_name, extra_name))
            else:
                report_df_filtered.loc[i, 'extra'] = ''
            # delete None if text is empty
            if report_df_filtered.loc[i, 'text'] is None:
                report_df_filtered.loc[i, 'text'] = ''

        # save filtered report table as csv
        out_file = 'report_%s.csv' % self.datetime
        report_df_filtered.to_csv(os.path.join(output_dir, out_file))

        # save report table for JavaScript as JSON, and add tabs for legibility
        report_df_js = report_df_filtered.to_json(None, orient='records')
        report_df_js = report_df_js.replace('","', '",\n' + n2 + '"')
        report_df_js = report_df_js.replace('},{', '},\n' + n2 + '{')
        # add prefix and suffix so Data-tables accepts it
        report_df_js = ('%svar table_data = {\n%s"data":\n%s%s\n%s};'
                        % (n1, n1, n2, report_df_js, n1))

        # collate summary string for HTML site
        summaries = self.get_summary_stats()
        qc_sum = summaries['qc_sum']
        data_file_sum = summaries['data_file_sum']
        exec_time_sum = "{0:.2f}".format(summaries['total_exec_time']/60)
        passed_sum = summaries['passed_sum']
        failed_sum = summaries['failed_sum']
        total_qc_time = "{0:.2f}".format((time() - self.ts)/60)
        summary_str_js = ("%s<li>%d QC scripts were performed on</li>\n"
                          "%s<li>%d data file(s) in </li>\n"
                          "%s<li>%s mins (total) / %s mins (qc time).</li>\n"
                          "%s<li>%d qc scripts passed,</li>\n"
                          "%s<li>%d failed.</li>\n"
                          "%s</ul>\n%s</p>\n%s</div>\n</div>\n"
                          "<script type='text/javascript'>\n"
                          % (n4, qc_sum,
                             n4, data_file_sum,
                             n4, total_qc_time, exec_time_sum,
                             n4, passed_sum,
                             n4, failed_sum,
                             n3, n2, n1))

        # generate the final HTML site from results
        out_file = 'report_%s.html' % self.datetime
        out_file = os.path.join(output_dir, out_file)
        # the os.expanduser makes this it windows safe
        html_out = open(os.path.expanduser(out_file), 'w')
        html1 = open("paqc/report/report_template1.txt")
        html2 = open("paqc/report/report_template2.txt")

        # write first part of HTML template file
        for l in html1:
            html_out.write(l)
        html1.close()

        # add in the report specific variables
        html_out.write(summary_str_js)
        html_out.write(report_df_js)
        html_out.write(extra_js)

        # write the final part of the html template
        for l in html2:
            html_out.write(l)
        html2.close()
        html_out.close()
