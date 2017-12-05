import pandas as pd
import re

from paqc.report import report as rp
from paqc.utils import utils


def qc14(df, dict_config):
    """
    Checks for missing patient IDs.

    :param df:
    :param dict_config:
    :return: ReportItem:
                - self.extra=ls_idx_missing_id, the list of indices of rows
                that miss a patient ID.
    """

    patient_id_col = dict_config['general']['patient_id_col']
    ls_idx_missing_id = df[df[patient_id_col].isnull()].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_missing_id, dict_config['qc'])


def qc15(df, dict_config, keys_num_cols=('count_cols', 'freq_cols',
                                         'flag_cols', 'age_col')):
    """
    Tests if all columns expected to be numeric are actually numeric (
    according to the pd.read_csv function of pandas).

    :param df:
    :param dict_config:
    :param keys_num_cols: All the keys in the dict_config that point to
           column names/suffixes of columns that are supposed to be numeric.
    :return: ReportItem:
                - self.extra=ls_cols_not_numeric, the names of columns that
                should be numeric, but aren't.
    """
    ls_colnames = utils.generate_list_columns(df, dict_config, keys_num_cols)
    ls_cols_not_numeric = [colname for colname in ls_colnames if
                           not pd.api.types.is_numeric_dtype(df[colname])]

    return rp.ReportItem.init_conditional(ls_cols_not_numeric, dict_config['qc'])


# TODO: What is a good default value for max_fraction_diff?
def qc16(df, dict_config, max_fraction_diff=0.1):
    """
    Finds the columns for which the fraction of values that is missing or 0
    differs less over the two classes than the provided threshold, provided
    as max_fraction_diff.

    :param df:
    :param dict_config:
    :param max_fraction_diff: the parameter that decides how different the
           fraction of missing/zero values for the two different classes is
           allowed to be.
    :return: ReportItem:
                - self.extra=ls_cols_high_dif, the list of names of all
                columns that pass the previously explained threshold.
    """

    colname_target = dict_config['general']['target_col']

    # Some cohorts do not have a target column, let the qc go as passed=False
    try:
        df_grouped = df.groupby(colname_target)
    except KeyError:
        return rp.ReportItem(passed=False,
                             text='No matching target_col in the dataset',
                             **dict_config['qc'])

    df_fract = df_grouped.agg(utils.fraction_zeroes_or_null)
    ss_dif_high = abs(df_fract.iloc[0] - df_fract.iloc[1]) > max_fraction_diff
    ls_cols_high_dif = ss_dif_high[ss_dif_high].index.tolist()

    return rp.ReportItem.init_conditional(ls_cols_high_dif, dict_config['qc'])


def qc17(df, dict_config):
    """
    Checks that the gender column only contains "F" and "M".

    :param df:
    :param dict_config:
    :return: ReportItem:
                - self.extra=ls_idx_faulty: list of indices of rows of
                patients that have a gender value that is not M or F.
    """
    gender_col = dict_config['general']['gender_col']
    ls_idx_faulty = df[~df[gender_col].isin(['M', 'F'])].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_faulty, dict_config['qc'])


def qc18(df, dict_config):
    """
    Patient age should be between (including) 0 and 85.

    :param df:
    :param dict_config:
    :return: ReportItem:
                - self.extra=ls_idx_faulty: list of indices of rows of
                patients that have an age value not between 0 and 85.
    """
    age_col = dict_config['general']['age_col']
    ls_idx_faulty = df[~df[age_col].between(0, 85)].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_faulty, dict_config['qc'])


def qc19(df, dict_config, date_limit='2009-01-01 05:00:00'):
    """
    Checks that every row has a valid INDEX_DATE which is after a chosen date.

    :param df:
    :param dict_config:
    :param date_limit: All index dates have to be strictly after this date.
    :return: ReportItem:
                - self.extra=ls_idx_faulty: list of indices of rows of
                patients that have an has an index not after the chosen date
                limit.
    """
    # Typical problem would be that provided date_limit was not in the right
    # format compared to dict_config['general']['date_format']
    try:
        date_limit = pd.to_datetime(date_limit,
                                    format=dict_config['general']['date_format'])
    except ValueError as e:
        return rp.ReportItem(passed=False, text=str(e), **dict_config['qc'])

    index_date_col = dict_config['general']['index_date_col']
    ls_idx_faulty = df[~(df[index_date_col] > date_limit)].index.tolist()

    return rp.ReportItem.init_conditional(ls_idx_faulty, dict_config['qc'])


def qc20(df, dict_config, lookback_days_col='lookback_dys',
         days_months_years='years'):
    """
    Frequency variables are consistently calculated by dividing the COUNT by
    the lookback length in the same unit, default in years.

    This is done by recalculating the FREQ column for 10 000 randomly
    sampled cells.

    :param df:
    :param dict_config:
    :param lookback_days_col: Column name of the lookback length in days column
    :param days_months_years: The unit freq should be calculated in,
           'years' for counts/years, 'months' for counts/months and 'days' for
           counts/'days'
    :return: ReportItem:
                - self.extra=ls_feat_faulty: list of all features that were
                found to violate the expected way of calculating frequency
                columns.
    """
    try:
        cte = {'days': 1, 'months': 12, 'years': 365}[days_months_years.lower()]
    except KeyError as e:
        return rp.ReportItem(passed=False,
                             text='days_month_year value %s not part of '
                                  '[days, months, years]' % e,
                             **dict_config['qc'])

    dict_grouped_cols = utils.generate_dict_grouped_columns(df, dict_config,
                                                            ['freq_cols',
                                                             'count_cols'])
    # Only run the test on predictors that have all those two columns
    dict_grouped_cols = {predictor: dict_grouped for predictor, dict_grouped
                         in dict_grouped_cols.items() if (len(dict_grouped) == 2)}

    n_samples = 10000/len(dict_grouped_cols)
    ls_feat_faulty = []
    for feat, dict_feat in dict_grouped_cols.items():
        col_count, col_freq = dict_feat['count'], dict_feat['freq']
        df_sample = df[[lookback_days_col, col_count, col_freq]].sample(
            n=int(n_samples), replace=True)
        is_not_same = (abs(df_sample[col_count]/df_sample[lookback_days_col]*cte -
                       df_sample[col_freq]) > df_sample[col_freq]*0.1).any()
        if is_not_same:
            ls_feat_faulty.append(feat)

    return rp.ReportItem.init_conditional(ls_feat_faulty, dict_config['qc'])


def qc21(df, dict_config, lookback_days_col='lookback_dys', ls_dd_columns=(
        '_DATE_FIRST_LAST', '_DATE_FIRST_INDEX', '_DATE_LAST_INDEX',
        '_DATE_FIRST_DISEASE', '_DATE_LAST_DISEASE')):
    """
    The value of the date difference variables are always 0 or positive, and
    never exceed the lookback length for that patient. 

    :param df:
    :param dict_config:
    :param lookback_days_col: Column name of the lookback length in days
           column.
    :param ls_dd_columns: List of all column names/suffixes of columns that
           are date difference variables
    :return: ReportItem:
                - self.extra=ls_cols_faulty: list of all columns that have
                negative or higher than lookback length date difference values.
    """

    ls_regex = ["%s$" % col for col in ls_dd_columns]
    prog = re.compile("(" + ")|(".join(ls_regex) + ")")
    ls_colnames = [colname for colname in df if prog.search(colname)]
    ss_bool = df[ls_colnames].apply(lambda x:  (x < 0) | (x > df[
        lookback_days_col])).any()
    ls_cols_faulty = ss_bool[ss_bool].index.tolist()

    return rp.ReportItem.init_conditional(ls_cols_faulty, dict_config['qc'])
