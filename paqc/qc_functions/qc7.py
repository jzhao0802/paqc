import pandas as pd

def qc7(df, ):
    """

    :param df:
    :return:
    """

    output = (df.dropna(axis=0, how='all').shape[1] == df.shape[1])

    return output