import pandas as pd


def query_return_dataframe(connection, query):
    cur = connection.cursor()
    cur.execute(query)
    dat = cur.fetchall()
    return pd.DataFrame(dat)


def dataframe_assign_columns(df, list_of_col_names):
    df1 = df.copy()
    df1.columns = list_of_col_names
    return df1