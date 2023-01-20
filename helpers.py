import http.client
import snowflake_query_utils as snow
import pandas as pd
import numpy as np
import queries as q
from urllib.parse import urlparse, parse_qs
from database_connection import likewise_connection, likewise_engine, upload_to_snowflake

PUBLIC_CONN = likewise_connection(schema='PUBLIC')
ENG = likewise_engine(schema='STAGING')


KEEP_KEYS = [
    'utm_medium',
    'utm_content',
    'utm_campaign',
    'sectionNumber',
    'sectionName',
    'pid',
    'c',
    'af_dp',
    'af_adset',
    'domain',
    'parsed_onelink'
]


def get_onelinks():
    print('Querying new onelinks...')
    v = snow.query_return_dataframe(PUBLIC_CONN, q.EMAIL_CLICKS_QUERY)
    if len(v) == 0:
        return []
    else:
        v = snow.dataframe_assign_columns(v, q.EMAIL_CLICKS_COLS)
        return list(v['url'])


def parse_unique_onelinks(entry):
    if entry in ['email-preference', 'emailinboundprocessing']:
        key = entry
        return {'domain': 'N/A'}
    else:
        return extract_info_from_onelinks(entry)


def extract_info_from_onelinks(onelink):
    parsed = parse_onelink(onelink)
    response = get_parsed_onelink_reponse(parsed)
    location = get_response_header_location(response)

    if location in ['https://likewisetv.com/login/assistance', 'http://likewise.com']:
        query_param_dikt = parse_qs(parsed.query)
    else:
        query_param_dikt = parse_qs(location)

    domain = urlparse(location).netloc
    query_param_dikt['domain'] = [domain]
    query_param_dikt['parsed_onelink'] = [location]
    query_param_dikt = remove_unwanted_keys(query_param_dikt)
    query_param_dikt = {a: first_list_entry(b) for (a, b) in query_param_dikt.items()}
    return query_param_dikt


def parse_onelink(onelink):
    return urlparse(onelink)


def get_parsed_onelink_reponse(parsed_onelink):
    h = http.client.HTTPConnection(parsed_onelink.netloc)
    h.request('HEAD', parsed_onelink.path)
    return h.getresponse()


def get_response_header_location(http_request_response):
    return http_request_response.getheader('Location')


def remove_unwanted_keys(dikt, keep=KEEP_KEYS):
    return {k:v for (k,v) in dikt.items() if k in keep}


def first_list_entry(lst):
    if isinstance(lst, list):
        return lst[0]
    else:
        return lst


def convert_to_dataframe(query_param_dict):
    print("Converting query params to dataframe...")
    df = pd.DataFrame.from_dict(query_param_dict, orient='index')
    df = df.reset_index().rename(columns={'index': 'url'}).copy()
    return df


def get_current_onelinks_table():
    print("Querying known onelinks...")
    tbl = snow.query_return_dataframe(PUBLIC_CONN, q.ONELINKS_QUERY)
    tbl = snow.dataframe_assign_columns(tbl, q.ONELINKS_QUERY_COLS)
    return tbl


def ensure_column_consistency(df):
    print("Ensuring column consistency...")
    have_these_columns = ['url', 'utm_medium', 'utm_campaign', 'utm_content', 'sectionNumber',
                          'sectionName', 'domain', 'parsed_onelink', 'af_dp', 'af_adset', 'pid', 'c']

    for c in have_these_columns:
        if c not in df.columns:
            df[c] = np.nan

    return df


def concat_onelink_tables(existing_onelink_table, new_data):
    print("Appending new data to existing...")
    col_dict = {'url': 'URL',
                'c': 'C',
                'af_dp': 'AF_DP',
                'af_adset': 'AF_ADSET',
                'pid': 'PID',
                'domain': 'DOMAIN',
                'parsed_onelink': 'PARSED_ONELINK',
                'utm_content': 'UTM_CONTENT',
                'utm_medium': 'UTM_MEDIUM',
                'utm_campaign': 'UTM_CAMPAIGN'}

    df1 = new_data.rename(columns=col_dict).copy()
    df1 = df1[list(existing_onelink_table.columns)].copy()
    df_concat = pd.concat([existing_onelink_table, df1])
    df_concat = df_concat.drop_duplicates().reset_index(drop=True)
    return df_concat


def catch_stray_commas(df):
    print("Removing stray commas...")
    df = df.apply(lambda x: x.str.replace(',','.')).copy()
    return df


def push_to_snowflake(df):
    print('Pushing data to snowflake...')
    table_name = 'NEWSLETTER_ONELINKS_QUERY_PARAMS'.lower()
    try:
        upload_to_snowflake(df, ENG, table_name, create=True)
        return True
    except Exception as ex:
        # Log this
        print(f"Error {ex}")
        return False
