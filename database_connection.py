from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
import snowflake.connector
import os
from dotenv import load_dotenv
load_dotenv()


def likewise_connection(schema=os.environ.get('SNOWFLAKE_SCHEMA')):
    conn = snowflake.connector.connect(
        user=os.environ.get('SNOWFLAKE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
        database=os.environ.get('SNOWFLAKE_DATABASE'),
        schema=schema
    )
    return conn


def likewise_engine(schema=os.environ.get('SNOWFLAKE_SCHEMA')):
    registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
    engine = create_engine(
        'snowflake://{user}:{password}@{account}/{db}/{schema}?warehouse={warehouse}'.format(
            user=os.environ.get('SNOWFLAKE_USER'),
            password=os.environ.get('SNOWFLAKE_PASSWORD'),
            account=os.environ.get('SNOWFLAKE_ACCOUNT'),
            db=os.environ.get('SNOWFLAKE_DATABASE'),
            schema=schema,
            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE')
        )
    )

    return engine


def upload_to_snowflake(dataframe, engine, table_name, truncate=False, create=False):

    file_name = f"{table_name}.csv"
    file_path = os.path.abspath(file_name)
    dataframe.to_csv(file_path, index=False, header=False)

    with engine.connect() as con:

        if create:
            dataframe.head(0).to_sql(name=table_name, con=con, if_exists="replace", index=False)

        if truncate:
            con.execute(f"truncate table {table_name}")

        con.execute(f"put file://{file_path}* @%{table_name}")
        con.execute(f"copy into {table_name}")
