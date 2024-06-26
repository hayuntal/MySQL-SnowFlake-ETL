import pandas as pd
import numpy as np
import os
import logging

from utils import *
from config.config import *
from MySqlClient import *
from SnowFlakeClient import *


def extract():
    """
    Extracts data from all tables in a given SQL database and located in a directory.
    """
    host = os.getenv('SQL_CLIENT_HOST')
    username = os.getenv('SQL_CLIENT_USER')
    password = os.getenv('SQL_CLIENT_PASSWORD')
    database = os.getenv('SQL_CLIENT_DATABASE')

    client = MySqlClient(host, username, password, database)
    client.connect()

    tables = client.fetch_table_names()
    dict_stg_df = {}
    for table in tables:
        data = client.fetch_data(table)
        columns = client.fetch_column_names(table)

        df = pd.DataFrame(data, columns=columns)
        dict_stg_df[table] = df

    logging.info("Extraction completed.")
    return dict_stg_df

def transform(stg_dfs):
    """
    Performs a series of transformations on CSV files and located in a directory
    """
    dict_dataframes = {}
    
    for table_name, data in stg_dfs.items():
        processed_df = data.pipe(fix_dates) \
            .pipe(remove_duplicates) \
            .pipe(to_camel_case) \
            .pipe(fill_missing_with_mean)

        dict_dataframes[table_name] = processed_df

    logging.info("Transformation completed.")
    return dict_dataframes

def load(dict_dataframes):
    client_sf = SnowFlakeClient(SF_USER, SF_PASSWORD, SF_ACCOUNT, SF_WAREHOUSE, SF_DB, SF_SCHEMA)
    client_sf.connect()

    client_sf.create_dim_tables(dict_dataframes)
    client_sf.insert_data(dict_dataframes)
    client_sf.create_fact_table()
    client_sf.insert_fact_values()
    logging.info("Loading completed.")

def run_etl():

    # Extract - extract the database tables from RDBMS (MY SQL) and get dictinary of them
    stg_dfs = extract()

    # Transform - Clean, aggregate, or manipulate the data as needed
    dict_dfs = transform(stg_dfs)

    # Load - Load the database to SnowFlake db
    load(dict_dfs)

if __name__ == '__main__':
    run_etl()