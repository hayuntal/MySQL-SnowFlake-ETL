import pandas as pd
import numpy as np
import warnings 

warnings.filterwarnings('ignore', category=FutureWarning)


def fix_dates(dataframe):
    date_pattern = r'\b\d{2}/\d{2}/\d{2}\b'
    for column in dataframe.columns:
        if dataframe[column].astype(str).str.contains(date_pattern, regex=True, na=False).any():
            dataframe[column] = pd.to_datetime(dataframe[column], format='mixed')
    return dataframe

def remove_duplicates(dataframe):
    return dataframe.drop_duplicates()

def convert_to_camel_case(s):
    if isinstance(s, str) and s.isalpha() and ' ' not in s:
        return s[0].upper() + s[1:].lower()
    elif not isinstance(s, str) or not s.isalpha():
        return s
    else:
        parts = s.split()
        return parts[0][0].upper() + parts[0][1:].lower() + ''.join(part.capitalize() for part in parts[1:])

def to_camel_case(dataframe):
    dataframe.columns = (dataframe.columns.str.lower().str.replace('_(.)', lambda x: x.group(1).upper(), regex=True))
    dataframe = dataframe.applymap(convert_to_camel_case)
    return dataframe

def is_digit_column(col):
    return col.dropna().astype(str).apply(lambda x: x.replace('.', '', 1).isdigit()).all()

def fill_missing_with_mean(dataframe):
    dataframe = dataframe.apply(lambda x: pd.to_numeric(x, errors='ignore') if x.dtype == 'object' else x)
    numeric_columns = dataframe.select_dtypes(include=np.number).columns.tolist()

    for column in numeric_columns:
        dataframe[column] = dataframe[column].fillna(dataframe[column].mean())
    return dataframe

def get_df_info(df):
    mapping = {"object":"varchar(255)", "int64":"INT", "float64":"FLOAT", "bool": 'BOOLEAN',
     "datetime64": "DATETIME", "datetime64[ns]": "DATETIME"}
    col_names = df.columns.tolist()
    col_datatypes = df.dtypes.tolist()
    sql_types = []

    for col_name, pd_type in zip(col_names, col_datatypes):
        sql_types.append(f"{col_name} {mapping[str(pd_type)]}")
    sql_types = ", ".join(sql_types)
    return sql_types

def df_to_string(df):
    data_tuples = [tuple(str(value) if isinstance(value, pd.Timestamp) else value for value in row) for row in df.values]
    string_data = ',\n'.join(str(row) for row in data_tuples)
    return string_data