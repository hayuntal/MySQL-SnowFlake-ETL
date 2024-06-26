from snowflake import connector
from src.utils import *

class SnowFlakeClient:
    def __init__(self, user, password, account, warehouse, database, schema):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.conn = None

    def connect(self):
        conn_params = {
            'user': self.user,
            'password': self.password,
            'account': self.account,
            'warehouse': self.warehouse,
            'database': self.database,
            'schema': self.schema
        }

        self.conn = connector.connect(**conn_params)
        print('Success connection to SnowFlake')

    def create_dim_tables(self, dict_dataframes):
        cur = self.conn.cursor()
        for table_name, df in dict_dataframes.items():
            
            columns_datatypes = get_df_info(df) #  {col_name: sql datatype}
            if table_name == "patients":
                query = f"""
                        CREATE TABLE IF NOT EXISTS {self.database}.{self.schema}.{table_name} ({columns_datatypes});  
                        """
            else:
                columns_datatypes += f', {table_name}ID INT NOT NULL, PRIMARY KEY ({table_name}ID)'
                query = f"""
                        CREATE TABLE IF NOT EXISTS {self.database}.{self.schema}.{table_name} ({columns_datatypes});  
                        """
            cur.execute(query)
        print('Created dimention tables')
        cur.close()
        self.conn.commit()

    def insert_data(self, dict_dataframes):
        cur = self.conn.cursor()
        for table_name, df in dict_dataframes.items():
            col_names = [str(i).upper() for i in df.columns.tolist()]

            if table_name == "patients":
                df_data = df_to_string(df)
                col_names = (', '.join(col_names))
                query = f"""INSERT INTO {table_name} ({col_names})\n VALUES {df_data}""" 
            else:   
                id_column = f'{table_name}ID'
                id_values =  list(range(df.shape[0]))                
                df[id_column] = id_values
                df_data = df_to_string(df)
                col_names.append(id_column)
                col_names = (', '.join(col_names))
                query = f"""INSERT INTO {table_name} ({col_names})\n VALUES {df_data}"""

            cur.execute(query)

        print('Inserted values into dimention tables')
        cur.close()
        self.conn.commit()  
    
    def create_fact_table(self):

        cur = self.conn.cursor()
        query = f'CREATE TABLE IF NOT EXISTS {self.database}.{self.schema}.FACTTABLE \
         (PATIENTID INT NOT NULL, VISITINFOID INT NOT NULL, TESTID INT NOT NULL);'
        cur.execute(query)

        print('Created fact table')

        cur.close()
        self.conn.commit()  
    
    def insert_fact_values(self):
        cur = self.conn.cursor()
        values_query = f'SELECT  t.patientid, v.visitinfoid, t.testsid \
                  FROM {self.database}.{self.schema}.visitinfo v JOIN {self.database}.{self.schema}.tests t \
                  ON v.id = t.patientid AND v.date = t.date;'
        result = cur.execute(values_query).fetchall()
        values = ', '.join(str(t) for t in result)

        insert_fact_values = f'INSERT INTO FACTTABLE (PATIENTID, VISITINFOID, TESTID) VALUES {values};'
        cur.execute(insert_fact_values)

        print('Inserted values into fact table')

        cur.close()
        self.conn.commit()  
        self.conn.close()