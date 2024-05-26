from sqlalchemy import create_engine, URL, text
import pandas as pd

# SQL Server 세팅
sql_server = 'DESKTOP-20E03R8\SQLEXPRESS'
sql_database = 'AdventureWorksDW2022'
sql_username = 'reader'
sql_password = 'reader'
sql_driver = '{ODBC Driver 17 for SQL Server}'

# PostgreSQL 세팅
pos_database = 'AdventureWorks'
pos_username = 'etl'
pos_password = 'etl'

# SQL Server 연결
sql_connection_string = f'DRIVER={sql_driver};SERVER={sql_server};DATABASE={sql_database};UID={sql_username};PWD={sql_password}'
sql_connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": sql_connection_string})
sql_engine = create_engine(sql_connection_url)

# Postgre Server 연결
pos_connection_string = f'postgresql://{pos_username}:{pos_password}@localhost:5432/{pos_database}'
pos_engine = create_engine(pos_connection_string)

try:
    with sql_engine.connect() as sql_conn:
        # SQL Server에서 테이블 리스트 얻기
        query_table_list = """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        """
        tables = sql_conn.execute(text(query_table_list)).fetchall()
        
        # SQL Server에서 테이블을 읽어와 PostgreSQL에 쓰기
        for table_info in tables:
            schema = table_info.TABLE_SCHEMA
            table = table_info.TABLE_NAME
            print(f'processing table {table}')

            query_table = f"SELECT * FROM {schema}.{table}"
            chunk_size = 10000
            for chunk in pd.read_sql_query(query_table, sql_engine, chunksize=chunk_size):
                chunk.to_sql(table, pos_engine, schema=schema, if_exists='append', index=False)

except Exception as e:
    print(e)
finally:
    sql_engine.dispose()
    pos_engine.dispose()