
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import sqlalchemy


def get_tables():
    mssql_hook = MsSqlHook("sqlserver")
    sql = """SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'"""
    table_names = mssql_hook.get_pandas_df(sql)

    return table_names

def extract_table(table: str):
    mssql_hook = MsSqlHook("sqlserver")
    sql = f"SELECT * FROM {table}"
    table = mssql_hook.get_pandas_df(sql)

    return table

def load_table(table_name, table):
    conn = BaseHook.get_connection("postgreserver")
    conn_uri = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = sqlalchemy.create_engine(conn_uri)
    table.to_sql(name=table_name, con=engine, if_exists="replace", index=False, schema="dbo")
    
def main():
    table_names = get_tables()
    table_list = table_names['TABLE_NAME'].tolist()
    for table_name in table_list:
        table = extract_table(table_name)
        load_table(table_name, table)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1
}

with DAG("mssql_to_postgres",
         default_args=default_args,
         schedule_interval=None) as dag:
    
    mssql_to_postgres = PythonOperator(
        task_id="mssql_to_postgres",
        python_callable=main
    )