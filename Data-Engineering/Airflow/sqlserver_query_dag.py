from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pyodbc
from airflow.utils.dates import days_ago

def execute_query():
    conn = BaseHook.get_connection('my_sqlserver_connection')
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={conn.host};DATABASE={conn.schema};UID={conn.login};PWD={conn.password}"
    
    connection = pyodbc.connect(conn_str)
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM DimCustomer")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    cursor.close()
    connection.close()

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'sqlserver_query_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

query_task = PythonOperator(
    task_id='execute_query',
    python_callable=execute_query,
    dag=dag,
)
