from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.utils.dates import days_ago
import pyodbc

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'sqlserver_example',
    default_args=default_args,
    description='Exemplo de DAG para conectar ao SQL Server',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
)

def query_sql_server():
    hook = OdbcHook(odbc_conn_id='my_sqlserver_conn')
    sql = "SELECT * FROM my_table"
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
        print(row)

run_query = PythonOperator(
    task_id='query_sql_server',
    python_callable=query_sql_server,
    dag=dag,
)

run_query
