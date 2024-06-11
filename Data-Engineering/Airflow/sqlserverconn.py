from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.utils.dates import days_ago

def query_sql_server():
    jdbc_hook = JdbcHook(jdbc_conn_id='my_sql_server')
    sql = "
    USE AdventureWorksDW2019;
    GO
    SELECT *FROM dbo.DimCustomer
    GO    
    "
    results = jdbc_hook.get_records(sql)
    for result in results:
        print(result)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'sql_server_jdbc_dag',
    default_args=default_args,
    description='DAG para conexão a um SQL Server via JDBC',
    schedule_interval=None,
) as dag:

    run_sql_query = PythonOperator(
        task_id='run_sql_query',
        python_callable=query_sql_server,
    )

    run_sql_query
