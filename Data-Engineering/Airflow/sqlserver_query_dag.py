from airflow import DAG
from airflow.providers.jdbc.operators.jdbc import JdbcOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'sqlserver_jdbc_query_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

query_task = JdbcOperator(
    task_id='execute_query',
    jdbc_conn_id='my_sqlserver_connection',
    sql='SELECT TOP 10 * FROM dbo.DimCustomer',  # Substitua 'employees' pelo nome da sua tabela
    dag=dag,
)

query_task
