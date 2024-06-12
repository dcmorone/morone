from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.providers.microsoft.mssql.sensors.mssql import MsSqlSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 26),
    'email': ['your@email.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'sql_server_agent_dag',
    default_args=default_args,
    description='Execute SQL Server Agent jobs with Airflow',
    schedule_interval='@daily',
)

# Define the SQL Server connection details as Airflow connection variables.

t1 = MsSqlOperator(
    task_id='step_1',
    mssql_conn_id='my_sqlserver_connection',
    sql='SELECT *FROM dbo.DimCustomer',
    dag=dag,
)

t1
