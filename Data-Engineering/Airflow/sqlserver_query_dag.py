from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.utils.dates import days_ago

# Defina os argumentos padrão do DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Ajuste conforme necessário
    'retries': 1,
}

# Crie o DAG
dag = DAG(
    'sqlserver_query_dag',
    default_args=default_args,
    description='Um exemplo de DAG para interagir com SQL Server',
    schedule_interval=None,  # Ajuste conforme necessário
)

# Defina a tarefa usando MsSqlOperator
run_sql_query = MsSqlOperator(
    task_id='run_sql_query',
    mssql_conn_id='my_sqlserver_connection',  # Use o id da conexão configurada
    sql='SELECT * FROM dbo.DimCustomer;',  # Sua consulta SQL
    dag=dag,
)
print(sql)
