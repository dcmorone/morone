from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import sqlalchemy

def test_db_connection():
    try:
        # Substitua pelo seu URI de conexão ao banco de dados
        engine = sqlalchemy.create_engine('jdbc:sqlserver://172.16.3.102:1433;database=AdventureWorksDW2019;')
        with engine.connect() as connection:
            result = connection.execute("SELECT 1")
            print("Conexão bem-sucedida:", result.fetchone())
    except Exception as e:
        print("Erro de conexão:", e)
        raise

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'test_db_connection_dag',
    default_args=default_args,
    schedule_interval=None,
)

test_db_task = PythonOperator(
    task_id='test_db_connection',
    python_callable=test_db_connection,
    dag=dag,
)
