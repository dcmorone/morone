from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pyodbc

def test_odbc_connection():
    try:
        # Configure a string de conexão ODBC
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=172.16.3.102;"
            "DATABASE=AdventureWorksDW2019;"
            "UID=sa;"
            "PWD=r2d2c3po*"
        )
        conn = pyodbc.connect(conn_str)
        
        # Testar a conexão executando uma consulta simples
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        row = cursor.fetchone()
        while row:
            print(row[0])
            row = cursor.fetchone()
        
        cursor.close()
        conn.close()
        print("Conexão ODBC bem-sucedida!")
    except Exception as e:
        print("Erro de conexão ODBC:", e)
        raise

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'test_odbc_connection_dag',
    default_args=default_args,
    schedule_interval=None,
)

test_db_task = PythonOperator(
    task_id='test_odbc_connection',
    python_callable=test_odbc_connection,
    dag=dag,
)
