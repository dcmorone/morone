from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import mysql.connector

def test_mysql_connection():
    try:
        # Configura a conexão com o MySQL
        connection = mysql.connector.connect(
            host='172.16.3.103',
            user='dcmorone',
            password='r2d2c3po*',
            database='world'
        )
        
        # Testar a conexão executando uma consulta simples
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        row = cursor.fetchone()
        while row:
            print(row[0])
            row = cursor.fetchone()
        
        cursor.close()
        connection.close()
        print("Conexão MySQL bem-sucedida!")
    except mysql.connector.Error as err:
        print("Erro de conexão MySQL:", err)
        raise

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'test_mysql_connection_dag',
    default_args=default_args,
    schedule_interval=None,
)

test_db_task = PythonOperator(
    task_id='test_mysql_connection',
    python_callable=test_mysql_connection,
    dag=dag,
)
