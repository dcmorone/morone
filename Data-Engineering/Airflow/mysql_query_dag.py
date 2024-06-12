from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.dates import days_ago

def execute_query():
    mysql_hook = MySqlHook(mysql_conn_id='my_mysql_connection')
    sql = "SELECT * FROM world.city LIMIT 10"
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
        print(row)
    cursor.close()
    connection.close()

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'mysql_query_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

query_task = PythonOperator(
    task_id='execute_query',
    python_callable=execute_query,
    dag=dag,
)
