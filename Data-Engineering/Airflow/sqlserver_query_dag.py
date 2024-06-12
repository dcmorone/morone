import pyodbc
from airflow.hooks.base_hook import BaseHook
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def mssql_callable():
    # Fetch the connection information from Airflow's connection manager
    conn = BaseHook.get_connection('my_sqlserver_connection')
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={conn.host};'
        f'DATABASE={conn.schema};'
        f'UID={conn.login};'
        f'PWD={conn.password}'
    )
    
    # Establish the connection to SQL Server
    db_conn = pyodbc.connect(conn_str)
    cursor = db_conn.cursor()
    
    # Perform database operations
    cursor.execute("SELECT * FROM DimCustomer")
    rows = cursor.fetchall()
    
    for row in rows:
        print(row)  

dag = DAG('mssql_dag',
          default_args=default_args,
          description='A simple DAG to interact with SQL Server',
          schedule_interval=timedelta(days=1))

t1 = PythonOperator(
    task_id='run_mssql_callable',
    python_callable=mssql_callable,
    dag=dag,
)

t1

    # Close the connection
    cursor.close()
    db_conn.close()
