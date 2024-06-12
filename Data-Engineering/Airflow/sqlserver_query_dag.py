from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def initialize_etl():
    import pymssql
    conn = pymssql.connect(server="servername",
                                user="sa", 
                                password="r2d2c3po*", 
                                database="AdventureWorksDW2019",
                                port=1433)
    cursor = conn.cursor()
    cursor.execute ("SELECT @@VERSION")
    row = cursor.fetchone()
    print(f"\n\nSERVER VERSION:\n\n{row[0]}")
    cursor.close()
    conn.close()
   
with DAG(
        dag_id="my_sqlserver_connection",
        start_date=datetime(2021, 7, 1),
        schedule_interval=None,
        default_args=default_args,
        catchup=False
) as dag:

    mssql_select = MsSqlOperator(
         task_id='mssql_select',
         mssql_conn_id='mssql_conn',
         sql=f"SELECT CAST( GETDATE() AS Date );",            
         autocommit=False,
         database='AdventureWorksDW2019'
     )

    initialize = PythonOperator(
        task_id='initialize_etl_mssql',
        python_callable=initialize_etl
    )
    initialize >> mssql_select
