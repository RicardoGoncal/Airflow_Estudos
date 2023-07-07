from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests
from datetime import datetime

dag = DAG(
    "dagpostgres", 
    description="DAG de exemplo de Postgres Connection",
    schedule_interval=None,
    start_date=datetime(2023,7,5),
    catchup=False
)

def print_result(**context):
    task_instance = context['ti'].xcom_pull(task_ids='query_table')
    print('Resultado da Consulta: ')
    for row in task_instance:
        print(row)


# Criar tabela Postgres
create_table = PostgresOperator(task_id='create_table', postgres_conn_id='postgres', sql='create table if not exists teste(id int);', dag=dag)

# Insert tabela Postgres
insert_table = PostgresOperator(task_id='insert_table', postgres_conn_id='postgres', sql='insert into teste values(1);', dag=dag)

# query tabela Postgres
query_table = PostgresOperator(task_id='query_table', postgres_conn_id='postgres', sql='select * from teste;', dag=dag)


# Chamar xcom para mostar resultado da query Postgres
print_result_task = PythonOperator(task_id='print', python_callable=print_result, provide_context=True, dag=dag)


# Montar diagrama
create_table >> insert_table >> query_table >> print_result_task