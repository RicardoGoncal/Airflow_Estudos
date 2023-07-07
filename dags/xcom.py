from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

dag = DAG(
    "dagxcom", 
    description="DAG de exemplo de Xcom",
    schedule_interval=None,
    start_date=datetime(2023,7,3),
    catchup=False
)


def task_write(**kwargs):
    kwargs['ti'].xcom_push(key='valorxcom1', value=10200)


# Dag para Xcom
task1 = PythonOperator(task_id="tsk1",python_callable=task_write, dag=dag)


def task_read(**kwargs):
    valor = kwargs['ti'].xcom_pull(key='valorxcom1')
    print(f'valor recuperado: {valor}')


task2 = PythonOperator(task_id="tsk2",python_callable=task_read, dag=dag)

# Modelo de como executar DAG, no caso esse em precedencia
task1 >> task2
