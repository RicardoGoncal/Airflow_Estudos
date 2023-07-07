from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime

dag = DAG(
    "dagvariavel", 
    description="DAG de exemplo de usar variaveis",
    schedule_interval=None,
    start_date=datetime(2023,7,5),
    catchup=False
)

def print_var(**context):
    minha_var = Variable.get('minhavar')
    print(f'O valor da variavel e: {minha_var}')


# Dag para usar variavel
task1 = PythonOperator(task_id="tsk1",python_callable=print_var, dag=dag)


# Modelo de como executar DAG
task1 
