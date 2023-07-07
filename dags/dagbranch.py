from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
import random
from datetime import datetime

dag = DAG(
    "dagbranch", 
    description="DAG de exemplo de usar Branchs",
    schedule_interval=None,
    start_date=datetime(2023,7,5),
    catchup=False
)

# Função para gerar numero aleatorio
def gerar_random():
    return random.randint(1,100)


# Chama a função para gerar numero aleatorio
gerar_n_task = PythonOperator(task_id='gerarnum', python_callable=gerar_random, dag=dag)


# Função para verificar Par ou Impar
def par_impar(**context):
    num = context['task_instance'].xcom_pull(task_ids='gerarnum')
    if num % 2 == 0:
        return 'par_task'
    else:
        return 'impar_task'

# Criar a branch para analise de logica
branch_task = BranchPythonOperator(task_id='branch_task', python_callable=par_impar, provide_context=True, dag=dag)


# Dag para usar Branch
par_task = BashOperator(task_id="par_task", bash_command='echo "Numero Par"', dag=dag)
impar_task = BashOperator(task_id="impar_task", bash_command='echo "Numero Impar"', dag=dag)


# Modelo de como executar DAG
gerar_n_task >> branch_task
branch_task >> par_task
branch_task >> impar_task
