from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

dag = DAG(
    "dagpool", 
    description="DAG de exemplo de usar Pools",
    schedule_interval=None,
    start_date=datetime(2023,7,5),
    catchup=False
)


# Dag para usar Pool
task1 = BashOperator(task_id="tsk1", bash_command="sleep 5", dag=dag, pool='meupool')
task2 = BashOperator(task_id="tsk2", bash_command="sleep 5", dag=dag, pool='meupool', priority_weight=5)
task3 = BashOperator(task_id="tsk3", bash_command="sleep 5", dag=dag, pool='meupool')
task4 = BashOperator(task_id="tsk4", bash_command="sleep 5", dag=dag, pool='meupool', priority_weight=10)


# Modelo de como executar DAG

