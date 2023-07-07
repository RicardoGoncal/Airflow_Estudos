from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

dag = DAG(
    "trigger1", 
    description="DAG de exemplo para trigger",
    schedule_interval=None,
    start_date=datetime(2023,6,28),
    catchup=False
)

# Trigger a nivel de task
task1 = BashOperator(task_id="tsk1", bash_command="sleep 5", dag=dag)
task2 = BashOperator(task_id="tsk2", bash_command="sleep 5", dag=dag)
task3 = BashOperator(task_id="tsk3", bash_command="sleep 5", dag=dag, trigger_rule='one_failed')


# Modelo de como executar DAG, no caso esse em precedencia
[task1,task2] >> task3