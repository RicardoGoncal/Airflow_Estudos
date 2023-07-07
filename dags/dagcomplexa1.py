from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

dag = DAG(
    "dagcomplexa", 
    description="DAG de exemplo complexa",
    schedule_interval=None,
    start_date=datetime(2023,6,28),
    catchup=False
)

# Dag em forma mais complexa
task1 = BashOperator(task_id="tsk1", bash_command="sleep 5", dag=dag)
task2 = BashOperator(task_id="tsk2", bash_command="sleep 5", dag=dag)
task3 = BashOperator(task_id="tsk3", bash_command="sleep 5", dag=dag)
task4 = BashOperator(task_id="tsk4", bash_command="sleep 5", dag=dag)
task5 = BashOperator(task_id="tsk5", bash_command="sleep 5", dag=dag)
task6 = BashOperator(task_id="tsk6", bash_command="sleep 5", dag=dag)
task7 = BashOperator(task_id="tsk7", bash_command="sleep 5", dag=dag)
task8 = BashOperator(task_id="tsk8", bash_command="sleep 5", dag=dag)
task9 = BashOperator(task_id="tsk9", bash_command="sleep 5", dag=dag, trigger_rule='one_failed')


# Modelo de como executar DAG, no caso esse em precedencia
task1 >> task2
task3 >> task4
[task2,task4] >> task5 >> task6
task6 >> [task7, task8, task9]