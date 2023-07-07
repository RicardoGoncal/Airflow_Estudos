from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    "dummy", 
    description="DAG de exemplo Dummy",
    schedule_interval=None,
    start_date=datetime(2023,7,5),
    catchup=False
)

# Dag em forma mais complexa
task1 = BashOperator(task_id="tsk1", bash_command="sleep 5", dag=dag)
task2 = BashOperator(task_id="tsk2", bash_command="sleep 5", dag=dag)
task3 = BashOperator(task_id="tsk3", bash_command="sleep 5", dag=dag)
task4 = BashOperator(task_id="tsk4", bash_command="sleep 5", dag=dag)
task5 = BashOperator(task_id="tsk5", bash_command="sleep 5", dag=dag)
taskdummy = DummyOperator(task_id="taskdummy", dag=dag)

# Modelo de como executar DAG Dummy
[task1,task2,task3] >> taskdummy >> [task4,task5] 