from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.operators.dagrun_operator import TriggerDagRunOperator

dag = DAG(
    "dagrun1", 
    description="DAG de exemplo que vai chamar outra DAG",
    schedule_interval=None,
    start_date=datetime(2023,6,28),
    catchup=False
)


# Dag em forma mais complexa
task1 = BashOperator(task_id="tsk1", bash_command="sleep 5", dag=dag)
task2 = TriggerDagRunOperator(task_id="tsk2", trigger_dag_id="dagrun2", dag=dag)


# Modelo de como executar DAG, no caso esse em precedencia
task1 >> task2
