from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

with DAG("quartadag",description="DAG de exemplo para uso de outra forma PY:with",
        schedule_interval=None,
        start_date=datetime(2023,6,27),
        catchup=False) as dag:

    task1 = BashOperator(task_id="tsk1", bash_command="sleep 5")
    task2 = BashOperator(task_id="tsk2", bash_command="sleep 5")
    task3 = BashOperator(task_id="tsk3", bash_command="sleep 5")


    # Modelo de como executar DAG com metodos airflow
    task1.set_upstream(task2)
    task2.set_upstream(task3)