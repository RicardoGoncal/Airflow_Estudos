from airflow import DAG
from big_data_operator import BigDataOperator
from datetime import datetime

dag = DAG(
    "dagplugin", 
    description="Dag para exemplo de uso de plugin",
    schedule_interval=None,
    start_date=datetime(2023,7,6),
    catchup=False
)

# Criacao de Task com o novo plugin
bigdata = BigDataOperator(
    task_id="tsk2", 
    path_to_csv_file = "/opt/airflow/data/Churn.csv",
    path_to_save_file = "/opt/airflow/data/Churn.json",
    file_type = "json",
    dag = dag
    
)


# Modelo de como executar DAG