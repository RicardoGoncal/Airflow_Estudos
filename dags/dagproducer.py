from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from datetime import datetime

dag = DAG(
    "dagproducer", 
    description="DAG de exemplo de Producer",
    schedule_interval=None,
    start_date=datetime(2023,7,5),
    catchup=False
)

# Criar dataset no airflow - arquivo que esta sendo monitorado
my_dataset = Dataset("/opt/airflow/data/Churn_new.csv")



def my_file():
    dataset = pd.read_csv("/opt/airflow/data/Churn.csv", sep=';')
    dataset.to_csv("/opt/airflow/data/Churn_new.csv", sep=';', index=False)


# Taks para usar Producer
task1 = PythonOperator(task_id="task1", python_callable=my_file, dag=dag, outlets=[my_dataset]) # Outlet avisa que a task vai atualizar o Dataset


# Modelo de como executar DAG
task1