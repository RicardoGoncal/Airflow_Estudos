from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from datetime import datetime

# Criar dataset no airflow - arquivo que esta sendo monitorado
my_dataset = Dataset("/opt/airflow/data/Churn_new.csv")

dag = DAG(
    "dagconsumer", 
    description="DAG de exemplo de Consumer",
    schedule=[my_dataset],
    start_date=datetime(2023,7,5),
    catchup=False
)


def my_file():
    dataset = pd.read_csv("/opt/airflow/data/Churn_new.csv", sep=';')
    dataset.to_csv("/opt/airflow/data/Churn_new2.csv", sep=';', index=False)


# Taks para usar Consumer
task1 = PythonOperator(task_id="task1", python_callable=my_file, dag=dag, provide_context=True)


# Modelo de como executar DAG
task1