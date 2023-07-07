from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
import pandas as pd
import requests
from datetime import datetime

dag = DAG(
    "dagsensor", 
    description="DAG de exemplo de Sensor",
    schedule_interval=None,
    start_date=datetime(2023,7,5),
    catchup=False
)

# Função para chamar a API - mas precisa verificar com o sensor se a url esta ok
def query_api():
    response = requests.get("https://api.publicapis.org/entries")
    print(response.text)

# Create Sensor
check_api = HttpSensor(task_id='check_api', http_conn_id='connection',endpoint='entries', poke_interval=5, timeout=20, dag=dag)

# Taks para usar o Sensor
process_data = PythonOperator(task_id="process_data", python_callable=query_api, dag=dag)

# Modelo de como executar DAG
check_api >> process_data