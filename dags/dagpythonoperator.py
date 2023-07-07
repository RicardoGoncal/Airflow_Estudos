from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import statistics as sts
from datetime import datetime

dag = DAG(
    "dagpythonoperator", 
    description="DAG de exemplo de usar o Python Operator",
    schedule_interval=None,
    start_date=datetime(2023,7,5),
    catchup=False
)


def data_clean():
    dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';') # Import Dados

    # Dar nome as colunas
    dataset.columns = ['Id','Score','Estado','Genero','Idade','Patrimonio','Saldo','Produtos','TemCartaoCred','Ativo','Salario','Saiu']

    # Calcular mediana dos salarios
    mediana = sts.median(dataset['Salario'])
    dataset['Salario'].fillna(mediana, inplace=True) # Atualizar Dataset

    # Preencher Generos
    dataset['Genero'].fillna('Masculino', inplace=True)

    # Calcular mediana das idades
    mediana = sts.median(dataset['Idade'])
    dataset.loc[(dataset['Idade'] < 0) | (dataset['Idade'] > 120), 'Idade'] = mediana # Faz filtro e faz a troca pela mediana


    # Remover Duplicados pelo ID
    dataset.drop_duplicates(subset='Id', keep="first", inplace=True)

    # Criar novo dataset apos o tratamento
    dataset.to_csv("/opt/airflow/data/Churn_clean.csv", sep=';', index=False)


# Dag para usar Branch
task1 = PythonOperator(task_id="task1", python_callable=data_clean, dag=dag)


# Modelo de como executar DAG
task1