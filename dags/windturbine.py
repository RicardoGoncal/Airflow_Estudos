from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import os


default_args = {
    'depends_on_past' : False,
    'email' : ['rika_alves@hotmail.com'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(seconds=10)
}

# Configuração da DAG
dag = DAG('windturbine', description='Projeto WindTurbine', schedule_interval=None, start_date=datetime(2023,7,5), 
          catchup=False, default_args=default_args, default_view='graph',doc_md='## Dag para registrar dados de Turbina Eolica')


# Tasks que vao checar a temperatura
group_check_temp = TaskGroup("group_check_temp", dag=dag)

# Tasks que vao inserar dados no Postgres
group_database = TaskGroup("group_database", dag=dag)


# Criacao das tasks
file_sensor_task = FileSensor(
    task_id='file_sensor',
    filepath = Variable.get('path_file'),
    fs_conn_id = 'fs_default',
    poke_interval = 10,
    dag=dag
)


def process_file(**kwargs):
    with open(Variable.get('path_file')) as f:
        data = json.load(f)
        kwargs['ti'].xcom_push(key='idtemp', value=data['idtemp'])
        kwargs['ti'].xcom_push(key='powerfactor', value=data['powerfactor'])
        kwargs['ti'].xcom_push(key='hydraulicpressure', value=data['hydraulicpressure'])
        kwargs['ti'].xcom_push(key='temperature', value=data['temperature'])
        kwargs['ti'].xcom_push(key='timestamp', value=data['timestamp'])

    os.remove(Variable.get('path_file'))


get_data = PythonOperator(
    task_id='get_data',
    python_callable=process_file,
    provide_context = True,
    dag=dag
    )

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id = 'postgres',
    sql=''' create table if not exists
    sensors (idtemp varchar, powerfactor varchar,
    hydraulicpressure varchar, temperature varchar,
    timestamp varchar);
    ''',
    task_group=group_database,
    dag=dag
)

insert_data = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id = 'postgres',
    parameters=(
    '{{ ti.xcom_pull(task_ids="get_data", key="idtemp") }}',
    '{{ ti.xcom_pull(task_ids="get_data", key="powerfactor") }}',
    '{{ ti.xcom_pull(task_ids="get_data", key="hydraulicpressure") }}',
    '{{ ti.xcom_pull(task_ids="get_data", key="temperature") }}',
    '{{ ti.xcom_pull(task_ids="get_data", key="timestamp") }}'
    ),
    sql = ''' insert into sensors (idtemp,powerfactor,hydraulicpressure,temperature,timestamp) 
    values (%s, %s, %s, %s, %s);
    ''',
    task_group=group_database,
    dag=dag
)

send_email_alert = EmailOperator(
    task_id='email_alert',
    to = 'rika_alves@hotmail.com',
    subject = 'Airflow Alert',
    html_content = ''' <h3> Alerta de Temperatura. </h3>
    <p> DAG: windturbine </p>
    ''',
    task_group=group_check_temp,
    dag=dag
)

send_email_normal = EmailOperator(
    task_id='email_normal',
    to = 'rika_alves@hotmail.com',
    subject = 'Airflow Advise',
    html_content = ''' <h3> Temperatura Normal. </h3>
    <p> DAG: windturbine </p>
    ''',
    task_group=group_check_temp,
    dag=dag
)

# Branch Operator

def avaliar_temp(**context):
    temp = float(context['ti'].xcom_pull(task_ids='get_data', key="temperature"))
    if temp >= 24:
        return 'group_check_temp.email_alert'
    else:
        return 'group_check_temp.email_normal'


check_temp_branch = BranchPythonOperator(
    task_id = 'check_temp_branch',
    python_callable = avaliar_temp,
    provide_context = True,
    dag=dag,
    task_group = group_check_temp
)


# Definir Ordem das Tasks
with group_check_temp:
    check_temp_branch >> [send_email_alert, send_email_normal]

with group_database:
    create_table >> insert_data


file_sensor_task >> get_data
get_data >> group_check_temp
get_data >> group_database