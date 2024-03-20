from airflow import DAG
from datetime import datatime
from airflow.operators.python import PythonOperator
import pandas as pandas
import requests
import json


#Criado minha prmieira DAG:
with DAG('first_dag', start_date = datetime(2024,3,21),
    schedule_interval = '30 * * * *', catchup = False ) as dag:
    
    captura_conta_dados = PythonOperator(
        task_id = 'captura_conta_dados',
        python_callable = captura_conta_dados
    )