from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook

# Define a função para contar as linhas no PostgreSQL
def count_rows():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')  # Defina o ID de conexão do PostgreSQL
    query = "SELECT COUNT(*) FROM venda1"  # Substitua "sua_tabela" pelo nome da tabela desejada
    rows = postgres_hook.get_records(sql=query)
    if rows:
        row_count = rows[0][0]
        return row_count
    else:
        return None

# Define a função para decidir o caminho com base na contagem de linhas
def decide_path(**kwargs):
    task_instance = kwargs['task_instance']
    row_count = task_instance.xcom_pull(task_ids='count_rows_task')
    if row_count is not None:
        if row_count > 100:  # Defina o limite de contagem de linhas para escolher o caminho
            return 'path_greater_than_100'
        else:
            return 'path_less_than_or_equal_to_100'
    else:
        return 'error'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('postgres_count_rows_dag',
          default_args=default_args,
          description='DAG to count rows in PostgreSQL and decide paths based on count',
          schedule_interval='@once',  # Define a frequência de execução
        )

start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

# Task para contar as linhas
count_rows_task = PythonOperator(
    task_id='count_rows_task',
    python_callable=count_rows,
    dag=dag,
)

# Task para decidir o caminho
decision_task = PythonOperator(
    task_id='decision_task',
    python_callable=decide_path,
    provide_context=True,
    dag=dag,
)

# Define os caminhos com base na decisão
path_greater_than_100 = DummyOperator(task_id='path_greater_than_100', dag=dag)
path_less_than_or_equal_to_100 = DummyOperator(task_id='path_less_than_or_equal_to_100', dag=dag)
error_task = DummyOperator(task_id='error_task', dag=dag)

# Define as dependências entre as tarefas
start_task >> count_rows_task >> decision_task
decision_task >> path_greater_than_100 >> end_task
decision_task >> path_less_than_or_equal_to_100 >> end_task
decision_task >> error_task >> end_task
