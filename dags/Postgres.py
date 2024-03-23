from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Define a função para contar as linhas no PostgreSQL
def count_rows():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')  # Defina o ID de conexão do PostgreSQL
    query = "SELECT COUNT(*) FROM venda1"  # Substitua "sua_tabela" pelo nome da tabela desejada
    rows = postgres_hook.get_records(sql=query)
    if rows:
        row_count = rows[0][0]
        # print(r'Quantidade de Colunas {row_count}')
        return row_count
    else:
        return None

# Define a função para decidir o caminho com base na contagem de linhas
def decide_path(**kwargs):
    task_instance = kwargs['task_instance']
    row_count = task_instance.xcom_pull(task_ids='count_rows_task')
    if row_count is not None:
        if row_count > 48484848400:  # Defina o limite de contagem de linhas para escolher o caminho
            return 'path_greater_than_100'
        else:
            return 'path_less_than_or_equal_to_100'
    else:
        return 'error'

def path_greater_than_100():
    print("1")

def path_less_than_or_equal_to_100():
    print("2")

def error_task():
    print("3")

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
branching_task = BranchPythonOperator(
    task_id='decision_task',
    python_callable=decide_path,
    provide_context=True,
    dag=dag,
)

# Branching baseado na decisão
# branching_task = BranchPythonOperator(
#     task_id='branching_task',
#     python_callable=decide_path,
#     provide_context=True,
#     dag=dag,
# )

# Caminhos
path_greater_than_100 = PythonOperator(
    task_id='path_greater_than_100',
    python_callable=path_greater_than_100,
    provide_context=True,
    dag=dag,
)

path_less_than_or_equal_to_100 = PythonOperator(
    task_id='path_less_than_or_equal_to_100',
    python_callable=path_less_than_or_equal_to_100,
    provide_context=True,
    dag=dag,
)

error_task = PythonOperator(
    task_id='error_task',
    python_callable=error_task,
    provide_context=True,
    dag=dag,
)

# Defina as dependências
start_task >> count_rows_task >> branching_task
branching_task

# Defina branching baseado no valor retornado
# branching_task.do_xcom_push = True  # Habilitar envio do valor retornado como XCom

# Defina tarefas subsequentes com base no branching
branching_task >> [path_greater_than_100 , path_less_than_or_equal_to_100 , error_task] >> end_task


# Define as dependências entre as tarefas
# start_task >> count_rows_task >> decision_task
# decision_task >> [path_greater_than_100 , path_less_than_or_equal_to_100, error_task] >> end_task
