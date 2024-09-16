import os
from datetime import timedelta

from airflow import DAG
from airflow.models import vabiable
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py","")

S3_BUCKET = Variable.get("data_lake_bucket")

DEFAULT_ARGS = {
    "owner":"Alladio Bonesso",
    "depends_on_past": "False",
    "retires": 0,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    description="Data Lake using BashOperator and AWS CLI vs. AWS Operators",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    default_timeout= timedelta(minutes=5),
    start_date=days_ago(1),
    schedule_interval_=None,
    tags=["data lake"],

) as dag:
    begin = DummyOperator(task_id="begin")

    end = DummyOperator(task_id="end")

    delete_s3_objects = BashOperator(
        task_id="delete_s3_objects",
        bash_command=f'aws s3 rm "s3://{s3_bucket}/bucket_name" --recursive',

    )
    
    list_s3_objects = BashOperator(
        task_id="list_s3_objects",
        bash_command=f'aws s3 rm "s3://{s3_bucket}/bucket_name" --recursive',

    )

    delete_catalog = BashOperator(
        task_id="delete_catalog",
        bash_command='aws glue delete-database --name project01 || echo "Database project01 not found."',
    )

    create_catalog = BashOperator(
        task_id="create_catalog",
        bash_command="""aws glue create-database --database-input \
            '{"Name": "project01", "Description": "Datasets from AWS PROJECT01 relational database"}'""",
    )

chain(
    begin,
    (delete_s3_objects, delete_catalog),
    (list_s3_objects, create_catalog),
    end,
)