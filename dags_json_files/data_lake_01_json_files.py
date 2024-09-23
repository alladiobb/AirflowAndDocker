from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.
from airflow.datasets import Dataset

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

    
#  criada dentr do airflow para conectar direto com o diretório na nuvem
GCS_CONN_ID = "google_cloud_default"
BIGQUERY_COON_ID = "google_cloud_default"

orders_dataset = Dataset("bigquery://abcd.orders")

DEFAULT_ARGS = {
    "owner":"Alladio Bonesso",
    "depends_on_past": "False",
    "retires": 0,
    "email_on_failure": False,
    "email_on_retry": False,
}
@dag(
        dag_id="etl-data-set",
        start_date=datetime(2024,9,3),
        max_active_runs=1,
        schedule_interval=None,
        default_args=DEFAULT_ARGS,
        catchup=False,
        owner_links={"linkedin":"https://www.linkedin.com/in/architect-engineer-data-cloud-alladio/"},
        tags=["development","etl",'astrosdk']
)
def load_json_files():

    init = EmptyOperator(task_id="init")

    with TaskGroup(group_id="Detection") as detection_group:
        wait_for_file = GCSObjectsWithPrefixExistenceSensor(]
        )