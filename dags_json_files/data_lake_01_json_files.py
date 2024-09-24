from datetime import datetime, timedelta

#Providers AWS apache-airflow-providers-amazon==8.28.0 
#And GCP apache-airflow-providers-google v10.22.0
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
# from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.
from airflow.datasets import Dataset

#Astro SDK  astro-sdk-python==1.8.1
from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata
    
#  criada dentr do airflow para conectar direto com o diretório na nuvem
GCS_CONN_ID = "google_cloud_default"
BIGQUERY_COON_ID = "google_cloud_default"

sales_dataset = Dataset("bigquery://kafkaschema-your-bucket.sales")

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
        tags=["development","etl",'astrosdk','gcs','bq','detection','sensor']
)
def load_json_files():

    init = EmptyOperator(task_id="init")

    with TaskGroup(group_id="Detection") as detection_group:
        wait_for_file = GCSObjectsWithPrefixExistenceSensor(
            task_id="wait_for_file",
            bucket="kafkaschema-your-bucket",
            prefix="com.kafkaschema.data/kafka/sales/",
            google_cloud_conn_id=GCS_CONN_ID,
            timeoutError=2500,
            # deferrable=True,
            poke_interval=20,
            

        )
    
    with TaskGroup(group_id="Sales") as sales_group:
        load_sales_json_file = aql.load_file(
            task_id="locad_sales_json_file",
            input_file=File(
                path="gs://kafkaschema-your-bucket/com.kafkaschema.data/kafka/sales/",
                filetype=FileType.JSON,
                conn_id=GCS_CONN_ID
            ),
            output_table=Table(
                name="sales",
                metadata=Metadata(schema="kafkaschema")
                conn_id=BIGQUERY_COON_ID
            ),
            if_exists="append",
            use_native_support=True,
            Columns_names_capitalization="original",
            outlets=[sales_dataset]
        )

        load_sales_json_file

    finish = EmptyOperator(task_id="finish")

    init >> detection_group >> sales_group >> finish

dag = load_json_files
