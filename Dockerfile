FROM apache/airflow:2.2.5-python3.9

RUN pip install --no-cache-dir \
    clickhouse-sqlalchemy \
    psycopg2-binary

ENV AIRFLOW_HOME /opt/airflow
ENV AIRFLOW_UID 50000
ENV AIRFLOW_GID 0

USER root

COPY . /opt/airflow/dags

RUN chown -R ${AIRFLOW_UID}:${AIRFLOW_GID} /opt/airflow

WORKDIR /opt/airflow

CMD ["airflow", "webserver", "-p", "8080"]

EXPOSE 8080
