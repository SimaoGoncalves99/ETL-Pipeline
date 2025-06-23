FROM apache/airflow:3.0.2

COPY --chown=airflow:root dags/etl.py /opt/airflow/dags