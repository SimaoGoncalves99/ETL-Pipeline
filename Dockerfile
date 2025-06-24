FROM apache/airflow:3.0.2

COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
COPY --chown=airflow:root dags/etl.py /opt/airflow/dags
COPY --chown=airflow:root tests/dags/test_dag_example.py /opt/airflow/dags