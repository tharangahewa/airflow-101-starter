FROM apache/airflow:2.2.1-python3.8

USER airflow
COPY ./dags /opt/airflow/dags
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt