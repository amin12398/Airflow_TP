FROM apache/airflow:2.6.3

USER root

RUN pip install --no-cache-dir \
    pymysql \
    pandas \
    sqlalchemy

USER airflow