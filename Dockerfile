FROM apache/airflow:3.1.8

USER root
# Cài Java (PySpark cần JVM)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
# Cài packages vào image một lần duy nhất
# Thay vì _PIP_ADDITIONAL_REQUIREMENTS cài lại mỗi lần container restart
RUN pip install --no-cache-dir \
    pymssql \
    apache-airflow-providers-microsoft-mssql \
    pyspark==3.5.5 \
    delta-spark \
    google-cloud-bigquery \
    google-cloud-storage \
    dbt-bigquery
