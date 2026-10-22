# Airflow plus two isolated virtualenvs so job dependencies never fight Airflow's:
#   jobs-venv: PySpark + Iceberg jars + Great Expectations (spark-submit jobs)
#   dbt-venv:  dbt-core + dbt-trino
FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java \
    TZ=UTC

USER airflow
COPY infra/spark/requirements.txt /tmp/jobs-requirements.txt
RUN export PIP_USER=false \
    && python -m venv /opt/airflow/jobs-venv \
    && /opt/airflow/jobs-venv/bin/pip install --no-cache-dir pyspark==3.5.3 -r /tmp/jobs-requirements.txt \
    && python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core==1.9.*" "dbt-trino==1.9.3"

ARG ICEBERG_VERSION=1.6.1
ARG MAVEN=https://repo1.maven.org/maven2
RUN cd "$(/opt/airflow/jobs-venv/bin/python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && for jar in \
      "org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      "org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar" \
      "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar" \
      "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar" \
      "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" \
      "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" ; \
    do curl -fsSL -O "${MAVEN}/${jar}"; done

ENV PYTHONPATH=/opt/reconstream \
    PYSPARK_PYTHON=/opt/airflow/jobs-venv/bin/python
