# Spark 3.5 with Iceberg, the AWS SDK bundle (S3FileIO), the Postgres JDBC driver
# (Iceberg JDBC catalog), and the Kafka source baked in, so jobs start without
# downloading anything at runtime.
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
ENV TZ=UTC \
    PYTHONPATH=/app \
    PYSPARK_PYTHON=python3

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

ARG ICEBERG_VERSION=1.6.1
ARG SPARK_VERSION=3.5.3
ARG SCALA=2.12
ARG MAVEN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars && for jar in \
      "org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA}-${ICEBERG_VERSION}.jar" \
      "org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar" \
      "org/apache/spark/spark-sql-kafka-0-10_${SCALA}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA}-${SPARK_VERSION}.jar" \
      "org/apache/spark/spark-token-provider-kafka-0-10_${SCALA}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA}-${SPARK_VERSION}.jar" \
      "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" \
      "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" ; \
    do curl -fsSL -O "${MAVEN}/${jar}"; done

COPY infra/spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY infra/spark/log4j2.properties /opt/spark/conf/log4j2.properties
COPY reconstream /app/reconstream
RUN mkdir -p /checkpoints && chown -R spark:spark /checkpoints /app

USER spark
WORKDIR /app
