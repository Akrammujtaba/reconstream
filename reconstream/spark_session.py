"""SparkSession wired to an Iceberg catalog (JDBC catalog in Postgres, data in S3/MinIO)."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def build_spark(app_name: str, catalog: str = "lake") -> SparkSession:
    prefix = f"spark.sql.catalog.{catalog}"
    conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
        f"{prefix}.uri": os.environ.get(
            "ICEBERG_JDBC_URI", "jdbc:postgresql://postgres:5432/iceberg"
        ),
        f"{prefix}.jdbc.user": os.environ.get("ICEBERG_JDBC_USER", "recon"),
        f"{prefix}.jdbc.password": os.environ.get("ICEBERG_JDBC_PASSWORD", "recon"),
        f"{prefix}.warehouse": os.environ.get("ICEBERG_WAREHOUSE", "s3://lake/warehouse"),
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{prefix}.s3.endpoint": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
        f"{prefix}.s3.path-style-access": "true",
        f"{prefix}.s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "minio"),
        f"{prefix}.s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "minio123"),
        f"{prefix}.client.region": os.environ.get("AWS_REGION", "us-east-1"),
        "spark.sql.defaultCatalog": catalog,
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"),
        "spark.sql.adaptive.enabled": "true",
    }
    builder = SparkSession.builder.appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
