import datetime
import uuid

from airflow import DAG
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.utils.trigger_rule import TriggerRule

# Данные вашей инфраструктуры
YC_DP_AZ = "ru-central1-a"
YC_DP_SSH_PUBLIC_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAICKAi6OCjVnyCCOH9Sx4A91YASYh6DSOmADapxxpfC3X dataproc-homework"
YC_DP_SUBNET_ID = "e9bm24murans0honrsth"
YC_DP_SA_ID = "ajec7n3986ggs2q2kqmg"
YC_DP_METASTORE_URI = "10.128.0.26"
YC_BUCKET = "etl-postgresql-storage-kate"

# Настройки DAG
with DAG(
    "DATA_INGEST",
    schedule="@hourly",
    tags=["data-processing-and-airflow"],
    start_date=datetime.datetime.now(),
    max_active_runs=1,
    catchup=False,
) as ingest_dag:
    # 1 этап: создание временного кластера Yandex Data Processing
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id="dp-cluster-create-task",
        cluster_name=f"tmp-dp-{uuid.uuid4()}",
        cluster_description="Временный кластер для выполнения PySpark-задания под оркестрацией Managed Service for Apache Airflow",
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version="2.1",
        # Минимальные ресурсы для ДЗ
        masternode_resource_preset="s2.small",
        masternode_disk_type="network-hdd",
        masternode_disk_size=50,
        computenode_resource_preset="s2.small",
        computenode_disk_type="network-hdd",
        computenode_disk_size=50,
        computenode_count=1,
        computenode_max_hosts_count=1,
        services=["YARN", "SPARK"],
        datanode_count=0,
        properties={
            "spark:spark.hive.metastore.uris": f"thrift://{YC_DP_METASTORE_URI}:9083",
        },
    )

    # 2 этап: запуск PySpark-задания
    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id="dp-cluster-pyspark-task",
        main_python_file_uri=f"s3a://{YC_BUCKET}/scripts/create-table.py",
    )

    # 3 этап: удаление временного кластера
    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id="dp-cluster-delete-task",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> poke_spark_processing >> delete_spark_cluster
