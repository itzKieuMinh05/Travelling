"""
BƯỚC 5: Airflow DAG — Orchestrate toàn bộ pipeline
Schedule: chạy mỗi ngày lúc 2:00 AM
Flow: ingest → bronze_to_silver → dbt_transform → notify_done
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago


# ── Default args ───────────────────────────────────────────────
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG definition ─────────────────────────────────────────────
with DAG(
    dag_id="airbnb_lakehouse_pipeline",
    description="Full Airbnb Lakehouse pipeline: Kafka → MinIO → Spark → dbt",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 2 * * *",     # 2:00 AM mỗi ngày
    catchup=False,
    tags=["airbnb", "lakehouse", "data-engineering"],
    doc_md="""
    ## Airbnb Lakehouse Pipeline

    **Flow:**
    1. `ingest_to_kafka` — đẩy CSV vào Kafka topics
    2. `consume_to_bronze` — Consumer ghi từ Kafka vào MinIO bronze zone
    3. `spark_bronze_to_silver` — PySpark làm sạch → Silver Iceberg tables
    4. `dbt_run` — dbt transform Silver → Gold (marts)
    5. `dbt_test` — dbt chạy data quality tests
    6. `pipeline_complete` — log pipeline done

    **Connections cần setup trong Airflow UI:**
    - `spark_default` → spark://spark-master:7077
    """,
) as dag:

    # ── Task 1: Kafka Producer ─────────────────────────────────
    ingest_to_kafka = BashOperator(
        task_id="ingest_to_kafka",
        bash_command="""
            cd /opt/airflow && \
            pip install kafka-python -q && \
            python /opt/airflow/scripts/kafka_producer.py \
                --listings /opt/airflow/data/Listings.csv \
                --reviews  /opt/airflow/data/Reviews.csv
        """,
        execution_timeout=timedelta(hours=2),
        doc_md="Đọc CSV và publish records vào Kafka topics",
    )

    # ── Task 2: Kafka Consumer → Bronze ───────────────────────
    consume_to_bronze = BashOperator(
        task_id="consume_to_bronze",
        bash_command="""
            cd /opt/airflow && \
            python /opt/airflow/scripts/kafka_consumer.py
        """,
        execution_timeout=timedelta(hours=1),
        doc_md="Consume từ Kafka và ghi vào MinIO bronze zone",
    )

    # ── Task 3: Spark Bronze → Silver ─────────────────────────
    spark_bronze_to_silver = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command="""
            docker exec spark-master \
            spark-submit \
              --master spark://spark-master:7077 \
              --packages \
                org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,\
                org.apache.hadoop:hadoop-aws:3.3.4,\
                com.amazonaws:aws-java-sdk-bundle:1.12.262 \
              /opt/spark-apps/bronze_to_silver.py
        """,
        execution_timeout=timedelta(hours=3),
        doc_md="PySpark: clean Bronze → write Silver Iceberg tables",
    )

    # ── Task 4: dbt run (Silver → Gold) ───────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
            docker exec dbt \
            dbt run \
              --project-dir /usr/app/dbt \
              --profiles-dir /usr/app/dbt \
              --target dev \
              --select marts.*
        """,
        execution_timeout=timedelta(hours=1),
        doc_md="dbt: build Gold mart tables từ Silver",
    )

    # ── Task 5: dbt test (data quality) ───────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
            docker exec dbt \
            dbt test \
              --project-dir /usr/app/dbt \
              --profiles-dir /usr/app/dbt \
              --select marts.*
        """,
        execution_timeout=timedelta(minutes=30),
        doc_md="dbt: chạy data quality tests trên Gold tables",
    )

    # ── Task 6: Generate dbt docs ──────────────────────────────
    dbt_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command="""
            docker exec dbt \
            dbt docs generate \
              --project-dir /usr/app/dbt \
              --profiles-dir /usr/app/dbt
        """,
        execution_timeout=timedelta(minutes=10),
        doc_md="Generate dbt documentation (lineage graph, column docs)",
    )

    # ── Task 7: Log completion ─────────────────────────────────
    def log_pipeline_success(**context):
        run_date = context["ds"]
        print(f"""
        ╔══════════════════════════════════════════════╗
        ║   ✅ AIRBNB LAKEHOUSE PIPELINE COMPLETE      ║
        ║   Run date : {run_date}                      ║
        ║   Flow     : Kafka → Bronze → Silver → Gold  ║
        ╚══════════════════════════════════════════════╝
        """)
        print("Gold tables available:")
        print("  - iceberg.gold.mart_city_pricing")
        print("  - iceberg.gold.mart_host_performance")
        print("  - iceberg.gold.mart_review_trends")
        print("\nConnect Superset to Trino at trino:8080 to visualize!")

    pipeline_complete = PythonOperator(
        task_id="pipeline_complete",
        python_callable=log_pipeline_success,
        doc_md="Log pipeline success và thông tin các Gold tables",
    )

    # ── Dependency chain ───────────────────────────────────────
    # ingest → consume → spark → dbt run → dbt test → dbt docs → done
    (
        ingest_to_kafka
        >> consume_to_bronze
        >> spark_bronze_to_silver
        >> dbt_run
        >> dbt_test
        >> dbt_docs
        >> pipeline_complete
    )
