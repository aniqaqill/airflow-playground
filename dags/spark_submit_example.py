"""
Spark Submit DAG (External Job File)

Uses SparkSubmitOperator for external .py/.jar files.
- Requires Spark binaries in Airflow container
- Spark code in separate files (better separation)
- Good for: production jobs, JAR files, existing Spark apps
"""
from airflow.sdk import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import json

@dag(schedule=None, catchup=False, tags=["spark", "spark-submit"])
def spark_submit_example():
    """Spark job using SparkSubmitOperator (external files, needs binaries)."""

    submit_job = SparkSubmitOperator(
        task_id="submit_spark_job",
        application="/opt/spark-apps/example_job.py",
        application_args=["/opt/spark-data/output.json"],
        conn_id="spark_default",
        verbose=True,
    )

    @task
    def read_metrics() -> dict:
        """Read metrics from shared filesystem after Spark job completes."""
        with open("/opt/spark-data/output.json") as f:
            return json.load(f)

    @task
    def process_metrics(metrics: dict):
        """Process returned metrics."""
        print(f"Records: {metrics['business_metrics']['total_records']}")
        print(f"Duration: {metrics['execution_metrics']['execution_duration_seconds']}s")

    metrics = read_metrics()
    submit_job >> metrics >> process_metrics(metrics)

spark_submit_example()
