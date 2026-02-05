"""
Spark Connect with External File DAG (Subprocess Approach)

Uses subprocess to run external Python files that connect via Spark Connect.
- No Spark binaries needed
- External job files (separation of concerns)
- XCom return via stdout prefix convention

Best of both worlds: external files + no binaries.
"""
from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
import subprocess
import json

@dag(schedule=None, catchup=False, tags=["spark", "spark-connect", "subprocess"])
def spark_subprocess_example():
    """Spark job via subprocess + Spark Connect (external files, no binaries)."""

    @task
    def run_external_spark_job() -> dict:
        """
        Run external Spark job file via subprocess.
        
        Returns XCom data parsed from stdout prefix.
        """
        # Get Spark Connect URL from Airflow connection
        conn = BaseHook.get_connection("spark_connect")
        remote_url = f"sc://{conn.host}:{conn.port}"

        # Run external Python script with Spark Connect URL
        result = subprocess.run(
            ["python", "/opt/spark-apps/spark_connect_job.py", remote_url],
            capture_output=True,
            text=True,
        )

        print(result.stdout)

        if result.returncode != 0:
            print(result.stderr)
            raise Exception(f"Spark job failed: {result.stderr}")

        # Parse XCom from stdout (convention: AIRFLOW_XCOM_JSON:{"key": "value"})
        for line in result.stdout.split("\n"):
            if line.startswith("AIRFLOW_XCOM_JSON:"):
                return json.loads(line.replace("AIRFLOW_XCOM_JSON:", ""))

        raise Exception("No AIRFLOW_XCOM_JSON found in output")

    @task
    def process_result(metrics: dict):
        """Process metrics returned from Spark job."""
        print(f"App ID: {metrics.get('app_id', 'N/A')}")
        print(f"Records: {metrics['row_count']}")
        print(f"Status: {metrics['status']}")

    process_result(run_external_spark_job())


spark_subprocess_example()
