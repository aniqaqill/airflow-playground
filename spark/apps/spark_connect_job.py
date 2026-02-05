"""
Spark Connect Job (External File)

This file is called via subprocess from Airflow DAG.
Uses Spark Connect protocol - no Spark binaries needed.

XCom Convention:
    Print AIRFLOW_XCOM_JSON:{"key": "value"} to return data to Airflow.
"""
import sys
import json
from datetime import datetime


def main():
    if len(sys.argv) < 2:
        print("Error: No Spark Connect URL provided")
        sys.exit(1)

    remote_url = sys.argv[1]
    print(f"Connecting to Spark Connect at: {remote_url}")

    try:
        from pyspark.sql import SparkSession
        import time

        start_time = time.time()

        spark = SparkSession.builder \
            .remote(remote_url) \
            .appName("SubprocessSparkJob") \
            .getOrCreate()

        # Sample data processing
        data = [
            ("Alice", 34, "Engineering"),
            ("Bob", 45, "Sales"),
            ("Charlie", 29, "Engineering"),
        ]
        df = spark.createDataFrame(data, ["name", "age", "department"])

        # Business logic
        row_count = df.count()
        avg_age = float(df.agg({"age": "avg"}).collect()[0][0])

        # Build metrics to return to Airflow
        metrics = {
            "row_count": row_count,
            "average_age": avg_age,
            "execution_seconds": round(time.time() - start_time, 2),
            "status": "COMPLETED",
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Return XCom via stdout convention
        print(f"AIRFLOW_XCOM_JSON:{json.dumps(metrics)}")

        spark.stop()

    except Exception as e:
        print(f"Spark Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
