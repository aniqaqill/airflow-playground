"""
Example Spark Job for Airflow Integration

Demonstrates:
1. Data processing on Spark cluster
2. Custom metrics collection (execution time, record counts, etc.)
3. Writing results to shared filesystem for Airflow consumption
"""
import sys
import json
import time
from datetime import datetime


def main(output_path: str) -> dict:
    """
    Process data and return custom metrics to Airflow.
    
    This job collects execution metrics that Airflow can use for:
    - Monitoring and alerting
    - SLA tracking
    - Dashboard reporting
    """
    from pyspark.sql import SparkSession
    
    start_time = time.time()
    job_start = datetime.utcnow().isoformat()
    
    spark = SparkSession.builder.appName("ExampleJob").getOrCreate()
    
    # Sample data processing
    data = [
        ("Alice", 34, "Engineering"),
        ("Bob", 45, "Sales"),
        ("Charlie", 29, "Engineering"),
        ("Diana", 38, "Marketing"),
        ("Eve", 52, "Engineering"),
    ]
    df = spark.createDataFrame(data, ["name", "age", "department"])
    
    # Business logic - compute aggregations
    total_records = df.count()
    avg_age = float(df.agg({"age": "avg"}).collect()[0][0])
    dept_counts = {row["department"]: row["count"] for row in df.groupBy("department").count().collect()}
    
    end_time = time.time()
    
    # Custom metrics to return to Airflow
    result = {
        # Business metrics
        "total_records": total_records,
        "average_age": avg_age,
        "department_breakdown": dept_counts,
        
        # Execution metrics (similar to FlowEngine callback data)
        "execution_metrics": {
            "job_start_time": job_start,
            "job_end_time": datetime.utcnow().isoformat(),
            "execution_duration_seconds": round(end_time - start_time, 2),
            "spark_app_id": spark.sparkContext.applicationId,
            "status": "SUCCESS",
        }
    }
    
    # Write to shared filesystem for Airflow to consume
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)
    
    spark.stop()
    return result


if __name__ == "__main__":
    output_file = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark-data/output.json"
    result = main(output_file)
    print(f"Job completed: {result['execution_metrics']['status']}")
