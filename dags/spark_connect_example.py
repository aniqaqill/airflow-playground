"""
Spark Connect DAG (Inline Code)

Uses Spark Connect for inline PySpark execution.
- No Spark binaries needed in Airflow
- Spark code runs directly in DAG tasks
- Good for: quick jobs, interactive development
"""
from airflow.sdk import dag, task
import json


@dag(schedule=None, catchup=False, tags=["spark", "spark-connect"])
def spark_connect_example():
    """Spark job using Spark Connect (inline code, no binaries)."""
    
    @task
    def run_spark_job() -> dict:
        """Execute Spark job inline via Spark Connect."""
        from pyspark.sql import SparkSession
        import time
        from datetime import datetime
        
        start_time = time.time()
        
        spark = SparkSession.builder \
            .remote("sc://spark-connect:15002") \
            .appName("SparkConnectJob") \
            .getOrCreate()
        
        # Spark logic inline
        data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        result = {
            "total_records": df.count(),
            "average_age": float(df.agg({"age": "avg"}).collect()[0][0]),
            "execution_seconds": round(time.time() - start_time, 2),
        }
        
        spark.stop()
        return result
    
    @task
    def process_result(result: dict):
        print(f"Processed {result['total_records']} records in {result['execution_seconds']}s")
    
    process_result(run_spark_job())


spark_connect_example()
