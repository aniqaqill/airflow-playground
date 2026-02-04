"""
Spark Processing DAG

Demonstrates Spark integration with Airflow using Spark Connect:
- Connect to Spark cluster via Spark Connect (gRPC, no binaries needed)
- Pass data between Spark jobs and Airflow tasks via shared filesystem
"""
from airflow.sdk import dag, task
import json


@dag(schedule=None, catchup=False)
def spark_processing():
    """DAG demonstrating Spark job via Spark Connect."""
    
    @task
    def run_spark_job():
        """Execute Spark job using Spark Connect."""
        from pyspark.sql import SparkSession
        
        # Connect to Spark cluster via Spark Connect (gRPC)
        spark = SparkSession.builder \
            .remote("sc://spark-connect:15002") \
            .appName("ExampleJob") \
            .getOrCreate()
        
        # Sample data processing
        data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        # Compute aggregation
        result = {
            "total_records": df.count(),
            "average_age": float(df.agg({"age": "avg"}).collect()[0][0])
        }
        
        # Write result to shared filesystem
        with open("/opt/spark-data/output.json", "w") as f:
            json.dump(result, f)
        
        spark.stop()
        return result
    
    @task
    def process_spark_result(spark_result):
        """Process Spark job results."""
        print(f"Spark job processed {spark_result['total_records']} records")
        print(f"Average age: {spark_result['average_age']}")
        return spark_result
    
    @task
    def final_action(spark_result):
        """Downstream task using Spark results."""
        print(f"Final processing with: {spark_result}")
        return {"status": "completed", "input": spark_result}
    
    spark_result = run_spark_job()
    processed = process_spark_result(spark_result)
    final_action(processed)


spark_processing()
