"""
Example Spark Job for Airflow Integration

This PySpark job demonstrates data processing with output to a shared filesystem
for consumption by downstream Airflow tasks.
"""
import sys
import json
from pyspark.sql import SparkSession


def main(output_path: str) -> dict:
    """
    Process sample data and write results to shared filesystem.
    
    Args:
        output_path: Path to write JSON output for Airflow consumption.
    
    Returns:
        Dictionary containing job results.
    """
    spark = SparkSession.builder.appName("ExampleJob").getOrCreate()
    
    # Sample data processing
    data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    # Compute aggregation
    result = {
        "total_records": df.count(),
        "average_age": float(df.agg({"age": "avg"}).collect()[0][0])
    }
    
    # Write result to shared filesystem for Airflow to read
    with open(output_path, "w") as f:
        json.dump(result, f)
    
    spark.stop()
    return result


if __name__ == "__main__":
    output_file = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark-data/output.json"
    main(output_file)
