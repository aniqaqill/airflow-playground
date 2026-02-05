# Airflow Playground

Apache Airflow with Spark 4.0 integration - three approaches demonstrated.

## Quick Start

```bash
docker compose up -d --build
```

## Services

| Service | URL |
|---------|-----|
| Airflow UI | http://localhost:8080 (airflow/airflow) |
| Spark Master | http://localhost:8081 |
| Spark Worker | http://localhost:8082 |

## Three Spark Integration Approaches

### 1. Spark Connect Inline (`spark_connect_example`)

**PySpark code directly in DAG task.**

```
Airflow Task --(gRPC)--> Spark Connect --> Spark Cluster
      └── Returns via XCom
```

- No Spark binaries needed
- No external files
- Best for: quick jobs, simple transforms

### 2. SparkSubmitOperator (`spark_submit_example`)

**External .py/.jar files via spark-submit.**

```
SparkSubmitOperator --(spark-submit)--> Spark Master --> Worker
                                                            |
         Airflow <--(read file)-- Shared Filesystem <-------
```

- Requires Spark binaries in Airflow container
- Job files in `spark/apps/`
- Best for: production jobs, JAR files, existing Spark apps

### 3. Subprocess + Spark Connect (`spark_subprocess_example`)

**External Python files using Spark Connect. Best of both worlds.**

```
PythonOperator --(subprocess)--> External .py --(gRPC)--> Spark Connect
      └── Returns via stdout prefix (AIRFLOW_XCOM_JSON:)
```

- No Spark binaries needed
- External files for separation
- Best for: new Spark apps with clean separation

## Comparison

| Aspect | Spark Connect | SparkSubmitOperator | Subprocess |
|--------|---------------|---------------------|------------|
| External Files | No | Yes | Yes |
| Binaries Needed | No | Yes | No |
| XCom Return | Direct | Filesystem | Stdout prefix |
| JAR Support | No | Yes | No |
| Best For | Quick jobs | Production JARs | New Python jobs |

## XCom Return Convention (Subprocess)

External scripts return data by printing to stdout:
```python
print(f"AIRFLOW_XCOM_JSON:{json.dumps(metrics)}")
```

DAG parses this prefix to extract XCom data.

## Connections Required

| Connection ID | Type | Host | Port | Used By |
|---------------|------|------|------|---------|
| `spark_default` | Spark | spark://spark-master | 7077 | SparkSubmitOperator |
| `spark_connect` | Generic | spark-connect | 15002 | Subprocess approach |

## Verify

```bash
docker compose ps
cat spark/data/output.json
```

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Permission denied | `sudo chown -R $(id -u):0 logs/` |
| Version mismatch | Pin `pyspark==4.0.0` |
