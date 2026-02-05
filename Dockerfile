FROM apache/airflow:3.1.7

USER root

# Install OpenJDK-17 (required for PySpark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Note: Spark binaries NOT installed.
# - spark_connect_example: Works (no binaries needed)
# - spark_subprocess_example: Works (no binaries needed)
# - spark_submit_example: Requires binaries - install manually or use different image

USER airflow

# Install Python dependencies
COPY requirements.txt /
ARG AIRFLOW_VERSION=3.1.7
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
