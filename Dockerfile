FROM apache/airflow:3.1.7

USER root

# Install OpenJDK-17 (required for PySpark Spark Connect client)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Copy and install requirements
COPY requirements.txt /
ARG AIRFLOW_VERSION=3.1.7
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
