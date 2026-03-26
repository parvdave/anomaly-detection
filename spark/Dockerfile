FROM python:3.11-slim

# Install Java (required by Spark) and curl for JAR downloads
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
# JAVA_HOME is discovered at runtime so the path is arch-independent (see CMD)

# Install Python dependencies (pyspark pip package bundles Spark binaries)
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    psycopg2-binary==2.9.9 \
    numpy==1.26.4 \
    pandas==2.2.2

# Download required JARs into /app/jars
RUN mkdir -p /app/jars && \
    curl -fsSL -o /app/jars/postgresql-42.7.3.jar \
        https://jdbc.postgresql.org/download/postgresql-42.7.3.jar && \
    curl -fsSL -o /app/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -fsSL -o /app/jars/kafka-clients-3.4.0.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    curl -fsSL -o /app/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -fsSL -o /app/jars/commons-pool2-2.11.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

COPY src/ /app/src/

WORKDIR /app

# Discover JAVA_HOME at runtime so path works on any arch (amd64/arm64/etc.)
CMD ["sh", "-c", "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && export PATH=$JAVA_HOME/bin:$PATH && exec python -m src.main"]
