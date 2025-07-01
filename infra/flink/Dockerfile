FROM apache/flink:1.20.0-scala_2.12-java11

# Install Python 3.11 and essential development tools
RUN apt-get update && apt-get install -y \
    python3.11 \
    python3.11-dev \
    python3.11-venv \
    python3-pip \
    curl \
    wget \
    vim \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create symbolic links for python3.11
RUN ln -sf /usr/bin/python3.11 /usr/bin/python3 && \
    ln -sf /usr/bin/python3.11 /usr/bin/python

# Install PyFlink and Jupyter
RUN pip3 install --no-cache-dir \
    apache-flink==1.20.0 \
    jupyter \
    jupyterlab \
    notebook \
    ipykernel \
    pandas \
    numpy \
    pyarrow==11.0.0

# Create directories for notebooks and workspace
RUN mkdir -p /opt/notebooks && \
    mkdir -p /opt/workspace

# Download essential JARs - starting minimal as requested
ENV ICEBERG_VERSION=1.9.1
ENV HADOOP_VERSION=3.3.4
ENV FLINK_VERSION=1.20.0
ENV FLINK_SHADED_VERSION=1.20.0-16.1
ENV MAVEN_REPO=https://repo1.maven.org/maven2

# 1. Iceberg Flink runtime (includes most dependencies)
RUN wget -q "${MAVEN_REPO}/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar" \
    -O /opt/flink/lib/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar

# 2. Flink S3 filesystem connector
RUN wget -q "${MAVEN_REPO}/org/apache/flink/flink-s3-fs-hadoop/${FLINK_VERSION}/flink-s3-fs-hadoop-${FLINK_VERSION}.jar" \
    -O /opt/flink/lib/flink-s3-fs-hadoop-${FLINK_VERSION}.jar

# 3. Hadoop common (for Configuration class)
RUN wget -q "${MAVEN_REPO}/org/apache/hadoop/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar" \
    -O /opt/flink/lib/hadoop-common-${HADOOP_VERSION}.jar

# 4. Flink shaded Hadoop uber JAR (using available version)
RUN wget -q "${MAVEN_REPO}/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar" \
    -O /opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# 5. Hadoop HDFS client
RUN wget -q "${MAVEN_REPO}/org/apache/hadoop/hadoop-hdfs-client/${HADOOP_VERSION}/hadoop-hdfs-client-${HADOOP_VERSION}.jar" \
    -O /opt/flink/lib/hadoop-hdfs-client-${HADOOP_VERSION}.jar

# 6. Iceberg AWS bundle (for S3 integration)
RUN wget -q "${MAVEN_REPO}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    -O /opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Set environment variables
ENV FLINK_HOME=/opt/flink
ENV PYTHONPATH="${FLINK_HOME}/opt/python:${PYTHONPATH}"
ENV JUPYTER_ENABLE_LAB=yes

# Configure Flink for development
COPY flink-conf.yaml /opt/flink/conf/flink-conf.yaml

# Copy startup script
COPY start.sh /opt/start.sh
RUN chmod +x /opt/start.sh

# Create Jupyter config
RUN jupyter lab --generate-config && \
    echo "c.ServerApp.ip = '0.0.0.0'" >> ~/.jupyter/jupyter_lab_config.py && \
    echo "c.ServerApp.port = 8888" >> ~/.jupyter/jupyter_lab_config.py && \
    echo "c.ServerApp.open_browser = False" >> ~/.jupyter/jupyter_lab_config.py && \
    echo "c.ServerApp.allow_root = True" >> ~/.jupyter/jupyter_lab_config.py && \
    echo "c.ServerApp.token = ''" >> ~/.jupyter/jupyter_lab_config.py && \
    echo "c.ServerApp.password = ''" >> ~/.jupyter/jupyter_lab_config.py

# Set working directory
WORKDIR /opt/workspace

# Expose ports
EXPOSE 8081 8888 6123 6124

# Default command
CMD ["/opt/start.sh"]
