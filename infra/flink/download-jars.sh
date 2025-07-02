#!/bin/bash

# Download JAR files for Flink Iceberg integration
# Usage: ./download-jars.sh <dest-path>
#   dest-path: Destination directory to copy JARs to (e.g., PyFlink lib directory)

set -e

# Check for help argument
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    echo "Usage: $0 <dest-path>"
    echo ""
    echo "Downloads Flink Iceberg JAR files and copies them to the specified destination"
    echo ""
    echo "Arguments:"
    echo "  dest-path    Destination directory to copy JARs to (REQUIRED)"
    echo "               Example: ~/.local/lib/python3.11/site-packages/pyflink/lib"
    echo ""
    echo "Examples:"
    echo "  $0 /path/to/pyflink/lib              # Download and copy to PyFlink"
    echo "  $0 ./local-jars                      # Download and copy to local directory"
    echo ""
    echo "To find your PyFlink lib directory:"
    echo "  poetry run python -c \"import pyflink; print(pyflink.__file__.replace('__init__.py', 'lib'))\""
    exit 0
fi

# Validate required argument
if [[ -z "$1" ]]; then
    echo "❌ Error: dest-path argument is required"
    echo ""
    echo "Usage: $0 <dest-path>"
    echo ""
    echo "Examples:"
    echo "  $0 ~/.local/lib/python3.11/site-packages/pyflink/lib"
    echo "  $0 ./local-jars"
    echo ""
    echo "Use '$0 --help' for more information"
    exit 1
fi

# Configuration
ICEBERG_VERSION="1.9.1"
HADOOP_VERSION="3.3.4"
FLINK_VERSION="1.20.0"
MAVEN_REPO="https://repo1.maven.org/maven2"

# Get destination path
DEST_PATH="$1"

# Validate destination directory exists or can be created
if [[ ! -d "${DEST_PATH}" ]]; then
    echo "📁 Destination directory does not exist: ${DEST_PATH}"
    echo "🔨 Attempting to create destination directory..."
    
    if mkdir -p "${DEST_PATH}" 2>/dev/null; then
        echo "✅ Successfully created: ${DEST_PATH}"
    else
        echo "❌ Failed to create destination directory: ${DEST_PATH}"
        echo "💡 Please check permissions or create the directory manually"
        exit 1
    fi
fi

echo "Downloading JAR files directly to: ${DEST_PATH}"

# Function to download JAR with error handling and progress bar
download_jar() {
    local url="$1"
    local filename="$2"
    local filepath="${DEST_PATH}/${filename}"
    
    CURRENT_JAR=$((CURRENT_JAR + 1))
    
    if [ -f "${filepath}" ]; then
        echo "[${CURRENT_JAR}/${TOTAL_JARS}] ✓ ${filename} already exists, skipping..."
        return 0
    fi
    
    echo "[${CURRENT_JAR}/${TOTAL_JARS}] ⬇ Downloading ${filename}..."
    
    # Use wget with progress bar, capture both stdout and stderr
    if wget --progress=bar:force:noscroll "${url}" -O "${filepath}" 2>&1; then
        echo "[${CURRENT_JAR}/${TOTAL_JARS}] ✓ Successfully downloaded ${filename}"
        return 0
    else
        echo "[${CURRENT_JAR}/${TOTAL_JARS}] ✗ Failed to download ${filename}"
        rm -f "${filepath}"
        return 1
    fi
}

# Download essential JARs
TOTAL_JARS=6
CURRENT_JAR=0

echo "Starting JAR downloads (${TOTAL_JARS} files)..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# 1. Iceberg Flink runtime (includes most dependencies)
download_jar \
    "${MAVEN_REPO}/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar" \
    "iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar"

# 2. Flink S3 filesystem connector
download_jar \
    "${MAVEN_REPO}/org/apache/flink/flink-s3-fs-hadoop/${FLINK_VERSION}/flink-s3-fs-hadoop-${FLINK_VERSION}.jar" \
    "flink-s3-fs-hadoop-${FLINK_VERSION}.jar"

# 3. Hadoop common (for Configuration class)
download_jar \
    "${MAVEN_REPO}/org/apache/hadoop/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar" \
    "hadoop-common-${HADOOP_VERSION}.jar"

# 4. Flink shaded Hadoop uber JAR
download_jar \
    "${MAVEN_REPO}/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar" \
    "flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"

# 5. Hadoop HDFS client
download_jar \
    "${MAVEN_REPO}/org/apache/hadoop/hadoop-hdfs-client/${HADOOP_VERSION}/hadoop-hdfs-client-${HADOOP_VERSION}.jar" \
    "hadoop-hdfs-client-${HADOOP_VERSION}.jar"

# 6. Iceberg AWS bundle (for S3 integration)
download_jar \
    "${MAVEN_REPO}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    "iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ JAR download process completed!"

# Count downloaded JAR files
JAR_COUNT=$(find "${DEST_PATH}" -name "*.jar" -type f | wc -l)

echo ""
echo "🎉 Successfully downloaded ${JAR_COUNT} JAR files to: ${DEST_PATH}"
echo "💡 Restart your PyFlink environment to load the new JARs"

echo ""
echo "Downloaded files:"
ls -la "${DEST_PATH}"/*.jar 2>/dev/null || echo "No JAR files found in destination"
