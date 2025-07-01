#!/bin/bash

echo "🚀 Starting Flink Development Environment"
echo "📦 Essential JARs loaded:"
ls -la /opt/flink/lib/*.jar | grep -E "(iceberg|hadoop|flink-s3)" || echo "No Iceberg/Hadoop JARs found"

echo ""
echo "🔧 Starting Flink cluster..."

# Start JobManager in background
echo "Starting JobManager..."
${FLINK_HOME}/bin/jobmanager.sh start-foreground &
JOBMANAGER_PID=$!

# Wait a bit for JobManager to start
sleep 5

# Start TaskManager in background  
echo "Starting TaskManager..."
${FLINK_HOME}/bin/taskmanager.sh start-foreground &
TASKMANAGER_PID=$!

# Wait for Flink to be ready
sleep 10

echo ""
echo "✅ Flink cluster ready!"
echo "🌐 Flink Web UI: http://localhost:8081"
echo "📓 Starting Jupyter Lab..."
echo "🌐 Jupyter Lab: http://localhost:8888"
echo ""

# Cleanup function for graceful shutdown
cleanup() {
    echo "🛑 Shutting down services..."
    kill $JOBMANAGER_PID $TASKMANAGER_PID 2>/dev/null
    exit 0
}

# Trap signals for graceful shutdown
trap cleanup SIGTERM SIGINT

# Start Jupyter Lab
cd /opt/workspace
exec jupyter lab --allow-root --ip=0.0.0.0 --port=8888 --no-browser --NotebookApp.token="" 