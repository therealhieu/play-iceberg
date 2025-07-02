# Apache Iceberg Playground

Experiment with Apache Iceberg using REST catalog, MinIO, Spark, and Flink.

## Quick Start

```bash
# Setup and start infrastructure
./setup.sh
docker compose up -d --build

# For local notebooks (1-2, 10)
uv sync && source .venv/bin/activate && jupyter lab

# Download Flink JARs
./infra/flink/download-jars.sh .venv/lib/python3.11/site-packages/pyflink/lib

# For Spark notebooks (3-9)
# Open http://localhost:8888 in browser
```

## Services

- **Jupyter Lab (Spark)**: http://localhost:8888
- **MinIO Console**: http://localhost:9001 (admin/password)
- **Flink Web UI**: http://localhost:8082
- **Iceberg REST Catalog**: http://localhost:8181

## Notebooks

### Local Environment (run with `jupyter lab`)
1. **Create table** - PyIceberg table creation and schema design
2. **Insert data** - Spark DataFrame insertion and validation
10. **Flink local** - Local Flink development with Iceberg

### Spark Environment (run at http://localhost:8888)
3. **Select with Spark** - SQL queries and DataFrame operations
4. **Upsert operations** - MERGE statements and conflict resolution
5. **Schema evolution** - Add/modify columns safely
6. **Time travel** - Query historical table versions
7. **Metadata inspection** - Table internals and optimization
8. **Architecture layers** - Iceberg's three-layer design
9. **Flink Docker** - Containerized Flink-Iceberg integration

## Learn More

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [REST Catalog Spec](https://iceberg.apache.org/docs/latest/rest/)
- [Spark Integration](https://iceberg.apache.org/docs/latest/spark-configuration/)
