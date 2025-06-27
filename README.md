# Apache Iceberg Playground

A playground for experimenting with Apache Iceberg using REST catalog, MinIO, and various query engines.

## 🏗️ Infrastructure

- **Apache Iceberg REST Catalog** - Catalog service for table metadata
- **MinIO** - S3-compatible object storage for data files
- **Spark** - Distributed query engine with Jupyter Lab
- **PostgreSQL** - Optional backend for catalog metadata

## 🚀 Setup

1. **Start the infrastructure:**
   ```bash
   docker compose up -d --build
   ```

2. **Access the services:**
   - Jupyter Lab (Spark): http://localhost:8888
   - MinIO Console: http://localhost:9001 (admin/password)
   - Spark UI: http://localhost:4040 (when running)

3. **Set up local Python environment:**
   ```bash
   # Install uv if not already installed
   curl -LsSf https://astral.sh/uv/install.sh | sh
   
   # Create and activate virtual environment
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   
   # Install dependencies
   uv sync
   
   # Start Jupyter Lab
   jupyter lab
   ```





4. **Configuration Files:**
   - `spark-defaults.conf` - Spark configuration for Docker setup
   - `pyproject.toml` - Python dependencies for local setup
   - `docker-compose.yml` - Infrastructure setup

## 📊 Notebooks

### Core Tutorials

#### `/notebooks/1-basic_operations.ipynb` - Getting Started
- **Simplified Spark Configuration**: Uses `spark-defaults.conf` for clean notebook code
- **Full Iceberg Integration**: Native connection to REST catalog
- **Basic CRUD Operations**: Create, read, update, delete operations
- **Table Management**: Schema definition and data insertion

#### `/notebooks/6-evolution_with_spark.ipynb` - Schema & Partition Evolution ✨
- **Schema Evolution**: Add, rename, and modify columns without downtime
- **Partition Evolution**: Optimize query performance with dynamic partitioning
- **Backward Compatibility**: Handle mixed schema versions gracefully
- **Best Practices**: Evolution monitoring and data quality validation
- **Refactored Code**: Clean, modular functions with comprehensive error handling
- **Setup**: Works with both Docker Spark and local uv environments

### Advanced Features

#### `/notebooks/select_with_duckdb.ipynb` - Alternative Query Engine
- **DuckDB + PyIceberg Hybrid**: Combines SQL processing with Iceberg I/O
- **Handles Authentication Issues**: Works around DuckDB catalog limitations
- **Performance Comparisons**: DuckDB vs Spark approaches

#### Additional Notebooks
- **Time Travel & Versioning**: Explore historical data states
- **Metadata Operations**: Table history, files, partitions analysis
- **Performance Optimization**: Query tuning and file management


## 🗂️ File Structure

```
play-iceberg/
├── docker-compose.yml                    # Infrastructure setup
├── spark-defaults.conf                   # Spark configuration
├── pyproject.toml                        # Python dependencies
├── uv.lock                               # Dependency lock file
├── notebooks/
│   ├── 1-basic_operations.ipynb          # Getting started tutorial
│   ├── 6-evolution_with_spark.ipynb      # Schema & partition evolution
│   ├── select_with_duckdb.ipynb          # DuckDB integration
│   └── [additional notebooks...]         # More advanced topics
└── README.md                             # This file
```

## 🌐 Endpoints

- **Iceberg REST Catalog**: http://localhost:8181
- **MinIO S3 API**: http://localhost:9000
- **MinIO Console**: http://localhost:9001
- **Jupyter Lab**: http://localhost:8888
- **Spark UI**: http://localhost:4040

## 🔒 Default Credentials

- **MinIO**: admin / password
- **S3 Access**: admin / password

## 🎯 Key Features Demonstrated

### Schema Evolution
- **Zero-downtime column additions**: Add new fields without rewriting data
- **Column operations**: Rename, reorder, and modify existing columns
- **Backward compatibility**: Support mixed schema versions in production
- **Data type evolution**: Handle type changes and compatibility

### Partition Evolution
- **Dynamic partitioning**: Add/remove partition fields based on query patterns
- **Performance optimization**: Bucket partitioning for high-cardinality fields
- **Query optimization**: Leverage partition pruning for faster queries
- **Partition monitoring**: Track partition distribution and skew


## 📚 Learn More

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Iceberg REST Catalog Spec](https://iceberg.apache.org/docs/latest/rest/)
- [Spark Iceberg Integration](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Schema Evolution Guide](https://iceberg.apache.org/docs/latest/evolution/)
- [Partition Evolution Guide](https://iceberg.apache.org/docs/latest/partitioning/)
