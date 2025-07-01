# Apache Iceberg Playground

A playground for experimenting with Apache Iceberg using REST catalog, MinIO, and various query engines.

## 🏗️ Infrastructure

- **Apache Iceberg REST Catalog** - Catalog service for table metadata
- **MinIO** - S3-compatible object storage for data files
- **Spark** - Distributed query engine with Jupyter Lab
- **Flink 1.20** - Stream processing engine with Iceberg integration
- **PostgreSQL** - Optional backend for catalog metadata

## 🚀 Setup

1. **Setup infrastructure:**
   ```bash
   ./setup.sh
   ```

2. **Start the infrastructure:**
   ```bash
   docker compose up -d --build
   ```

3. **Access the services:**
   - Jupyter Lab (Spark): http://localhost:8888
   - Flink Web UI: http://localhost:8082
   - MinIO Console: http://localhost:9001 (admin/password)
   - Spark UI: http://localhost:4040 (when running)

4. **Set up local Python environment:**
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

5. **Running Notebooks:**

   **For Notebooks 1-3, 10 (Local Environment):**
   ```bash
   # Ensure Docker is running first
   docker compose up -d --build
   
   # Download Flink dependencies (for notebook 10)
   ./setup.sh
   
   # Then start local Jupyter
   uv sync
   source .venv/bin/activate
   jupyter lab
   # Open notebooks 1-3, 10 in the local Jupyter Lab
   ```

   **For Notebooks 4-9 (Spark Environment):**
   ```bash
   # Access Spark Jupyter Lab in browser
   # http://localhost:8888
   # Open notebooks 4-9 in the Docker Jupyter Lab
   ```

6. **Configuration Files:**
   - `infra/spark/spark-defaults.conf` - Spark configuration for Docker setup
   - `infra/flink/flink-conf.yaml` - Flink configuration for Docker setup
   - `infra/flink/setup-flink-deps.sh` - Download Flink Iceberg dependencies
   - `pyproject.toml` - Python dependencies for local setup
   - `docker-compose.yml` - Infrastructure setup
   - `setup.sh` - Main setup script

## 📊 Notebooks

### 🏠 Local Environment Notebooks (uv + PyIceberg)
**Run these in your local Jupyter Lab after `uv sync` and environment activation:**

#### `/notebooks/1-create_user_table.ipynb` - Table Creation with PyIceberg
- **Environment**: Local Python (uv venv) + PyIceberg
- **Connection**: localhost endpoints to Docker services
- **Focus**: Table creation, schema definition, namespace management
- **Run with**: `jupyter lab` (local environment)

#### `/notebooks/2-insert_with_polars.ipynb` - Data Insertion with Polars
- **Environment**: Local Python (uv venv) + Polars + PyIceberg
- **Connection**: localhost endpoints to Docker services  
- **Focus**: Efficient data loading with Polars DataFrames
- **Run with**: `jupyter lab` (local environment)

#### `/notebooks/3-select_with_duckdb.ipynb` - Analytics with DuckDB
- **Environment**: Local Python (uv venv) + DuckDB + PyIceberg
- **Connection**: localhost endpoints to Docker services
- **Focus**: High-performance analytics and SQL querying
- **Run with**: `jupyter lab` (local environment)

#### `/notebooks/10-flink_iceberg_streaming.ipynb` - Stream Processing with PyFlink
- **Environment**: Local Python (uv venv) + PyFlink 1.20 + PyIceberg
- **Connection**: localhost endpoints to Docker services
- **Focus**: Stream processing and real-time analytics with Flink
- **Run with**: `jupyter lab` (local environment)

### 🐳 Spark Docker Environment Notebooks
**Run these in the Spark Docker container at http://localhost:8888:**

#### `/notebooks/4-select_with_spark.ipynb` - Spark SQL Querying
- **Environment**: Spark Docker container with full cluster capabilities
- **Connection**: Internal Docker network (iceberg-rest:8181, minio:9000)
- **Focus**: Distributed SQL operations and DataFrame API
- **Run with**: Docker Jupyter Lab at http://localhost:8888

#### `/notebooks/5-upsert_with_spark.ipynb` - ACID Operations
- **Environment**: Spark Docker container
- **Focus**: Upsert operations, ACID transactions, conflict resolution

#### `/notebooks/6-evolution_with_spark.ipynb` - Schema & Partition Evolution ✨
- **Environment**: Spark Docker container
- **Focus**: Schema evolution, partition evolution, backward compatibility
- **Features**: Clean, modular functions with comprehensive error handling

#### `/notebooks/7-time_travel_and_rollback.ipynb` - Versioning & Time Travel
- **Environment**: Spark Docker container
- **Focus**: Historical data access, snapshot management

#### `/notebooks/8-metadata_inspection.ipynb` - Table Metadata
- **Environment**: Spark Docker container  
- **Focus**: Table history, files, partitions analysis

#### `/notebooks/9-iceberg_architecture_layers.ipynb` - Advanced Architecture
- **Environment**: Spark Docker container
- **Focus**: Deep dive into Iceberg internals and optimization

## 🗂️ File Structure

```
play_iceberg/
├── docker-compose.yml                    # Infrastructure setup
├── setup.sh                              # Main setup script
├── pyproject.toml                        # Python dependencies (local uv)
├── uv.lock                               # Dependency lock file
├── infra/                                # Infrastructure configuration
│   ├── spark/
│   │   └── spark-defaults.conf           # Spark configuration (Docker)
│   └── flink/
│       ├── flink-conf.yaml               # Flink configuration (Docker)
│       ├── flink-iceberg-example.sql     # Flink SQL example
│       ├── setup-flink-deps.sh           # Download Flink dependencies
│       ├── test-flink-setup.sh           # Test Flink setup
│       └── flink-lib/                    # Flink JAR dependencies
├── notebooks/
│   # 🏠 Local Environment (uv + jupyter lab)
│   ├── 1-create_user_table.ipynb         # PyIceberg table creation
│   ├── 2-insert_with_polars.ipynb        # Polars data insertion
│   ├── 3-select_with_duckdb.ipynb        # DuckDB analytics
│   ├── 10-flink_iceberg_streaming.ipynb  # PyFlink stream processing
│   # 🐳 Spark Docker Environment (http://localhost:8888)
│   ├── 4-select_with_spark.ipynb         # Spark SQL querying
│   ├── 5-upsert_with_spark.ipynb         # ACID operations  
│   ├── 6-evolution_with_spark.ipynb      # Schema & partition evolution
│   ├── 7-time_travel_and_rollback.ipynb  # Versioning & time travel
│   ├── 8-metadata_inspection.ipynb       # Table metadata analysis
│   └── 9-iceberg_architecture_layers.ipynb # Advanced architecture
└── README.md                             # This file
```

## 🌐 Endpoints

- **Iceberg REST Catalog**: http://localhost:8181
- **MinIO S3 API**: http://localhost:9000
- **MinIO Console**: http://localhost:9001
- **Jupyter Lab**: http://localhost:8888
- **Flink Web UI**: http://localhost:8082
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
