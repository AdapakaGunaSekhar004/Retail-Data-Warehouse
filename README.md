# Retail Data Warehouse

An end-to-end data warehouse pipeline built with PostgreSQL, dbt, Apache Airflow, and Metabase. The pipeline ingests raw retail sales data, transforms it into analytical models, and exposes business KPIs through a BI dashboard.

---

## Architecture
```
Raw CSV Data
     |
     v
PostgreSQL (Staging Layer)
     |
     v
dbt Models (Transformation Layer)
  |-- Staging Models   : clean and rename raw data
  |-- Mart Models      : business-ready aggregations
     |
     v
Apache Airflow (Orchestration)
     |
     v
Metabase (BI Dashboard)
```

---

## Tech Stack

| Tool           | Version | Purpose                   |
|----------------|---------|---------------------------|
| PostgreSQL     | 15      | Data storage and staging  |
| dbt-core       | 1.8.7   | Data transformation       |
| dbt-postgres   | 1.8.2   | dbt PostgreSQL adapter    |
| Apache Airflow | 2.9.3   | Pipeline orchestration    |
| Metabase       | Latest  | BI dashboards             |
| Docker         | -       | Container runtime         |

---

## Getting Started

### Prerequisites
- Docker Desktop
- Python 3.12
- WSL2 with Ubuntu 24.04 (Windows users)

### 1. Start Infrastructure
```bash
docker-compose up -d
```

### 2. Set Up Python Environment
```bash
python3 -m venv airflow_venv
source airflow_venv/bin/activate
pip install "apache-airflow==2.9.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt"
pip install apache-airflow-providers-postgres psycopg2-binary "dbt-postgres==1.8.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt"
```

### 3. Initialize Airflow
```bash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow webserver --port 8080 -D
airflow scheduler -D
```

### 4. Verify dbt Connection
```bash
cd retail_dw
dbt debug
```

### 5. Run the Pipeline
```bash
dbt run
dbt test
```

---

## Data Models

### Staging
- stg_sales - cleaned and renamed raw sales records

### Marts
- fct_sales - fact table with sales metrics
- dim_products - product dimension table

---

## Author

Guna Sekhar Adapaka
- GitHub: https://github.com/AdapakaGunaSekhar004
- LinkedIn: https://www.linkedin.com/in/guna-sekhar-adapaka-6903ab23b/
- Portfolio: https://adapakagunasekhar004.github.io/portfolioper/
