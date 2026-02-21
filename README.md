# Telecom-Medallion-ETL-Pipeline

An Airflow-driven ETL pipeline using a Medallion Architecture. It ingests telecom Parquet data into Bronze, standardizes and rounds metrics in Silver, and manages SCD Type 2 subscriber history in Gold. Fully containerized via Docker, it leverages Pandas and SQLAlchemy for robust data processing and business reporting.

---

## 🚀 How to Run

### 1. Prerequisites
* **Docker** and **Docker Compose** installed.
* **WSL2** (if using Windows).

### 2. Setup Environment
Clone the repository and create your environment file:
```bash
git clone [https://github.com/Tartmo0097/Telecom-Medallion-ETL-Pipeline.git](https://github.com/Tartmo0097/Telecom-Medallion-ETL-Pipeline.git)
cd Telecom-Medallion-ETL-Pipeline
cp .env.example .env
mkdir -p data/subscribers data/usage
# Place your .parquet files into these folders
docker-compose up -d

Airflow UI: Open http://localhost:8080 (User: admin | Pass: admin).

Database: Connect to Postgres at localhost:5432 (DB: mydatabase).