# Agriaku Data Engineer Test

Created as part of the **Senior Data Engineer Technical Test** at Agriaku. Built with using Apache Airflow, PostgreSQL, and Docker Compose.

---

## 🗂 Project Structure

```
agriaku-data-engineer-test/
├── dags/                         # Airflow DAGs
│   ├── airflow_etl_dag.py        # Airflow DAG definition
│   └── etl_pipeline.py           # Main ETL DAG
├── data/                         # Source data only (CSV files)
├── output/                       # Data mart output CSV
├── processed_files/              # Tracks already processed files
│   └── file_tracker.csv
├── docker-compose.yaml           # Docker services
├── Dockerfile                    # Docker file
├── requirements.txt              # Python dependencies
├── README.md                     # Project instructions
├── start.sh                      # Bash script to run Dockerized services
├── stop.sh                       # Bash script to stop all Dockerized services
```

---

## 🧱 Data Layers

This ETL pipeline uses a layered data warehouse architecture:

- **Raw**: Timestamped CSVs (e.g., `attendance_20250409_103000.csv`) in `/data`.
- **Staging**: Parsed and cleaned records loaded into PostgreSQL staging tables.
- **ODS (Operational Data Store)**: Integrated tables joining relevant staging data.
- **Data Mart**: Aggregated metrics (e.g., attendance percentage) ready for reporting. Also exported to `/output`.

---

## 🚀 How to Run

### 1. Clone the Repository
```bash
git clone https://github.com/riobatubara/agriaku-data-engineer-test.git
cd agriaku-data-engineer-test
```

### 2. (Optional) Add Your CSV Files
Put raw timestamped CSVs into the `/data/` directory. Example filenames:
- `students_20250409_103000.csv`
- `courses_20250409_103000.csv`
- `enrollments_20250409_103000.csv`
- `attendance_20250409_103000.csv`

> Ensure filenames follow this pattern to enable file tracking.

### 3. Start Dockerized Services
```bash
./start.sh
```

### 4. Stop All Dockerized Services
```bash
./stop.sh
```

This launches:
- PostgreSQL on port `5432`
- Airflow webserver on port `8080`

### 5. Access Airflow Web UI
Go to: [http://localhost:8080](http://localhost:8080)
- Username: `admin`
- Password: `admin`

Enable and trigger the DAG named `etl_pipeline`.

> 💡 Tip: You can manually trigger or schedule the DAG from the UI.

### 6. View the Output
The final data mart CSV will be saved to:
```
/output/report_weekly_attendance_xxxxxxxx_xxxxxx.csv
```

---

## ⚙️ PostgreSQL Configuration
Credentials used in the pipeline:
```
POSTGRES_USER=agriakutest
POSTGRES_PASSWORD=agriakutest123
POSTGRES_DB=agriakutestdb
```

Tables are created and populated automatically by the ETL pipeline across these layers:
- `staging_students`, `staging_courses`, etc.
- `ods_enrollments`, `ods_attendance`
- `report_weekly_attendance_pct`

---

## ✅ Features
- Supports timestamped CSV ingestion
- Skips files already processed (via `file_tracker.csv`)
- Full ETL: raw → staging → ODS → data mart
- Data exported both to PostgreSQL and to `/output/*.csv`
- Modular Airflow DAG with exception handling and logs

---

## 🧪 To Do (optional improvements)
- Add DAG validation tests or unit tests
- Enable DAG to poll `/data` for new files automatically

---

