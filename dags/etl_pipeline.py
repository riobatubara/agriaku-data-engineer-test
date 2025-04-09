# etl_pipeline.py
import os
import glob
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text, inspect
from datetime import datetime

# Use paths relative to Docker container's structure
BASE_DIR = "/opt/airflow"
data_dir = os.path.join(BASE_DIR, "data")
output_dir = os.path.join(BASE_DIR, "output")
tracker_dir = os.path.join(BASE_DIR, "processed_files")
TRACKER_PATH = os.path.join(tracker_dir, "file_tracker.csv")

# PostgreSQL DB URL
db_url = "postgresql+psycopg2://agriakutest:agriakutest123@postgres:5432/agriakutestdb"
engine = create_engine(db_url)
tracking_table = "etl_file_tracking"

def log(msg):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    print(f"{timestamp} {msg}")

def ensure_tracker_exists():
    os.makedirs(tracker_dir, exist_ok=True)
    if not os.path.exists(TRACKER_PATH):
        pd.DataFrame(columns=["filename"]).to_csv(TRACKER_PATH, index=False)
        log(f"Created tracker file at {TRACKER_PATH}")

def read_tracker():
    return pd.read_csv(TRACKER_PATH)

def update_tracker(filename):
    try:
        tracker = read_tracker()
        if filename not in tracker["filename"].values:
            tracker.loc[len(tracker)] = [filename]
            tracker.to_csv(TRACKER_PATH, index=False)
            log(f"Updated tracker with: {filename}")
    except PermissionError:
        log(f"Warning: Permission denied when updating tracker for: {filename} — skipping.")
    except Exception as e:
        log(f"Error updating tracker for {filename}: {e}")

def get_latest_file(prefix):
    files = glob.glob(os.path.join(data_dir, f"{prefix}_*.csv"))
    if not files:
        return None
    return max(files, key=os.path.getctime)

def create_tables():
    with engine.connect() as conn:
        log("Creating base tables if they do not exist.")
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw_students (
                student_id TEXT,
                student_name TEXT
            );
            CREATE TABLE IF NOT EXISTS raw_courses (
                course_id TEXT,
                course_name TEXT
            );
            CREATE TABLE IF NOT EXISTS raw_enrollments (
                enrollment_id TEXT,
                student_id TEXT,
                course_id TEXT
            );
            CREATE TABLE IF NOT EXISTS raw_attendance (
                attendance_id TEXT,
                enrollment_id TEXT,
                semester_id INTEGER,
                week_id INTEGER,
                is_present BOOLEAN
            );
        """))

def check_table_exists(table_name):
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()

def load_csv_to_db(df, table_name):
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        log(f"Loaded table: {table_name}")
    except Exception as e:
        log(f"Error loading table {table_name}: {e}")
        raise

def validate_dataframe(df, name, required_columns=None, pk_column=None):
    if df.empty:
        raise ValueError(f"{name}: CSV is empty.")
    if required_columns:
        missing = set(required_columns) - set(df.columns)
        if missing:
            raise ValueError(f"{name}: Missing columns: {missing}")
    if pk_column and df[pk_column].isnull().any():
        raise ValueError(f"{name}: Null values in PK column '{pk_column}'")
    log(f"{name}: Validation passed")

def run_etl():
    try:
        ensure_tracker_exists()
        tracker = read_tracker()
        create_tables()

        file_map = {}
        for prefix in ["students", "courses", "enrollments", "attendance"]:
            latest = get_latest_file(prefix)
            if not latest:
                raise FileNotFoundError(f"No {prefix} file found")
            if latest in tracker["filename"].values:
                log(f"Skipping already processed file: {latest}")
                return
            file_map[prefix] = latest

        # Read files
        students_df = pd.read_csv(file_map["students"])
        courses_df = pd.read_csv(file_map["courses"])
        enrollments_df = pd.read_csv(file_map["enrollments"])
        attendance_df = pd.read_csv(file_map["attendance"])

        # Validate files
        validate_dataframe(students_df, "Students", ["student_id", "student_name"], "student_id")
        validate_dataframe(courses_df, "Courses", ["course_id", "course_name"], "course_id")
        validate_dataframe(enrollments_df, "Enrollments", ["enrollment_id", "student_id", "course_id"], "enrollment_id")
        validate_dataframe(attendance_df, "Attendance", ["attendance_id", "enrollment_id", "week_id", "semester_id", "is_present"], "attendance_id")

        # Load to RAW
        load_csv_to_db(students_df, "raw_students")
        load_csv_to_db(courses_df, "raw_courses")
        load_csv_to_db(enrollments_df, "raw_enrollments")
        load_csv_to_db(attendance_df, "raw_attendance")

        # Clean ODS
        students_df = students_df.drop_duplicates()
        courses_df = courses_df.drop_duplicates()
        enrollments_df = enrollments_df.drop_duplicates()
        attendance_df = attendance_df.drop_duplicates()
        attendance_df["is_present"] = attendance_df["is_present"].astype(bool)

        load_csv_to_db(students_df, "ods_students")
        load_csv_to_db(courses_df, "ods_courses")
        load_csv_to_db(enrollments_df, "ods_enrollments")
        load_csv_to_db(attendance_df, "ods_attendance")

        # Data Mart
        merged = attendance_df.merge(enrollments_df, on="enrollment_id")\
                              .merge(courses_df, on="course_id")

        merged["week_id"] = merged["week_id"].fillna(0).astype(int)
        merged["semester_id"] = merged["semester_id"].fillna(0).astype(int)

        summary = merged.groupby(["semester_id", "week_id", "course_name"]).agg(
            attendance_pct=("is_present", lambda x: round(100 * x.sum() / len(x), 2))
        ).reset_index()

        load_csv_to_db(summary, "report_weekly_attendance_pct")

        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"report_weekly_attendance_{timestamp_str}.csv"
        output_file = os.path.join(output_dir, output_filename)
        summary.to_csv(output_file, index=False)
        log(f"Exported report to {output_file}")

        # Mark processed
        for f in file_map.values():
            update_tracker(f)

        log("ETL pipeline completed successfully.")

    except Exception as e:
        log(f"ETL pipeline failed: {e}")
        raise

if __name__ == "__main__":
    run_etl()
