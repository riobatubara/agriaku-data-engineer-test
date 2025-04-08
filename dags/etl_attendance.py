# etl_pipeline.py
import os
import glob
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime

# Constants
data_dir = "/agriaku-data-engineer-test/data"
output_dir = "/agriaku-data-engineer-test/output"
tracker_dir = "/agriaku-data-engineer-test/processed_files"
TRACKER_PATH = os.path.join(tracker_dir, "file_tracker.csv")

db_url = "postgresql+psycopg2://agriakutest:agriakutest123@postgres:5432/agriakutestdb"
engine = create_engine(db_url)

tracking_table = "etl_file_tracking"


def ensure_tracker_exists():
    os.makedirs(tracker_dir, exist_ok=True)
    if not os.path.exists(TRACKER_PATH):
        pd.DataFrame(columns=["filename"]).to_csv(TRACKER_PATH, index=False)


def read_tracker():
    return pd.read_csv(TRACKER_PATH)


def update_tracker(filename):
    tracker = read_tracker()
    if filename not in tracker["filename"].values:
        tracker.loc[len(tracker)] = [filename]
        tracker.to_csv(TRACKER_PATH, index=False)


def get_latest_file(prefix):
    files = glob.glob(os.path.join(data_dir, f"{prefix}_*.csv"))
    if not files:
        return None
    return max(files, key=os.path.getctime)


def validate_data(df, required_columns, df_name):
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"{df_name} missing required column: {col}")


def create_tables():
    with engine.connect() as conn:
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


def load_csv_to_db(df, table_name):
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Loaded table: {table_name}")
    except Exception as e:
        print(f"Error loading table {table_name}: {e}")
        raise


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
                print(f"Skipping already processed file: {latest}")
                return
            file_map[prefix] = latest

        try:
            students_df = pd.read_csv(file_map["students"])
            courses_df = pd.read_csv(file_map["courses"])
            enrollments_df = pd.read_csv(file_map["enrollments"])
            attendance_df = pd.read_csv(file_map["attendance"])
        except Exception as e:
            print(f"Error reading CSV files: {e}")
            raise

        validate_data(students_df, ["student_id", "student_name"], "students_df")
        validate_data(courses_df, ["course_id", "course_name"], "courses_df")
        validate_data(enrollments_df, ["enrollment_id", "student_id", "course_id"], "enrollments_df")
        validate_data(attendance_df, ["attendance_id", "enrollment_id", "semester_id", "week_id", "is_present"], "attendance_df")

        # RAW LAYER
        load_csv_to_db(students_df, "raw_students")
        load_csv_to_db(courses_df, "raw_courses")
        load_csv_to_db(enrollments_df, "raw_enrollments")
        load_csv_to_db(attendance_df, "raw_attendance")

        # STAGING / ODS (cleaning, deduplication)
        students_df = students_df.drop_duplicates()
        courses_df = courses_df.drop_duplicates()
        enrollments_df = enrollments_df.drop_duplicates()
        attendance_df = attendance_df.drop_duplicates()

        attendance_df["is_present"] = attendance_df["is_present"].astype(bool)

        load_csv_to_db(students_df, "ods_students")
        load_csv_to_db(courses_df, "ods_courses")
        load_csv_to_db(enrollments_df, "ods_enrollments")
        load_csv_to_db(attendance_df, "ods_attendance")

        # DATA MART
        merged = attendance_df.merge(enrollments_df, on="enrollment_id", how="inner")\
                               .merge(courses_df, on="course_id", how="inner")

        merged['week_id'] = merged['week_id'].fillna(0).astype(int)
        merged['semester_id'] = merged['semester_id'].fillna(0).astype(int)

        summary = merged.groupby(["semester_id", "week_id", "course_name"]).agg(
            attendance_pct=("is_present", lambda x: round(100 * x.sum() / len(x), 2))
        ).reset_index()

        load_csv_to_db(summary, "fact_weekly_attendance_pct")

        output_file = os.path.join(output_dir, "fact_weekly_attendance_pct.csv")
        summary.to_csv(output_file, index=False)
        print(f"Exported data mart to: {output_file}")

        for f in file_map.values():
            update_tracker(f)

        print("ETL pipeline completed successfully.")

    except Exception as e:
        print(f"ETL pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run_etl()
