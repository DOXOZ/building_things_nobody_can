from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
import pandas as pd
import clickhouse_connect
from dotenv import load_dotenv
import logging

WORKSPACE_DIR = "/opt/airflow/workspace"
CSV_PATH = os.path.join(WORKSPACE_DIR, "output.csv")

# Load environment from .env in workspace (for ClickHouse Cloud)
load_dotenv(os.path.join(WORKSPACE_DIR, ".env"))

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")
CLICKHOUSE_SECURE = os.environ.get("CLICKHOUSE_SECURE", "false").lower() == "true"
CLICKHOUSE_INTERFACE = os.environ.get("CLICKHOUSE_INTERFACE", "http")
TABLE_NAME = os.environ.get("CLICKHOUSE_TABLE", "youtube_channels")


def upload_to_clickhouse(**context):
    logging.info(f"Connecting to ClickHouse host={CLICKHOUSE_HOST} port={CLICKHOUSE_PORT} secure={CLICKHOUSE_SECURE} interface={CLICKHOUSE_INTERFACE}")
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
        secure=CLICKHOUSE_SECURE,
        interface=CLICKHOUSE_INTERFACE,
    )

    logging.info(f"Ensuring table {TABLE_NAME} exists")
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            channel_link String,
            contact_info String,
            country String,
            subscribers Int64,
            videos Int64,
            views Int64,
            scraped_at DateTime DEFAULT now()
        ) ENGINE = MergeTree
        ORDER BY channel_link
        """
    )

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}. Ensure scrape task produced output.csv")

    logging.info(f"Reading CSV from {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)

    # Normalize columns to expected schema
    expected_cols = ["channel_link", "contact_info", "country", "subscribers", "videos", "views"]

    # If the shape is transposed (from dict-of-dicts without orient='index'), try to fix
    if not set(expected_cols).issubset(set(df.columns)):
        # Attempt to reconstruct: columns are channel links; rows correspond to fields
        # We'll transpose and then reset index to build expected columns
        df_t = df.transpose().reset_index()
        df_t.columns = ["channel_link", "contact_info", "country", "subscribers", "videos", "views"][: len(df_t.columns)]
        df = df_t

    # Ensure types and serialize list contact_info to string
    if "contact_info" in df.columns:
        def normalize_ci(x):
            if isinstance(x, list):
                return ";".join(x)
            if isinstance(x, str):
                return x.strip()
            return str(x)
        df["contact_info"] = df["contact_info"].apply(normalize_ci)

    for col in ["subscribers", "videos", "views"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")

    rows = df[["channel_link", "contact_info", "country", "subscribers", "videos", "views"]].values.tolist()

    logging.info(f"Inserting {len(rows)} rows into {TABLE_NAME}")
    if rows:
        client.insert(TABLE_NAME, rows, column_names=["channel_link", "contact_info", "country", "subscribers", "videos", "views"])
        logging.info("Insert completed")


def build_dag():
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 0,
    }

    dag = DAG(
        dag_id="scrape_and_upload",
        default_args=default_args,
        start_date=datetime(2026, 1, 10),
        schedule_interval=None,
        catchup=False,
        description="Scrape YouTube with DrissionPage, then upload results to ClickHouse",
    )

    scrape_task = BashOperator(
        task_id="run_scraper",
        bash_command=f"cd {WORKSPACE_DIR} && python scrape.py",
        dag=dag,
    )

    upload_task = PythonOperator(
        task_id="upload_to_clickhouse",
        python_callable=upload_to_clickhouse,
        dag=dag,
    )

    scrape_task >> upload_task
    return dag


globals()["scrape_and_upload"] = build_dag()
