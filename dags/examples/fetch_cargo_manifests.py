import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task, Asset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd


ENVIRONMENT = os.getenv("ENVIRONMENT", "DEV").upper()
DB_NAME = os.getenv("SNOWFLAKE_DATABASE", "DWH")
SCHEMA = f"{DB_NAME}.{ENVIRONMENT}"
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_STAGE = f"{SCHEMA}.ORDERS_STAGE"

S3_BUCKET = "event-demo-staging"
S3_PREFIX = "logistics/cargo_manifests"

TBL_MANIFESTS = f"{SCHEMA}.CARGO_MANIFESTS"
STG_MANIFESTS = f"{SCHEMA}.RAW_STG_CARGO_MANIFESTS"

AIRFLOW_HOME = Path(os.getenv("AIRFLOW_HOME", Path(__file__).parent.parent.parent))
DDL_FILE = (
    AIRFLOW_HOME
    / "include"
    / "sql"
    / "create_tables"
    / "logistics"
    / "cargo_manifests.sql"
)


@dag(
    dag_id="fetch_cargo_manifests",
    description="Fetch outbound cargo manifests for interplanetary shipments.",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "data-generation", "ecommerce", "logistics"],
    default_args={
        "owner": "demo",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def fetch_cargo_manifests():
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables_if_not_exists",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=DDL_FILE.read_text().format(
            tbl_manifests=TBL_MANIFESTS,
            stg_manifests=STG_MANIFESTS,
        ),
        split_statements=True,
        queue="compute-medium",
    )

    @task(queue="compute-heavy")
    def generate_cargo_manifests() -> dict:
        import random
        import uuid

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        spacecraft_rows = hook.get_records(
            f"SELECT spacecraft_id FROM {SCHEMA}.SPACECRAFT"
        )
        planet_rows = hook.get_records(f"SELECT planet_id FROM {SCHEMA}.PLANETS")

        spacecraft_ids = (
            [r[0] for r in spacecraft_rows] if spacecraft_rows else [1, 2, 3, 4, 5]
        )
        planet_ids = [r[0] for r in planet_rows] if planet_rows else list(range(1, 11))

        origin_hubs = [
            "Earth Distribution Hub",
            "Luna Logistics Center",
            "Mars Orbital Depot",
            "Ceres Freight Terminal",
        ]
        priority_classes = ["standard", "expedited", "critical"]

        now = datetime.utcnow()
        manifests = []
        for _ in range(15):
            departure_offset_hours = random.randint(2, 72)
            total_weight_kg = round(random.uniform(500.0, 25000.0), 2)
            total_volume_m3 = round(random.uniform(10.0, 450.0), 2)

            manifests.append(
                {
                    "manifest_id": str(uuid.uuid4()),
                    "shipment_id": str(uuid.uuid4()),
                    "spacecraft_id": random.choice(spacecraft_ids),
                    "origin_hub": random.choice(origin_hubs),
                    "destination_planet_id": random.choice(planet_ids),
                    "departure_ts": (
                        now + timedelta(hours=departure_offset_hours)
                    ).isoformat(),
                    "total_weight_kg": total_weight_kg,
                    "total_volume_m3": total_volume_m3,
                    "cargo_fill_pct": f"{random.uniform(35.0, 98.0):.1f}%",
                    "cargo_value_usd": round(random.uniform(10000.0, 5_000_000.0), 2),
                    "priority_class": random.choice(priority_classes),
                    "created_at": now.isoformat(),
                }
            )

        batch_id = now.strftime("%Y%m%d_%H%M%S")
        return {"batch_id": batch_id, "manifests": manifests}

    @task(queue="compute-heavy")
    def stage_to_s3(data: dict) -> dict:
        import io

        manifests = data["manifests"]
        df = pd.DataFrame(manifests)

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        s3_key = f"{S3_PREFIX}/data.parquet"
        S3Hook(aws_conn_id="aws_default").load_bytes(
            bytes_data=buffer.getvalue(),
            key=s3_key,
            bucket_name=S3_BUCKET,
            replace=True,
        )

        s3_path = f"s3://{S3_BUCKET}/{s3_key}"
        print(f"Staged {len(manifests)} cargo manifests to {s3_path}")

        return {
            "batch_id": data["batch_id"],
            "s3_path": s3_path,
            "record_count": len(manifests),
        }

    copy_to_staging = SQLExecuteQueryOperator(
        task_id="copy_to_staging",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            COPY INTO {STG_MANIFESTS}
            FROM @{SNOWFLAKE_STAGE}/{S3_PREFIX}/
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = TRUE;
        """,
        queue="compute-medium",
    )

    insert_to_manifests = SQLExecuteQueryOperator(
        task_id="insert_to_manifests",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            INSERT INTO {TBL_MANIFESTS}
            SELECT * FROM {STG_MANIFESTS};
            TRUNCATE TABLE {STG_MANIFESTS};
        """,
        queue="compute-medium",
    )

    @task(outlets=[Asset("cargo_manifests_updated")])
    def publish_asset(s3_info: dict):
        print("=" * 60)
        print("CARGO MANIFEST INGESTION COMPLETE")
        print("=" * 60)
        print(f"Batch ID: {s3_info['batch_id']}")
        print(f"Records: {s3_info['record_count']}")
        print(f"S3 Path: {s3_info['s3_path']}")
        print("=" * 60)

    data = generate_cargo_manifests()
    s3_info = stage_to_s3(data)

    create_tables >> data
    s3_info >> copy_to_staging >> insert_to_manifests >> publish_asset(s3_info)


fetch_cargo_manifests()
