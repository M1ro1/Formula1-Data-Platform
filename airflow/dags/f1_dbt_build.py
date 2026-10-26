import os
from pathlib import Path

import pendulum
from airflow.datasets import Dataset
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from include.dag_helpers import DEFAULT_ARGS, RAW_DATASET_URI

DBT_PROJECT_DIR = Path(
    os.getenv("F1_DBT_PROJECT_DIR") or os.getenv("DBT_PROJECT_PATH", "/opt/airflow/dbt")
)
DBT_PROFILES_FILE = Path(
    os.getenv("F1_DBT_PROFILES_FILE", str(DBT_PROJECT_DIR / "airflow_profiles" / "profiles.yml"))
)
DBT_EXECUTABLE = os.getenv("F1_DBT_EXECUTABLE", "/home/airflow/dbt_venv/bin/dbt")


project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_DIR,
)

profile_config = ProfileConfig(
    profile_name="f1_dwh",
    target_name="airflow",
    profiles_yml_filepath=DBT_PROFILES_FILE,
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE
)

concurrency_config = {
    "max_active_runs": 1,
    "max_active_tasks": 4,
}

f1_dbt_build = DbtDag(
    dag_id="f1_dbt_build",

    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    operator_args={"install_deps": True},

    schedule=[Dataset(RAW_DATASET_URI)],
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,

    **concurrency_config,
    default_args={**DEFAULT_ARGS, "retries": 1},
    tags=["f1", "dbt", "cosmos"],
    doc_md=__doc__,
)