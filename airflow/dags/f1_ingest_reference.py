import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from include.dag_helpers import (
    DEFAULT_ARGS,
    LOAD_TASK_KWARGS,
    RAW_DATASET_URI,
    load_partition,
    make_item,
    summarize_results,
)

REFERENCE_ENTITIES = ["seasons", "drivers", "constructors", "circuits"]

RAW_DATASET = Dataset(RAW_DATASET_URI)

@dag(
    dag_id="f1_ingest_reference",
    schedule="@weekly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["f1", "ingestion", "reference"],
    doc_md=__doc__,
)
def f1_ingest_reference():
    @task(**LOAD_TASK_KWARGS)
    def load_one(item: dict) -> dict:
        return load_partition(item)

    @task(outlets=[RAW_DATASET])
    def summarize(res) -> dict:
        return summarize_results(res)

    results = load_one.expand(item=[make_item(e) for e in REFERENCE_ENTITIES])
    summarize(results)


f1_ingest_reference()
