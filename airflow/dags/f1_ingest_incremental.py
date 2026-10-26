import logging
from datetime import date

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models.param import Param
from include.dag_helpers import (
    DEFAULT_ARGS,
    LOAD_TASK_KWARGS,
    RAW_DATASET_URI,
    build_incremental_items,
    finished_rounds,
    load_partition,
    loaded_rounds,
    summarize_results,
)
from include.f1_client import F1Client

log = logging.getLogger(__name__)

ROUND_ENTITIES = ["results", "qualifying", "pit_stops", "laps"]


RAW_DATASET = Dataset(RAW_DATASET_URI)


@dag(
    dag_id="f1_ingest_incremental",
    schedule="0 6 * * 1",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "season": Param(
            None, type=["null", "integer"], description="Override season (default: current year)"
        ),
    },
    tags=["f1", "ingestion", "incremental"],
    doc_md=__doc__,
)
def f1_ingest_incremental():
    @task
    def find_work(**context) -> list[dict]:
        season = int(context["params"].get("season") or date.today().year)
        races = F1Client().list_races(season)
        finished = finished_rounds(races)
        loaded = loaded_rounds(season, ROUND_ENTITIES)
        items = build_incremental_items(season, finished, loaded, ROUND_ENTITIES)
        log.info(
            "Season %s: %s race(s) in calendar, %s finished, %s partition(s) to load",
            season, len(races), len(finished), len(items),
        )
        return items

    @task(**LOAD_TASK_KWARGS)
    def load_one(item: dict) -> dict:
        return load_partition(item)

    @task(outlets=[RAW_DATASET])
    def summarize(results) -> dict:
        return summarize_results(results)

    results = load_one.expand(item=find_work())
    summarize(results)


f1_ingest_incremental()
