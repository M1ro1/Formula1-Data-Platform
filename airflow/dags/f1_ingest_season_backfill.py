import logging

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

from include.dag_helpers import (
    DEFAULT_ARGS,
    RAW_DATASET_URI,
    LOAD_TASK_KWARGS,
    build_backfill_items,
    load_partition,
    parse_entities,
    summarize_results,
)
from include.f1_loader import rounds_for_season

log = logging.getLogger(__name__)


RAW_DATASET = Dataset(RAW_DATASET_URI)


@dag(
    dag_id="f1_ingest_season_backfill",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "season": Param(2023, type="integer", minimum=1950, maximum=2100, description="Season to load"),
        "entities": Param(
            "races,results",
            type="string",
            description="Comma-separated: races, results, qualifying, pit_stops, laps",
        ),
    },
    tags=["f1", "ingestion", "backfill"],
    doc_md=__doc__,
)
def f1_ingest_season_backfill():
    @task
    def plan(**context) -> list[dict]:
        params = context["params"]
        season = int(params["season"])
        entities = parse_entities(params["entities"])
        items = build_backfill_items(season, entities, rounds_for_season)
        if not items:
            raise AirflowFailException(f"Nothing to load for season {season} (no rounds found)")
        log.info("Planned %s partition(s) for season %s: %s", len(items), season, entities)
        return items

    @task(**LOAD_TASK_KWARGS)
    def load_one(item: dict) -> dict:
        return load_partition(item)

    @task(outlets=[RAW_DATASET])
    def summarize(results) -> dict:
        return summarize_results(results)

    results = load_one.expand(item=plan())
    summarize(results)


f1_ingest_season_backfill()
