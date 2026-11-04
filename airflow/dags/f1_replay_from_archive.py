"""Reload RAW tables from the object-storage archive (manual trigger, no API calls).

Use it when a RAW table was dropped or damaged, or to rebuild a season without hitting the API.
Trigger with config:
  entities - comma-separated, e.g. "races,results" (global entities like drivers also work)
  season   - required for races/results/qualifying/pit_stops/laps; ignored for global entities

Round-level entities are expanded from the files that exist in the archive (dynamic task mapping,
one task per round). After a successful replay the RAW dataset is updated, so dbt runs again.
"""
import logging

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from include.dag_helpers import (
    DEFAULT_ARGS,
    LOAD_TASK_KWARGS,
    RAW_DATASET_URI,
    build_replay_items,
    parse_entities,
    replay_partition,
    summarize_results,
)
from include.f1_loader import archived_rounds

log = logging.getLogger(__name__)

RAW_DATASET = Dataset(RAW_DATASET_URI)

@dag(
    dag_id="f1_replay_from_archive",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "entities": Param(
            "races,results",
            type="string",
            description="Comma-separated entities to replay from the archive",
        ),
        "season": Param(
            2023,
            type=["null", "integer"],
            description="Season (needed for season/round-level entities)",
        ),
    },
    tags=["f1", "ingestion", "replay"],
    doc_md=__doc__,
)
def f1_replay_from_archive():
    @task
    def plan(**context) -> list[dict]:
        params = context["params"]
        season = int(params["season"]) if params.get("season") is not None else None
        entities = parse_entities(params["entities"])
        items = build_replay_items(season, entities, archived_rounds)
        if not items:
            raise AirflowFailException("Nothing found in the archive for these parameters")
        log.info("Planned %s partition(s) to replay: %s", len(items), entities)
        return items

    @task(**LOAD_TASK_KWARGS)
    def replay_one(item: dict) -> dict:
        return replay_partition(item)

    @task(outlets=[RAW_DATASET])
    def summarize(results) -> dict:
        return summarize_results(results)

    results = replay_one.expand(item=plan())
    summarize(results)

f1_replay_from_archive()