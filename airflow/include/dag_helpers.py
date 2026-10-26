from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Callable, Iterable

from include.f1_loader import ENTITIES, load_entity

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "f1",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}

RAW_DATASET_URI = "snowflake://f1_dwh/raw"

API_CONCURRENCY = 2

LOAD_TASK_KWARGS = {
    "max_active_tis_per_dag": API_CONCURRENCY,
    "map_index_template": "{{ my_custom_map_index }}",
}


def make_item(entity: str, season: int | None = None, round_: int | None = None) -> dict[str, Any]:
    return {"entity": entity, "season": season, "round": round_}


def label(item: dict[str, Any]) -> str:
    parts = [item["entity"]]
    if item.get("season") is not None:
        parts.append(str(item["season"]))
    if item.get("round") is not None:
        parts.append(f"R{item['round']}")
    return " ".join(parts)


def load_partition(item: dict[str, Any]) -> dict[str, Any]:
    try:
        from airflow.operators.python import get_current_context

        get_current_context()["my_custom_map_index"] = label(item)
    except Exception:
        pass
    return load_entity(item["entity"], item.get("season"), item.get("round"))


def summarize_results(results: Iterable[dict[str, Any]]) -> dict[str, int]:
    results = list(results)
    summary = {"partitions": len(results), "rows": sum(r["rows"] for r in results)}
    log.info("Loaded %(partitions)s partition(s), %(rows)s row(s) in total", summary)
    return summary


def parse_entities(value: str | Iterable[str]) -> list[str]:
    if isinstance(value, str):
        value = value.split(",")
    entities = [v.strip() for v in value if v and v.strip()]
    unknown = [e for e in entities if e not in ENTITIES]
    if unknown:
        raise ValueError(f"Unknown entities {unknown}. Choose from: {', '.join(ENTITIES)}")
    if not entities:
        raise ValueError("No entities given")
    return entities


def build_backfill_items(
    season: int, entities: list[str], rounds_fn: Callable[[int], list[int]]
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    rounds: list[int] | None = None
    for entity in entities:
        level = ENTITIES[entity].level
        if level == "global":
            raise ValueError(
                f"'{entity}' is a global entity - load it with the f1_ingest_reference DAG"
            )
        if level == "season":
            items.append(make_item(entity, season))
        else:
            if rounds is None:
                rounds = rounds_fn(season)
            items.extend(make_item(entity, season, r) for r in rounds)
    return items


def finished_rounds(races: list[dict[str, Any]], today: date | None = None) -> list[int]:
    """Rounds whose race day is strictly before today (race-day itself is not 'finished' yet)."""
    today = today or date.today()
    return sorted(
        int(r["round"]) for r in races if date.fromisoformat(r["date"]) < today
    )

def loaded_rounds(season: int, entities: list[str]) -> dict[str, set[int]]:
    """Rounds already present in RAW for each entity.

    Only pages that actually contain data count (API 'total' > 0), so a round that
    was fetched too early and came back empty is picked up again on the next run.
    """
    from include.snowflake_conn import get_connection

    season = int(season)
    loaded: dict[str, set[int]] = {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            for entity in entities:
                cur.execute(
                    f"SELECT DISTINCT ROUND FROM {ENTITIES[entity].table} "
                    f"WHERE SEASON = {season} AND TRY_TO_NUMBER(PAYLOAD:total::string) > 0"
                )
                loaded[entity] = {int(row[0]) for row in cur.fetchall() if row[0] is not None}
        finally:
            cur.close()
    finally:
        conn.close()
    return loaded


def build_incremental_items(
    season: int,
    finished: list[int],
    loaded: dict[str, set[int]],
    round_entities: list[str],
) -> list[dict[str, Any]]:
    """Always refresh `races`; per entity load missing rounds plus the latest loaded one
    (results can be corrected after the race)."""
    finished_set = set(finished)
    items = [make_item("races", season)]
    for entity in round_entities:
        done = loaded.get(entity, set()) & finished_set
        todo = finished_set - done
        if done:
            todo.add(max(done))
        items.extend(make_item(entity, season, r) for r in sorted(todo))
    return items
