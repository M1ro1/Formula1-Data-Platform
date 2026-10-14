from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from include.f1_client import F1Client

log = logging.getLogger(__name__)

STAGE = "STG_F1_RAW"


@dataclass(frozen=True)
class Entity:
    table: str   # RAW table name
    method: str  # F1Client method that fetches it
    level: str   # "global" | "season" | "round"  -> which parameters are required


ENTITIES: dict[str, Entity] = {
    "seasons":      Entity("RAW_SEASONS",      "seasons",      "global"),
    "drivers":      Entity("RAW_DRIVERS",      "drivers",      "global"),
    "constructors": Entity("RAW_CONSTRUCTORS", "constructors", "global"),
    "circuits":     Entity("RAW_CIRCUITS",     "circuits",     "global"),
    "races":        Entity("RAW_RACES",        "races",        "season"),
    "results":      Entity("RAW_RESULTS",      "results",      "round"),
    "qualifying":   Entity("RAW_QUALIFYING",   "qualifying",   "round"),
    "pit_stops":    Entity("RAW_PIT_STOPS",    "pit_stops",    "round"),
    "laps":         Entity("RAW_LAPS",         "laps",         "round"),
}


def _data_dir() -> Path:
    return Path(os.getenv("F1_DATA_DIR", "/opt/airflow/data"))


def _resolve(
    entity: str, season: int | None, round_: int | None
) -> tuple[Entity, int | None, int | None]:
    """Validate the entity/season/round combination and normalise to ints."""
    try:
        spec = ENTITIES[entity]
    except KeyError:
        raise ValueError(f"Unknown entity '{entity}'. Choose from: {', '.join(ENTITIES)}") from None

    needs_season = spec.level in ("season", "round")
    needs_round = spec.level == "round"
    if needs_season != (season is not None) or needs_round != (round_ is not None):
        raise ValueError(
            f"'{entity}' is a {spec.level}-level entity: "
            f"season is {'required' if needs_season else 'not allowed'}, "
            f"round is {'required' if needs_round else 'not allowed'}"
        )
    return spec, (int(season) if season is not None else None), (
        int(round_) if round_ is not None else None
    )


def _partition(entity: str, season: int | None, round_: int | None) -> str:
    """Relative path shared by the local file and the stage prefix."""
    if season is None:
        return f"{entity}/all"
    if round_ is None:
        return f"{entity}/season_{season}"
    return f"{entity}/season_{season}/round_{round_}"


def _sql_value(value: int | None) -> str:
    return "NULL" if value is None else str(int(value))


def _where(season: int | None, round_: int | None) -> str:
    """Partition predicate (values are validated ints, so f-strings are safe here)."""
    s = "SEASON IS NULL" if season is None else f"SEASON = {int(season)}"
    r = "ROUND IS NULL" if round_ is None else f"ROUND = {int(round_)}"
    return f"{s} AND {r}"


def _connect():
    # imported lazily so extract-only runs do not need the Snowflake driver
    from include.snowflake_conn import get_connection

    return get_connection()


def extract(
    entity: str,
    season: int | None = None,
    round_: int | None = None,
    client: F1Client | None = None,
) -> Path:
    """Fetch all pages from the API and write them as NDJSON (one page per line)."""
    spec, season, round_ = _resolve(entity, season, round_)
    client = client or F1Client()

    args = [a for a in (season, round_) if a is not None]
    pages = getattr(client, spec.method)(*args)

    path = _data_dir() / "raw" / _partition(entity, season, round_) / f"{entity}.ndjson"
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp = path.with_suffix(".tmp")
    n_pages = 0
    with tmp.open("w", encoding="utf-8") as f:
        for page in pages:
            f.write(json.dumps(page, ensure_ascii=False, separators=(",", ":")) + "\n")
            n_pages += 1
    tmp.replace(path)

    log.info("Extracted %s (season=%s, round=%s): %s page(s) -> %s", entity, season, round_, n_pages, path)
    return path


def upload_to_stage(
    path: Path,
    entity: str,
    season: int | None = None,
    round_: int | None = None,
    conn=None,
) -> str:
    """PUT the local file into the internal stage. Returns the stage prefix."""
    _, season, round_ = _resolve(entity, season, round_)
    prefix = f"{_partition(entity, season, round_)}/"

    own_conn = conn is None
    conn = conn or _connect()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"PUT file://{Path(path).resolve().as_posix()} @{STAGE}/{prefix} "
                "AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            )
        finally:
            cur.close()
    finally:
        if own_conn:
            conn.close()

    log.info("Uploaded %s to @%s/%s", Path(path).name, STAGE, prefix)
    return prefix


def load_to_raw(
    entity: str,
    season: int | None = None,
    round_: int | None = None,
    conn=None,
) -> int:
    """Replace the partition in the RAW table with what is in the stage. Returns the row count."""
    spec, season, round_ = _resolve(entity, season, round_)
    prefix = f"{_partition(entity, season, round_)}/"
    where = _where(season, round_)

    copy_sql = f"""
        COPY INTO {spec.table} (PAYLOAD, SEASON, ROUND, _SOURCE_FILE)
        FROM (
            SELECT $1, {_sql_value(season)}, {_sql_value(round_)}, METADATA$FILENAME
            FROM @{STAGE}/{prefix}
        )
        FILE_FORMAT = (FORMAT_NAME = 'FF_JSON')
        FORCE = TRUE
    """

    own_conn = conn is None
    conn = conn or _connect()
    try:
        cur = conn.cursor()
        try:
            cur.execute("BEGIN")
            try:
                cur.execute(f"DELETE FROM {spec.table} WHERE {where}")
                cur.execute(copy_sql)
                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise
            cur.execute(f"SELECT COUNT(*) FROM {spec.table} WHERE {where}")
            rows = int(cur.fetchone()[0])
        finally:
            cur.close()
    finally:
        if own_conn:
            conn.close()

    log.info("Loaded %s (season=%s, round=%s): %s row(s) in %s", entity, season, round_, rows, spec.table)
    return rows


def load_entity(
    entity: str,
    season: int | None = None,
    round_: int | None = None,
    client: F1Client | None = None,
    conn=None,
) -> dict[str, Any]:
    """extract -> upload_to_stage -> load_to_raw for a single partition."""
    path = extract(entity, season, round_, client=client)
    upload_to_stage(path, entity, season, round_, conn=conn)
    rows = load_to_raw(entity, season, round_, conn=conn)
    return {"entity": entity, "season": season, "round": round_, "rows": rows}


def rounds_for_season(season: int, client: F1Client | None = None) -> list[int]:
    """Round numbers of a season (also useful for Airflow dynamic task mapping)."""
    client = client or F1Client()
    return [int(race["round"]) for race in client.list_races(int(season))]


def load_season(
    entity: str, season: int, client: F1Client | None = None, conn=None
) -> list[dict[str, Any]]:
    """Load a whole season: every round for round-level entities, one partition otherwise."""
    spec, season, _ = _resolve(entity, season, 1 if ENTITIES[entity].level == "round" else None)
    client = client or F1Client()

    own_conn = conn is None
    conn = conn or _connect()
    try:
        if spec.level == "round":
            return [
                load_entity(entity, season, rnd, client=client, conn=conn)
                for rnd in rounds_for_season(season, client)
            ]
        return [load_entity(entity, season, None, client=client, conn=conn)]
    finally:
        if own_conn:
            conn.close()


def _main() -> None:
    parser = argparse.ArgumentParser(description="Load F1 API data into Snowflake RAW")
    parser.add_argument("entity", choices=ENTITIES)
    parser.add_argument("season", nargs="?", type=int, help="omit for global entities")
    parser.add_argument("--round", type=int, dest="round_", help="single round (round-level entities)")
    parser.add_argument("--extract-only", action="store_true", help="only write the local file")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    level = ENTITIES[args.entity].level

    if args.extract_only:
        extract(args.entity, args.season, args.round_)
    elif level == "round" and args.round_ is None:
        results = load_season(args.entity, args.season)
        print(f"Loaded {len(results)} round(s), {sum(r['rows'] for r in results)} row(s) total")
    else:
        print(load_entity(args.entity, args.season, args.round_))


if __name__ == "__main__":
    _main()