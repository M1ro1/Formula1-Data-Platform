import os
import sys
import tempfile
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow"))
os.environ["F1_DATA_DIR"] = tempfile.mkdtemp()

from include import f1_loader as L
from include.dag_helpers import build_replay_items


class FakeClient:
    def results(self, season, round_):
        yield {"total": "1", "RaceTable": {"Races": []}}


class FakeCur:
    def __init__(self, events):
        self.events = events

    def execute(self, sql):
        self.events.append("sql: " + " ".join(sql.split()))

    def fetchone(self):
        return (3,)

    def close(self):
        pass


class FakeConn:
    def __init__(self, events):
        self.events = events

    def cursor(self):
        return FakeCur(self.events)

    def close(self):
        pass


def test_archive_key_mirrors_partition_layout():
    assert L._archive_key("results", 2023, 1) == "raw/results/season_2023/round_1/results.ndjson"
    assert L._archive_key("races", 2023, None) == "raw/races/season_2023/races.ndjson"
    assert L._archive_key("drivers", None, None) == "raw/drivers/all/drivers.ndjson"


def test_rounds_from_keys_filters_entity_and_season_and_sorts_numerically():
    keys = [
        "raw/results/season_2023/round_2/results.ndjson",
        "raw/results/season_2023/round_10/results.ndjson",
        "raw/results/season_2023/round_1/results.ndjson",
        "raw/results/season_2022/round_5/results.ndjson",
        "raw/qualifying/season_2023/round_3/qualifying.ndjson",
    ]
    assert L._rounds_from_keys(keys, "results", 2023) == [1, 2, 10]


def test_load_entity_archives_before_staging_when_enabled():
    events = []
    with mock.patch.object(L.f1_archive, "is_enabled", return_value=True), \
         mock.patch.object(L.f1_archive, "upload_file",
                           side_effect=lambda path, key: events.append("archive: " + key)):
        L.load_entity("results", 2023, 1, client=FakeClient(), conn=FakeConn(events))
    assert events[0] == "archive: raw/results/season_2023/round_1/results.ndjson"
    assert events[1].startswith("sql: PUT ")          # archive happens before the stage upload


def test_load_entity_skips_archive_when_disabled():
    events = []
    with mock.patch.object(L.f1_archive, "is_enabled", return_value=False), \
         mock.patch.object(L.f1_archive, "upload_file") as upload:
        L.load_entity("results", 2023, 1, client=FakeClient(), conn=FakeConn(events))
    upload.assert_not_called()


def test_replay_entity_uses_archive_and_never_the_api():
    events = []

    def fake_download(key, dest):
        events.append("download: " + key)
        Path(dest).parent.mkdir(parents=True, exist_ok=True)
        Path(dest).write_text("{}\n")
        return Path(dest)

    with mock.patch.object(L.f1_archive, "download_file", side_effect=fake_download), \
         mock.patch.object(L, "F1Client", side_effect=AssertionError("API must not be called")):
        result = L.replay_entity("results", 2023, 1, conn=FakeConn(events))

    assert result["source"] == "archive" and result["rows"] == 3
    assert events[0] == "download: raw/results/season_2023/round_1/results.ndjson"
    assert any("PUT " in e for e in events) and any("COPY INTO RAW_RESULTS" in e for e in events)


def test_replay_season_expands_archived_rounds():
    with mock.patch.object(L, "archived_rounds", return_value=[1, 2]), \
         mock.patch.object(L, "replay_entity",
                           side_effect=lambda e, s, r, conn=None: {"entity": e, "round": r, "rows": 1}):
        out = L.replay_season("results", 2023, conn=FakeConn([]))
    assert [r["round"] for r in out] == [1, 2]


def test_build_replay_items():
    items = build_replay_items(2023, ["drivers", "races", "results"], lambda e, s: [1, 2])
    assert [(i["entity"], i["season"], i["round"]) for i in items] == [
        ("drivers", None, None), ("races", 2023, None), ("results", 2023, 1), ("results", 2023, 2),
    ]
    try:
        build_replay_items(None, ["results"], lambda e, s: [1])
    except ValueError:
        pass
    else:
        raise AssertionError("season should be required for round-level entities")
