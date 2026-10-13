"""Offline tests for F1Client pagination (no network needed)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow"))

import pytest  # noqa: E402

from include.f1_client import F1ApiError, F1Client  # noqa: E402


class FakeResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"{self.status_code}")

    def json(self):
        return self._payload


class FakeSession:
    """Serves `total` records, honouring limit/offset like the real API."""

    def __init__(self, total):
        self.total = total
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params)))
        limit, offset = params["limit"], params["offset"]
        return FakeResponse(
            {"MRData": {"total": str(self.total), "limit": str(limit), "offset": str(offset)}}
        )


def test_pagination_walks_all_pages():
    session = FakeSession(total=250)
    client = F1Client(base_url="http://x", min_interval=0, page_limit=100, session=session)
    pages = list(client.drivers(2023))
    assert len(pages) == 3
    assert [c[1]["offset"] for c in session.calls] == [0, 100, 200]
    assert session.calls[0][0] == "http://x/2023/drivers.json"


def test_empty_result_makes_single_call():
    session = FakeSession(total=0)
    client = F1Client(base_url="http://x", min_interval=0, session=session)
    assert len(list(client.seasons())) == 1


def test_round_requires_season():
    with pytest.raises(ValueError):
        F1Client._url_formatter("results", None, 1)


def test_http_error_becomes_f1apierror():
    class Bad:
        def get(self, *a, **k):
            return FakeResponse({}, status=500)

    client = F1Client(base_url="http://x", min_interval=0, session=Bad())
    with pytest.raises(F1ApiError):
        next(client.seasons())
