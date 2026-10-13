"""Small client for the Jolpica-F1 API (Ergast-compatible).

Features
--------
* pagination via ``limit`` / ``offset`` (generator of raw ``MRData`` pages)
* retries with exponential backoff for network errors and 429/5xx responses
* respects the ``Retry-After`` header
* client-side throttling (minimum pause between requests)

Check the Jolpica documentation for the current rate limits and tune
``F1_API_MIN_INTERVAL`` accordingly.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.jolpi.ca/ergast/f1"


class F1ApiError(RuntimeError):
    """The API could not be reached or returned an unexpected payload."""


class F1Client:
    def __init__(
        self,
        base_url: str | None = None,
        min_interval: float | None = None,
        timeout: float = 30.0,
        max_retries: int = 5,
        backoff_factor: float = 2.0,
        page_limit: int = 100,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = (base_url or os.getenv("F1_API_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        if min_interval is None:
            min_interval = float(os.getenv("F1_API_MIN_INTERVAL", "0.5"))
        self.min_interval = min_interval
        self.timeout = timeout
        self.page_limit = page_limit
        self.session = session or self._build_session(max_retries, backoff_factor)
        self._last_request_at = 0.0

    # ------------------------------------------------------------------ http
    @staticmethod
    def _build_session(max_retries: int, backoff_factor: float) -> requests.Session:
        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
            raise_on_status=False,  # we raise our own error after retries are exhausted
        )
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({"User-Agent": "f1-data-warehouse/0.1 (pet project)"})
        return session

    def _throttle(self) -> None:
        wait = self.min_interval - (time.monotonic() - self._last_request_at)
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.monotonic()

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}/{endpoint.strip('/')}.json"
        self._throttle()
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            body = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise F1ApiError(f"GET {url} failed: {exc}") from exc
        try:
            return body["MRData"]
        except (KeyError, TypeError) as exc:
            raise F1ApiError(f"Unexpected payload from {url}: no 'MRData' key") from exc

    # ------------------------------------------------------------ pagination
    def iter_pages(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        """Yield every page (the raw ``MRData`` object) of an endpoint."""
        offset = 0
        while True:
            page_params = {**(params or {}), "limit": self.page_limit, "offset": offset}
            data = self._get(endpoint, page_params)
            yield data

            total = int(data.get("total", 0))
            step = int(data.get("limit", self.page_limit)) or self.page_limit
            offset += step
            log.debug("%s: fetched up to offset %s of %s", endpoint, min(offset, total), total)
            if offset >= total:
                break

    # --------------------------------------------------------------- helpers
    @staticmethod
    def _url_formatter(resource: str, season: int | str | None, round_: int | str | None = None) -> str:
        if round_ is not None and season is None:
            raise ValueError("'round_' requires 'season'")
        parts = []
        if season is not None:
            parts.append(str(season))
        if round_ is not None:
            parts.append(str(round_))
        parts.append(resource)
        return "/".join(parts)

    # ------------------------------------------------------ entity endpoints
    def seasons(self) -> Iterator[dict[str, Any]]:
        return self.iter_pages("seasons")

    def drivers(self, season: int | None = None) -> Iterator[dict[str, Any]]:
        return self.iter_pages(self._url_formatter("drivers", season))

    def constructors(self, season: int | None = None) -> Iterator[dict[str, Any]]:
        return self.iter_pages(self._url_formatter("constructors", season))

    def circuits(self, season: int | None = None) -> Iterator[dict[str, Any]]:
        return self.iter_pages(self._url_formatter("circuits", season))

    def races(self, season: int) -> Iterator[dict[str, Any]]:
        return self.iter_pages(self._url_formatter("races", season))

    def results(self, season: int, round_: int) -> Iterator[dict[str, Any]]:
        return self.iter_pages(self._url_formatter("results", season, round_))

    def qualifying(self, season: int, round_: int) -> Iterator[dict[str, Any]]:
        return self.iter_pages(self._url_formatter("qualifying", season, round_))

    def pit_stops(self, season: int, round_: int) -> Iterator[dict[str, Any]]:
        return self.iter_pages(self._url_formatter("pitstops", season, round_))

    def laps(self, season: int, round_: int) -> Iterator[dict[str, Any]]:
        return self.iter_pages(self._url_formatter("laps", season, round_))

    def list_races(self, season: int) -> list[dict[str, Any]]:
        """All races of a season (round, name, date, circuit). Handy for DAG task mapping."""
        races: list[dict[str, Any]] = []
        for page in self.races(season):
            races.extend(page["RaceTable"]["Races"])
        return races


if __name__ == "__main__":
    # Smoke test:  python -m include.f1_client
    logging.basicConfig(level=logging.INFO)
    client = F1Client()
    races = client.list_races(2023)
    print(f"2023 season: {len(races)} races")
    if races:
        print("First race:", races[0]["raceName"], races[0]["date"])
        first_page = next(client.results(2023, 1))
        print("Round 1 results, total records reported by API:", first_page["total"])
