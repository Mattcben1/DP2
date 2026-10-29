"""DP2 package health pipeline: one Prefect flow, six tasks."""
import json
import logging
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx
import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from prefect.tasks import exponential_backoff

# ---------- Config ----------
DEV_MODE = True  # flip to False for the full run (the one-line change)

FULL_START_DATE = "2024-01-01"
DEV_START_DATE = "2026-01-01"
FULL_REPOS = ["scikit-learn/scikit-learn", "dask/dask", "plotly/plotly.py"]
DEV_REPOS = ["dask/dask"]

START_DATE = DEV_START_DATE if DEV_MODE else FULL_START_DATE
REPOS = DEV_REPOS if DEV_MODE else FULL_REPOS

TOKEN_ENV_VAR = "GH_PAT"  # name only; the value is read inside Task 1 and never logged
API_ROOT = "https://api.github.com"
PER_PAGE = 100
RATE_LIMIT_BUFFER = 25  # sleep until reset once this few requests remain

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
LOG_DIR = BASE_DIR / "logs"
PLOTS_DIR = BASE_DIR / "plots"
DB_PATH = BASE_DIR / "packages.duckdb"
DBT_DIR = BASE_DIR  # dbt_project.yml and profiles.yml live at the repo root


# ---------- Logging helper ----------
def _file_logger(task_name: str) -> logging.Logger:
    """Return a stdlib logger that appends to logs/<task_name>.log."""
    LOG_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger(f"dp2.{task_name}")
    if not logger.handlers:  # avoid duplicate handlers on retries / repeat calls
        handler = logging.FileHandler(LOG_DIR / f"{task_name}.log", mode="a")
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False  # don't echo to the root logger a second time
    return logger


class TaskLog:
    """Send each message to the Prefect run logger and to logs/<task_name>.log."""

    def __init__(self, task_name: str):
        self.prefect = get_run_logger()  # must be called inside a running flow/task
        self.file = _file_logger(task_name)

    def info(self, msg, *args):
        self.prefect.info(msg, *args)
        self.file.info(msg, *args)

    def warning(self, msg, *args):
        self.prefect.warning(msg, *args)
        self.file.warning(msg, *args)

    def error(self, msg, *args):
        self.prefect.error(msg, *args)
        self.file.error(msg, *args)


# ---------- Task 1 helpers: time ----------
def _iso(ts: datetime) -> str:
    """Format a timestamp the way GitHub does: 2026-01-01T00:00:00Z (UTC)."""
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_now() -> str:
    return _iso(datetime.now(timezone.utc))


# ---------- Task 1 helpers: HTTP ----------
class TransientHTTPError(Exception):
    """A GitHub failure worth retrying: 5xx, rate limits, network blips, truncated JSON."""


def _retry_if_transient(task, task_run, state) -> bool:
    """Prefect retry_condition_fn: retry only transient failures, never 401/404/422."""
    try:
        state.result()
    except TransientHTTPError:
        return True
    except Exception:
        return False
    return False


def _github_headers() -> dict:
    token = os.environ.get(TOKEN_ENV_VAR)
    if not token:
        raise RuntimeError(f"Environment variable {TOKEN_ENV_VAR} is not set")
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _url(path: str, **params) -> str:
    """Build the first-page URL for an endpoint, always with per_page=100."""
    return str(httpx.URL(path, params={**params, "per_page": PER_PAGE}))


def _sleep_until_reset(reset_epoch: str, log: TaskLog, reason: str) -> None:
    wait = max(0, int(reset_epoch) - time.time()) + 5  # +5 s margin for clock skew
    log.warning("%s: sleeping %.0f s until the rate limit resets", reason, wait)
    time.sleep(wait)


@task(
    name="fetch-page",
    retries=4,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),  # 10, 20, 40, 80 s
    retry_jitter_factor=0.5,
    retry_condition_fn=_retry_if_transient,
    cache_policy=NO_CACHE,
)
def fetch_page(client: httpx.Client, url: str) -> dict:
    """GET one page from GitHub. Returns the JSON body and the rel="next" URL (or None)."""
    log = TaskLog("extract")
    try:
        response = client.get(url)
    except httpx.TransportError as exc:
        raise TransientHTTPError(f"Network error for {url}: {exc}") from exc

    status = response.status_code
    remaining = response.headers.get("X-RateLimit-Remaining")
    reset = response.headers.get("X-RateLimit-Reset")

    if status in (403, 429):
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            log.warning("Secondary rate limit (%s) on %s: waiting %s s", status, url, retry_after)
            time.sleep(int(retry_after))
            raise TransientHTTPError(f"Secondary rate limit on {url}")
        if remaining == "0" and reset:
            _sleep_until_reset(reset, log, "Primary rate limit exhausted")
            raise TransientHTTPError(f"Primary rate limit on {url}")
        if status == 429 or "secondary rate limit" in response.text.lower():
            log.warning("Secondary rate limit (%s) on %s with no Retry-After: waiting 60 s", status, url)
            time.sleep(60)
            raise TransientHTTPError(f"Secondary rate limit on {url}")
    if status >= 500:
        raise TransientHTTPError(f"GitHub returned {status} for {url}")
    if status != 200:
        log.error("GitHub returned %s for %s: %s", status, url, response.text[:200])
        raise RuntimeError(f"GitHub returned {status} for {url}")  # retrying won't help

    try:
        data = response.json()
    except ValueError as exc:
        raise TransientHTTPError(f"Unparseable JSON from {url}") from exc

    if remaining is not None and reset is not None and int(remaining) <= RATE_LIMIT_BUFFER:
        _sleep_until_reset(reset, log, f"Only {remaining} requests left")

    return {"data": data, "next_url": response.links.get("next", {}).get("url")}


# ---------- Task 1 helpers: landing files ----------
def _page_path(endpoint_dir: Path, page: int) -> Path:
    return endpoint_dir / f"page_{page:04d}.json"


def _read_page(path: Path) -> dict | None:
    """Return a landed page, or None if it is missing or unreadable (so it gets re-fetched)."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


def _write_page(path: Path, doc: dict) -> None:
    """Write to a temp file, then rename: a crash mid-write never leaves a half-written page."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(doc))
    os.replace(tmp, path)


def _stop_before(cutoff: str):
    """Stop rule for PRs sorted by updated desc: stop once a page ends before the cutoff."""
    def stop(data: list) -> bool:
        return not data or data[-1]["updated_at"] < cutoff
    return stop


def _paginate(client, repo: str, endpoint: str, endpoint_dir: Path, first_url: str, stop=None) -> dict:
    """Land every page of one endpoint, skipping pages already on disk. Returns counts."""
    counts = {"pages_skipped": 0, "pages_fetched": 0, "records": 0}
    page = 1
    url = first_url

    # Resume: walk the pages already on disk and continue from the last one's next link.
    while True:
        doc = _read_page(_page_path(endpoint_dir, page))
        if doc is None:
            break
        counts["pages_skipped"] += 1
        counts["records"] += len(doc["data"])
        url = doc["next_url"]
        if url is None or (stop and stop(doc["data"])):
            return counts  # this endpoint was already finished
        page += 1

    while url:
        result = fetch_page(client, url)
        doc = {
            "repo": repo,
            "endpoint": endpoint,
            "url": url,
            "page": page,
            "fetched_at": _utc_now(),
            "next_url": result["next_url"],
            "data": result["data"],
        }
        _write_page(_page_path(endpoint_dir, page), doc)
        counts["pages_fetched"] += 1
        counts["records"] += len(result["data"])
        if stop and stop(result["data"]):
            break
        url = result["next_url"]
        page += 1
    return counts


def _load_watermarks(log: TaskLog) -> dict:
    """Read Task 2's _load_state: {(repo, endpoint): newest updated_at loaded}. Empty on first run."""
    if not DB_PATH.exists():
        return {}
    try:
        with duckdb.connect(str(DB_PATH), read_only=True) as con:
            rows = con.execute("SELECT repo, endpoint, max_updated_at FROM _load_state").fetchall()
    except duckdb.CatalogException:
        return {}  # database exists but Task 2 has not created _load_state yet
    except duckdb.IOException as exc:
        if "lock" in str(exc).lower():
            log.error("%s is locked: the DuckDB CLI (or another process) may have it open. "
                      "Close it and rerun.", DB_PATH.name)
        raise
    return {(repo, endpoint): _iso(ts) for repo, endpoint, ts in rows if ts is not None}


def _review_targets(pulls_dir: Path, window_start: str, cutoff: str) -> list[int]:
    """PR numbers needing review calls: created in the window and updated since the cutoff."""
    numbers = set()
    for path in sorted(pulls_dir.glob("page_*.json")):
        doc = _read_page(path)
        if doc is None:
            continue
        for pr in doc["data"]:
            if pr["created_at"] >= window_start and pr["updated_at"] >= cutoff:
                numbers.add(pr["number"])
    return sorted(numbers)


def _extract_repo(client, repo: str, watermarks: dict, window_start: str, log: TaskLog) -> dict:
    repo_dir = RAW_DIR / repo.replace("/", "__")
    base = f"{API_ROOT}/repos/{repo}"
    commits_since = watermarks.get((repo, "commits"), window_start)
    pulls_since = watermarks.get((repo, "pulls"), window_start)
    log.info("%s: commits since %s, PRs updated since %s", repo, commits_since, pulls_since)

    counts = {
        "commits": _paginate(client, repo, "commits", repo_dir / "commits",
                             _url(f"{base}/commits", since=commits_since)),
        "pulls": _paginate(client, repo, "pulls", repo_dir / "pulls",
                           _url(f"{base}/pulls", state="all", sort="updated", direction="desc"),
                           stop=_stop_before(pulls_since)),
        "releases": _paginate(client, repo, "releases", repo_dir / "releases",
                              _url(f"{base}/releases")),
    }

    targets = _review_targets(repo_dir / "pulls", window_start, pulls_since)
    log.info("%s: %d PRs need review calls", repo, len(targets))
    reviews = {"pages_skipped": 0, "pages_fetched": 0, "records": 0}
    for i, number in enumerate(targets, start=1):
        pr_counts = _paginate(client, repo, "reviews", repo_dir / "reviews" / f"pr_{number}",
                              _url(f"{base}/pulls/{number}/reviews"))
        for key in reviews:
            reviews[key] += pr_counts[key]
        if i % 100 == 0:
            log.info("%s: reviews for %d/%d PRs done", repo, i, len(targets))
    counts["reviews"] = reviews

    for endpoint, c in counts.items():
        log.info("%s %s: %d records (%d pages fetched, %d pages skipped from disk)",
                 repo, endpoint, c["records"], c["pages_fetched"], c["pages_skipped"])
    return counts


# ---------- Task 2 helpers ----------
# landing folder -> raw table, natural key, and the key path to the record's updated_at
RAW_TABLES = {
    "commits": {"table": "raw_commits", "key": "sha", "updated": ("commit", "committer", "date")},
    "pulls": {"table": "raw_pull_requests", "key": "number", "updated": ("updated_at",)},
    "reviews": {"table": "raw_pr_reviews", "key": "id", "updated": ("submitted_at",)},
    "releases": {"table": "raw_releases", "key": "id", "updated": ("published_at",)},
}

RAW_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    repo        VARCHAR,
    id          VARCHAR,
    updated_at  TIMESTAMP,
    fetched_at  TIMESTAMP,
    source_file VARCHAR,
    payload     JSON,
    PRIMARY KEY (repo, id)
)"""

LOAD_STATE_DDL = """
CREATE TABLE IF NOT EXISTS _load_state (
    repo           VARCHAR,
    endpoint       VARCHAR,
    max_updated_at TIMESTAMP,
    rows_loaded    BIGINT,
    loaded_at      TIMESTAMP,
    PRIMARY KEY (repo, endpoint)
)"""


def _dig(record: dict, path: tuple):
    """Follow a key path into nested JSON; None if any step is missing."""
    value = record
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def _to_utc_naive(series: pd.Series) -> pd.Series:
    """Parse GitHub ISO strings and store them as UTC TIMESTAMP (no time zone attached)."""
    return pd.to_datetime(series, utc=True).dt.tz_localize(None)


def _records_for(endpoint: str, spec: dict) -> tuple[pd.DataFrame, int]:
    """One row per record from every landed page of an endpoint, deduplicated on (repo, id)."""
    files = sorted(RAW_DIR.glob(f"*/{endpoint}/**/page_*.json"))  # never matches *.json.tmp
    rows = []
    for path in files:
        doc = _read_page(path)
        if doc is None:
            raise ValueError(f"Unreadable landing file: {path}")
        for record in doc["data"]:
            rows.append({
                "repo": doc["repo"],
                "id": str(record[spec["key"]]),
                "updated_at": _dig(record, spec["updated"]),
                "fetched_at": doc["fetched_at"],
                "page": doc["page"],
                "source_file": str(path.relative_to(BASE_DIR)),
                "payload": json.dumps(record),
            })

    columns = ["repo", "id", "updated_at", "fetched_at", "page", "source_file", "payload"]
    df = pd.DataFrame(rows, columns=columns)
    df["updated_at"] = _to_utc_naive(df["updated_at"])
    df["fetched_at"] = _to_utc_naive(df["fetched_at"])
    # A record can land on two pages; keep the newest copy (latest fetched_at, then highest page).
    df = df.sort_values(["fetched_at", "page"]).drop_duplicates(["repo", "id"], keep="last")
    return df, len(files)


def _row_count(con, table: str) -> int:
    # fetchall(), not fetchone(): DuckDB 2.0 streams results, and a half-read result blocks the connection
    return con.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0]


def _upsert(con, table: str, df: pd.DataFrame) -> None:
    """Insert new (repo, id) rows and replace existing ones, so reloading never duplicates."""
    con.register("batch", df)
    con.execute(f"""
        INSERT OR REPLACE INTO {table}
        SELECT repo, id, updated_at, fetched_at, source_file, payload::JSON
        FROM batch
    """)
    con.unregister("batch")


def _update_load_state(con, endpoint: str, table: str, loaded_at: datetime) -> None:
    """Watermark = newest updated_at in the whole raw table, so it never moves backwards."""
    con.execute(f"""
        INSERT OR REPLACE INTO _load_state
        SELECT repo, ?, max(updated_at), count(*), ?
        FROM {table}
        GROUP BY repo
    """, [endpoint, loaded_at])

@task(name="1-extract")
def extract() -> dict:
    """Task 1: land every page of commits, pulls, releases, and reviews into data/raw/."""
    log = TaskLog("extract")
    marker = RAW_DIR / "_LOADED"
    if marker.exists():
        log.info("Found _LOADED: everything on disk is in DuckDB, deleting data/raw and starting fresh")
        shutil.rmtree(RAW_DIR)
    elif RAW_DIR.exists():
        log.info("No _LOADED marker: resuming, pages already on disk will be skipped")
    else:
        log.info("No data/raw yet: first run, fetching the full window")

    watermarks = _load_watermarks(log)
    window_start = f"{START_DATE}T00:00:00Z"
    summary = {}
    with httpx.Client(headers=_github_headers(), timeout=30) as client:
        for repo in REPOS:
            summary[repo] = _extract_repo(client, repo, watermarks, window_start, log)

    requests = sum(c["pages_fetched"] for counts in summary.values() for c in counts.values())
    log.info("Extract finished: %d API requests this run", requests)
    return summary


@task(name="2-load")
def load() -> dict:
    """Task 2: upsert every landed page into raw_* tables, update _load_state, touch _LOADED."""
    log = TaskLog("load")
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"{RAW_DIR} does not exist: Task 1 has not landed any data")

    batches = {}
    for endpoint, spec in RAW_TABLES.items():
        df, n_files = _records_for(endpoint, spec)
        batches[endpoint] = df
        log.info("%s: read %d files, %d unique records", endpoint, n_files, len(df))

    counts = {}
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    try:
        with duckdb.connect(str(DB_PATH)) as con:
            for spec in RAW_TABLES.values():
                con.execute(RAW_TABLE_DDL.format(table=spec["table"]))
            con.execute(LOAD_STATE_DDL)

            # One transaction: if anything below fails, the connection closes without COMMIT,
            # DuckDB rolls everything back, and no _LOADED marker is written.
            con.execute("BEGIN TRANSACTION")
            for endpoint, spec in RAW_TABLES.items():
                table = spec["table"]
                before = _row_count(con, table)
                _upsert(con, table, batches[endpoint])
                after = _row_count(con, table)
                log.info("%s: %d rows before, %d rows after (%d new)", table, before, after, after - before)
                _update_load_state(con, endpoint, table, loaded_at)
                counts[table] = after
            con.execute("COMMIT")
    except duckdb.IOException as exc:
        if "lock" in str(exc).lower():
            log.error("%s is locked: the DuckDB CLI (or another process) may have it open. "
                      "Close it and rerun.", DB_PATH.name)
        raise

    (RAW_DIR / "_LOADED").touch()
    log.info("All files loaded and committed; created data/raw/_LOADED")
    return counts


@task(name="3-dbt-build")
def dbt_build() -> dict:
    log = TaskLog("dbt_build")
    log.info("STUB dbt_build: project_dir=%s", DBT_DIR.name)
    return {}


@task(name="4-validate")
def validate() -> dict:
    log = TaskLog("validate")
    log.info("STUB validate")
    return {}


@task(name="5-model")
def model() -> dict:
    log = TaskLog("model")
    log.info("STUB model")
    return {}


@task(name="6-plot")
def plot() -> dict:
    log = TaskLog("plot")
    log.info("STUB plot: out_dir=%s", PLOTS_DIR.name)
    return {}


# ---------- Flow ----------
@flow(name="bxp6yd-package-health-flow")
def package_health_flow() -> None:
    if DEV_MODE:
        TaskLog("flow").warning(
            "DEV_MODE is ON (start=%s, repos=%s). Set DEV_MODE = False before the final run.",
            START_DATE,
            REPOS,
        )
    extract()
    load()
    dbt_build()
    validate()
    model()
    plot()


if __name__ == "__main__":
    package_health_flow()
