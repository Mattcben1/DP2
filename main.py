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
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

# ---------- Config ----------
DEV_MODE = False  # flip to False for the full run (the one-line change)

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
OUTLIER_IQR_K = 3.0  # Task 4: weekly-commit bursts above Tukey's "far out" fence, Q3 + 3*IQR
SLOW_MERGE_DAYS = 180  # Task 4: slow-merge outliers; same 180 days dbt uses for is_stale

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


def _records_for(endpoint: str, spec: dict) -> tuple[pd.DataFrame, int, int]:
    """One row per record from every landed page of an endpoint, deduplicated on (repo, id).

    Returns (deduplicated rows, files read, records read before dedup)."""
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
    return df, len(files), len(rows)


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
        df, n_files, n_records = _records_for(endpoint, spec)
        batches[endpoint] = df
        log.info("%s: read %d files, %d records, %d unique records", endpoint, n_files, n_records, len(df))

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


@task(name="3-dbt-build", cache_policy=NO_CACHE)
def dbt_build() -> dict:
    """Task 3: run `dbt build` (models + tests); fail the flow if any node fails."""
    log = TaskLog("dbt_build")
    os.environ["DP2_DB_PATH"] = str(DB_PATH)  # profiles.yml reads this, so cwd doesn't matter
    runner = PrefectDbtRunner(
        settings=PrefectDbtSettings(project_dir=DBT_DIR, profiles_dir=DBT_DIR),
        raise_on_failure=False,  # we log every result to our own file first, then raise below
    )
    dbt_vars = json.dumps({"window_start": START_DATE})
    log.info("Running dbt build with vars %s", dbt_vars)
    res = runner.invoke(["build", "--vars", dbt_vars])

    statuses = {}
    failed = []
    for r in res.result.results:
        status = str(r.status)
        statuses[status] = statuses.get(status, 0) + 1
        log.info("%-5s %s %s (failures=%s)", status.upper(), r.node.resource_type, r.node.name, r.failures)
        if status in ("error", "fail"):
            failed.append(f"{r.node.name}: {r.message}")

    log.info("dbt build finished: %s", statuses)
    if not res.success:
        log.error("dbt build failed on %d node(s)", len(failed))
        raise RuntimeError("dbt build failed:\n" + "\n".join(failed))
    return statuses


# ---------- Task 4 helpers ----------
# raw table -> (staging model, SQL filter for rows staging drops on purpose; None = drops nothing)
STAGING_PAIRS = {
    "raw_commits": ("stg_commits", None),
    "raw_pull_requests": ("stg_pull_requests", None),
    "raw_pr_reviews": ("stg_pr_reviews", "payload->>'state' = 'PENDING'"),
    "raw_releases": ("stg_releases", "(payload->>'draft')::BOOLEAN"),
}


def _check_files_in_raw(con, log: TaskLog) -> list[str]:
    """Completeness 1: every distinct (repo, id) landed in data/raw/ is in its raw table."""
    failures = []

    # Independent tally: open every landed page and count records by the endpoint written in the file.
    tally = {}
    for path in RAW_DIR.rglob("*.json"):  # never matches *.json.tmp
        page = json.loads(path.read_text())
        tally[page["endpoint"]] = tally.get(page["endpoint"], 0) + len(page["data"])

    for endpoint, spec in RAW_TABLES.items():
        df, n_files, n_records = _records_for(endpoint, spec)
        n_tally = tally.get(endpoint, 0)
        log.info("%s: %d files; %d records by independent tally, %d read by the loader",
                 endpoint, n_files, n_tally, n_records)
        if n_tally != n_records:
            failures.append(f"{endpoint}: tally found {n_tally} records on disk but the loader read {n_records}")

        con.register("landed", df[["repo", "id"]])
        rows = con.execute(f"""
            SELECT l.repo,
                   count(*)    AS distinct_in_files,
                   count(t.id) AS found_in_raw,
                   list_slice(list(l.id) FILTER (WHERE t.id IS NULL), 1, 5) AS sample_missing
            FROM landed l
            LEFT JOIN {spec['table']} t ON t.repo = l.repo AND t.id = l.id
            GROUP BY l.repo
            ORDER BY l.repo
        """).fetchall()
        con.unregister("landed")

        for repo, n_distinct, n_found, sample in rows:
            missing = n_distinct - n_found
            log.info("  %s %s: %d distinct in files, %d found in %s, %d missing",
                     repo, endpoint, n_distinct, n_found, spec["table"], missing)
            if missing:
                failures.append(f"{repo} {endpoint}: {missing} landed records missing from "
                                f"{spec['table']} (e.g. {sample})")
    return failures


def _check_raw_vs_staging(con, log: TaskLog) -> list[str]:
    """Completeness 2: per repo, raw rows = staging rows + rows staging drops on purpose."""
    failures = []
    for raw, (stg, dropped_filter) in STAGING_PAIRS.items():
        dropped = f"count(*) FILTER (WHERE {dropped_filter})" if dropped_filter else "0"
        rows = con.execute(f"""
            WITH r AS (SELECT repo, count(*) AS n_raw, {dropped} AS n_dropped FROM {raw} GROUP BY repo),
                 s AS (SELECT repo, count(*) AS n_stg FROM {stg} GROUP BY repo)
            SELECT coalesce(r.repo, s.repo), coalesce(n_raw, 0), coalesce(n_dropped, 0), coalesce(n_stg, 0)
            FROM r FULL OUTER JOIN s ON r.repo = s.repo
            ORDER BY 1
        """).fetchall()
        for repo, n_raw, n_dropped, n_stg in rows:
            ok = n_raw == n_stg + n_dropped
            log.info("%s: %s %d = %s %d + %d dropped -> %s",
                     repo, raw, n_raw, stg, n_stg, n_dropped, "OK" if ok else "MISMATCH")
            if not ok:
                failures.append(f"{repo}: {raw} has {n_raw} rows but {stg} has {n_stg} "
                                f"(+{n_dropped} intentionally dropped)")
    return failures


def _check_monthly_commits(con, log: TaskLog) -> list[str]:
    """Completeness 3: every repo has commits in every complete month (the run month is exempt).

    A month with 0 commits AND 0 merged PRs is genuine inactivity, confirmed by a second endpoint:
    it is logged as a warning. 0 commits while PRs were merged means commits were lost: a failure.
    """
    failures = []
    rows = con.execute("""
        SELECT repo,
               count(*)     AS complete_months,
               min(commits) AS fewest_commits,
               list(strftime(month, '%Y-%m') ORDER BY month)
                   FILTER (WHERE commits = 0 AND prs_merged = 0) AS inactive_months,
               list(strftime(month, '%Y-%m') ORDER BY month)
                   FILTER (WHERE commits = 0 AND prs_merged > 0) AS missing_commit_months
        FROM mart_repo_monthly
        WHERE NOT is_partial_month
        GROUP BY repo
        ORDER BY repo
    """).fetchall()
    seen = set()
    for repo, n_months, fewest, inactive, missing in rows:
        seen.add(repo)
        log.info("%s: %d complete months, fewest commits in a month = %d", repo, n_months, fewest)
        if inactive:
            log.warning("%s: no commits AND no merged PRs in %s: genuine inactivity, not data loss",
                        repo, inactive)
        if missing:
            failures.append(f"{repo}: no commits in complete month(s) {missing} although PRs were merged")
    for repo in sorted(set(REPOS) - seen):
        failures.append(f"{repo}: no rows at all in mart_repo_monthly")
    return failures


def _tukey_cutoff(values: pd.Series) -> float:
    """Tukey's upper fence: Q3 + K * IQR."""
    q1, q3 = values.quantile(0.25), values.quantile(0.75)
    return q3 + OUTLIER_IQR_K * (q3 - q1)


def _find_outliers(con, log: TaskLog) -> dict:
    """Outlier scan of time to merge and weekly commits. Flagged records are logged and kept."""
    # Run time = newest fetched_at, the same definition dbt uses, so reruns on the same data agree.
    run_time = con.execute("SELECT max(fetched_at)::TIMESTAMPTZ FROM raw_commits").fetchall()[0][0]
    log.info("Outlier rules (upper side only): merge time > %d days; weekly commits > Q3 + %.0f*IQR "
             "per repo. Run time = %s", SLOW_MERGE_DAYS, OUTLIER_IQR_K, run_time)

    # Series 1: hours to merge, read from the fact table (not recomputed); title only for the report.
    merges = con.execute("""
        SELECT f.repo, f.number, f.author_login, f.hours_to_merge, r.payload->>'title' AS title
        FROM fct_pull_requests f
        LEFT JOIN raw_pull_requests r ON r.repo = f.repo AND r.id = f.number::VARCHAR
        WHERE f.is_merged
    """).df()
    # Merge time uses one FIXED cutoff for every repo, not a per-repo statistical fence:
    # "open for half a year before merging" means the same thing in any project, and 180 days
    # matches dbt's is_stale threshold. (A log-scale Tukey fence was tried and rejected: merge times
    # span ~2 orders of magnitude between Q1 and Q3, so the fence landed at thousands of years.)
    merge_cut_h = SLOW_MERGE_DAYS * 24
    slow = merges[merges["hours_to_merge"] > merge_cut_h]

    # Series 2: commits per complete week, on a week spine so quiet weeks count as 0.
    weeks = con.execute("""
        WITH spine AS (
            SELECT r.repo, unnest(generate_series(date_trunc('week', ?::TIMESTAMPTZ),
                                                  ?::TIMESTAMPTZ - INTERVAL 7 DAY,
                                                  INTERVAL 7 DAY)) AS week
            FROM (SELECT DISTINCT repo FROM stg_commits) r
        ),
        counts AS (
            SELECT repo, date_trunc('week', committed_at) AS week,
                   count(*) AS commits, avg(author_is_bot::INT) AS bot_share
            FROM stg_commits
            GROUP BY ALL
        )
        SELECT s.repo, s.week, coalesce(c.commits, 0) AS commits, c.bot_share
        FROM spine s LEFT JOIN counts c ON c.repo = s.repo AND c.week = s.week
    """, [START_DATE, run_time]).df()
    # Weekly commits use a PER-REPO Tukey fence on RAW counts: commit volume differs a lot between
    # repos (about 9 vs 23 commits/week), so "a burst" has to be judged against each repo's own
    # typical week. Weekly counts are only mildly skewed, so no log transform is needed.
    week_cut = weeks.groupby("repo")["commits"].apply(_tukey_cutoff)
    bursts = weeks[weeks["commits"] > weeks["repo"].map(week_cut)]

    summary = {}
    for repo in sorted(merges["repo"].unique()):
        repo_slow = slow[slow["repo"] == repo]
        n_merged = (merges["repo"] == repo).sum()
        log.info("%s time to merge: cutoff %d h (%d days); flagged %d of %d merged PRs (%.1f%%)",
                 repo, merge_cut_h, SLOW_MERGE_DAYS, len(repo_slow), n_merged,
                 100 * len(repo_slow) / n_merged)
        for pr in repo_slow.sort_values("hours_to_merge", ascending=False).itertuples():
            log.info("    #%d %.0f h (%.0f days) by %s: %s",
                     pr.number, pr.hours_to_merge, pr.hours_to_merge / 24, pr.author_login, pr.title)

        repo_bursts = bursts[bursts["repo"] == repo]
        n_weeks = (weeks["repo"] == repo).sum()
        log.info("%s weekly commits: cutoff %.0f commits/week; flagged %d of %d complete weeks (%.1f%%)",
                 repo, week_cut[repo], len(repo_bursts), n_weeks, 100 * len(repo_bursts) / n_weeks)
        for wk in repo_bursts.sort_values("week").itertuples():
            log.info("    week of %s: %d commits, %.0f%% by bots",
                     wk.week.strftime("%Y-%m-%d"), wk.commits, 100 * wk.bot_share)

        summary[repo] = {"slow_merges": len(repo_slow), "burst_weeks": len(repo_bursts)}

    log.info("Decision: flagged records are KEPT (not capped or dropped). They are real events, "
             "not data errors; Task 5 uses medians and rates that are robust to them.")
    return summary


def _describe(con, log: TaskLog) -> pd.DataFrame:
    """Per-repo descriptive statistics. Durations come from the marts; medians and p90, no means."""
    stats = con.execute("""
        SELECT f.repo,
               count(*)                                    AS n_prs,
               count(*) FILTER (WHERE f.is_open)           AS n_open,
               count(f.hours_to_first_review)              AS n_reviewed,
               count(f.hours_to_merge)                     AS n_merged,
               median(f.hours_to_first_review)             AS first_review_median_h,
               quantile_cont(f.hours_to_first_review, 0.9) AS first_review_p90_h,
               median(f.hours_to_merge)                    AS merge_median_h,
               quantile_cont(f.hours_to_merge, 0.9)        AS merge_p90_h,
               any_value(h.commits_per_week)               AS commits_per_week,
               any_value(h.releases_per_year)              AS releases_per_year
        FROM fct_pull_requests f
        JOIN mart_health_inputs h ON h.repo = f.repo
        GROUP BY f.repo
        ORDER BY f.repo
    """).df().round(1)

    table = stats.to_string(index=False)
    print(table)
    log.info("Descriptive statistics (hours; PRs created in window):")
    for line in table.splitlines():
        log.info("  %s", line)
    return stats


@task(name="4-validate")
def validate() -> dict:
    """Task 4 (Python, no dbt): end-to-end completeness checks, outlier scan, descriptive stats."""
    log = TaskLog("validate")
    try:
        # Not read_only: dbt-duckdb keeps its read-write connection open in this same process, and
        # DuckDB refuses a second connection to the same file with a different configuration.
        # This task still only runs SELECTs.
        con = duckdb.connect(str(DB_PATH))
    except duckdb.IOException as exc:
        if "lock" in str(exc).lower():
            log.error("%s is locked: the DuckDB CLI (or another process) may have it open. "
                      "Close it and rerun.", DB_PATH.name)
        raise

    with con:
        con.execute("SET TimeZone = 'UTC'")  # same session zone as dbt, so week buckets line up
        failures = (_check_files_in_raw(con, log)
                    + _check_raw_vs_staging(con, log)
                    + _check_monthly_commits(con, log))
        outliers = _find_outliers(con, log)  # always run these two, so the log is complete
        stats = _describe(con, log)

    if failures:
        for problem in failures:
            log.error("FAILED: %s", problem)
        raise RuntimeError(f"Validation failed with {len(failures)} problem(s):\n" + "\n".join(failures))
    log.info("All completeness checks passed")
    return {"outliers": outliers, "stats": stats.to_dict(orient="records")}


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
