"""DP2 package health pipeline: one Prefect flow, six tasks."""
import json
import logging
import os
import shutil
import threading
import time
import warnings
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import httpx
import matplotlib
import pandas as pd

matplotlib.use("Agg")  # file-only backend: no display needed (WSL, Prefect workers)
import matplotlib.dates as mdates  # must come after matplotlib.use
import matplotlib.pyplot as plt
from pandas.errors import Pandas4Warning
from prefect import flow, get_run_logger, task, unmapped
from prefect.blocks.system import Secret
from prefect.cache_policies import NO_CACHE
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.tasks import exponential_backoff
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

# DuckDB's .df() calls DataFrame.drop(inplace=True) internally when converting TIMESTAMPTZ
# columns; pandas 3.1 deprecates that. It's DuckDB-internal (our code never uses inplace=).
# Filtered by category + exact message, not module: Python attributes the warning to OUR
# calling line, so module="duckdb" would not match.
warnings.filterwarnings("ignore", category=Pandas4Warning,
                        message=r"The inplace keyword in DataFrame\.drop")

# ---------- Config ----------
DEV_MODE = False  # True = dev run (2026-01-01, dask only); False = full run. The one-line change.

FULL_START_DATE = "2024-01-01"
DEV_START_DATE = "2026-01-01"
FULL_REPOS = ["scikit-learn/scikit-learn", "dask/dask", "plotly/plotly.py"]
DEV_REPOS = ["dask/dask"]

START_DATE = DEV_START_DATE if DEV_MODE else FULL_START_DATE
REPOS = DEV_REPOS if DEV_MODE else FULL_REPOS

TOKEN_ENV_VAR = "GH_PAT"  # name only; the value is read inside Task 1 and never logged
TOKEN_SECRET_BLOCK = "bxp6yd-pat"  # Prefect Secret block: the fallback when GH_PAT is unset (deployed runs)
API_ROOT = "https://api.github.com"
GRAPHQL_URL = f"{API_ROOT}/graphql"
INGESTION = "graphql"  # PRs + reviews via "graphql" or "rest"; commits and releases are always REST
GRAPHQL_REVIEWS_FIRST = 50  # reviews nested per PR; more than this -> follow-up queries for that PR
PER_PAGE = 100
RATE_LIMIT_BUFFER = 25  # all workers pause until reset once this few requests remain
EXTRACT_WORKERS = 3  # Task 1: repos fetched in parallel (1 = sequential, same code path)
OUTLIER_IQR_K = 3.0  # Task 4: weekly-commit bursts above Tukey's "far out" fence, Q3 + 3*IQR
SLOW_MERGE_DAYS = 180  # Task 4: slow-merge outliers; same 180 days dbt uses for is_stale

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
LOG_DIR = BASE_DIR / "logs"
PLOTS_DIR = BASE_DIR / "plots"
# Task 6: one fixed color per repo in every plot (Okabe-Ito, colorblind-safe). Keyed by repo name,
# not rank, so a repo keeps its color even if the ranking changes.
REPO_COLORS = {
    "scikit-learn/scikit-learn": "#E69F00",  # orange
    "dask/dask": "#0072B2",                  # blue
    "plotly/plotly.py": "#009E73",           # green
}
PLOT_DPI = 150
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
    """Send each message to the Prefect run logger and to logs/<task_name>.log.

    With repo set, every message starts with [owner/name], so the interleaved lines of
    concurrent repo workers can be told apart.
    """

    def __init__(self, task_name: str, repo: str | None = None):
        self.prefect = get_run_logger()  # must be called inside a running flow/task
        self.file = _file_logger(task_name)
        self.prefix = f"[{repo}] " if repo else ""

    def info(self, msg, *args):
        self.prefect.info(self.prefix + msg, *args)
        self.file.info(self.prefix + msg, *args)

    def warning(self, msg, *args):
        self.prefect.warning(self.prefix + msg, *args)
        self.file.warning(self.prefix + msg, *args)

    def error(self, msg, *args):
        self.prefect.error(self.prefix + msg, *args)
        self.file.error(self.prefix + msg, *args)


def _explain_lock(exc: duckdb.IOException, log: TaskLog) -> None:
    """Log the likely cause when DuckDB can't open the file because it is locked.

    Only adds context; the caller still re-raises, so the task fails either way.
    """
    if "lock" in str(exc).lower():
        log.error("%s is locked: the DuckDB CLI (or another process) may have it open. "
                  "Close it and rerun.", DB_PATH.name)


# ---------- Task 1 helpers: time ----------
def _iso(ts: datetime) -> str:
    """Format a timestamp the way GitHub does: 2026-01-01T00:00:00Z (UTC)."""
    if ts.tzinfo is not None:
        ts = ts.astimezone(UTC)
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_now() -> str:
    return _iso(datetime.now(UTC))


# ---------- Task 1 helpers: HTTP ----------
class TransientHTTPError(Exception):
    """A GitHub failure worth retrying: 5xx, rate limits, network blips, truncated JSON."""


class ApiOdometer:
    """Counts every HTTP request sent to GitHub during one flow run, retries included.

    Hooked into the httpx client (see extract), so each attempt is counted where it is sent;
    no call site has to remember to count. The lock keeps counts exact while the repo workers
    send requests from parallel threads.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        with self._lock:
            self.requests = 0
            self.by_status = {}
            self.by_resource = {}  # "core" (REST) or "graphql": separate GitHub budgets

    def on_request(self, request: httpx.Request) -> None:
        resource = RateLimiter._resource_for(request)
        with self._lock:
            self.requests += 1
            self.by_resource[resource] = self.by_resource.get(resource, 0) + 1

    def on_response(self, response: httpx.Response) -> None:
        with self._lock:
            code = response.status_code
            self.by_status[code] = self.by_status.get(code, 0) + 1

    def summary(self) -> str:
        with self._lock:
            no_response = self.requests - sum(self.by_status.values())  # network errors, timeouts
            statuses = ", ".join(f"{code}: {n}" for code, n in sorted(self.by_status.items()))
            resources = ", ".join(f"{name}: {n}" for name, n in sorted(self.by_resource.items()))
            return (f"{self.requests} GitHub API requests this flow run "
                    f"(by resource: {resources or 'none'}; "
                    f"by status: {statuses or 'none'}; no response: {no_response})")


ODOMETER = ApiOdometer()  # one per process; the flow resets it at the start of each run


def _repo_from_url(url: httpx.URL) -> str | None:
    """owner/name from a /repos/{owner}/{name}/... URL, so a hook can name the repo it serves."""
    parts = url.path.split("/")  # ["", "repos", owner, name, ...]
    return "/".join(parts[2:4]) if len(parts) >= 4 and parts[1] == "repos" else None


class RateLimiter:
    """One GitHub rate-limit budget shared by every repo worker, so they all pause together.

    Hooked into the httpx client like the odometer: before_request runs before every request is
    sent, on_response after every response. Budgets are keyed by GitHub's resource name (the
    X-RateLimit-Resource header): "core" for REST, "graphql" for POST /graphql (budget in points,
    not requests; our queries cost 1 point each, so reserving 1 per request still fits).
    """

    def __init__(self, buffer: int):
        self.buffer = buffer
        self._lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        with self._lock:
            self._budgets = {}  # resource -> {"remaining": int, "reset": epoch seconds}
            self._pause_until = 0.0  # epoch seconds; every worker waits until then

    @staticmethod
    def _resource_for(request: httpx.Request) -> str:
        # Before a response arrives, the URL is all we know: GraphQL has one endpoint.
        return "graphql" if request.url.path == "/graphql" else "core"

    def pause(self, seconds: float) -> None:
        """Make every worker wait (secondary rate limit, Retry-After). Never shortens a pause."""
        with self._lock:
            self._pause_until = max(self._pause_until, time.time() + seconds)

    def before_request(self, request: httpx.Request) -> None:
        resource = self._resource_for(request)
        started_pause = None
        with self._lock:
            now = time.time()
            budget = self._budgets.get(resource)
            if budget is not None and budget["remaining"] <= self.buffer:
                # The first worker to see the low budget starts one shared pause until the reset.
                # Forgetting the budget means the first response after the pause re-seeds it.
                self._pause_until = max(self._pause_until, budget["reset"] + 5)  # +5 s clock skew
                del self._budgets[resource]
                started_pause = budget["remaining"]
            elif budget is not None:
                budget["remaining"] -= 1  # reserve this request so parallel workers can't overshoot
            wait = max(0.0, self._pause_until - now)
        if started_pause is not None and wait > 0:
            TaskLog("extract", repo=_repo_from_url(request.url)).warning(
                "Only %d %s requests left: all workers sleeping %.0f s until the rate limit resets",
                started_pause, resource, wait)
        if wait > 0:
            time.sleep(wait)  # outside the lock, so other workers can reach their own wait

    def on_response(self, response: httpx.Response) -> None:
        remaining = response.headers.get("X-RateLimit-Remaining")
        reset = response.headers.get("X-RateLimit-Reset")
        if remaining is None or reset is None:
            return
        resource = response.headers.get("X-RateLimit-Resource", self._resource_for(response.request))
        remaining, reset = int(remaining), int(reset)
        with self._lock:
            budget = self._budgets.get(resource)
            if budget is None or reset > budget["reset"]:
                self._budgets[resource] = {"remaining": remaining, "reset": reset}  # new window
            elif reset == budget["reset"]:
                # Parallel responses arrive out of order; the budget only shrinks within a window.
                budget["remaining"] = min(budget["remaining"], remaining)
            # reset < budget["reset"]: a late response from the previous window, ignored


LIMITER = RateLimiter(RATE_LIMIT_BUFFER)  # shared by all repo workers; reset with the odometer


def _retry_if_transient(task, task_run, state) -> bool:
    """Prefect retry_condition_fn: retry only transient failures, never 401/404/422."""
    try:
        state.result()
    except TransientHTTPError:
        return True
    except Exception:  # noqa: BLE001 - any non-transient error must not be retried
        return False
    return False


def _github_headers(log: TaskLog | None = None) -> dict:
    """Request headers with the GitHub token: GH_PAT if set (local runs), else the Secret block.

    Only the token's SOURCE is logged, never its value. log is optional so the headers can be
    built outside a Prefect run (e.g. a quick check from the shell).
    """
    token = os.environ.get(TOKEN_ENV_VAR)
    source = f"environment ({TOKEN_ENV_VAR})"
    if not token:
        try:
            token = Secret.load(TOKEN_SECRET_BLOCK).get()
        except Exception as exc:
            raise RuntimeError(f"{TOKEN_ENV_VAR} is not set and Secret block "
                               f"'{TOKEN_SECRET_BLOCK}' could not be loaded") from exc
        source = f"Secret block '{TOKEN_SECRET_BLOCK}'"
    if log is not None:
        log.info("GitHub token source: %s", source)
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _url(path: str, **params) -> str:
    """Build the first-page URL for an endpoint, always with per_page=100."""
    return str(httpx.URL(path, params={**params, "per_page": PER_PAGE}))


@task(
    name="fetch-page",
    retries=4,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),  # 10, 20, 40, 80 s
    retry_jitter_factor=0.5,
    retry_condition_fn=_retry_if_transient,
    cache_policy=NO_CACHE,
)
def fetch_page(client: httpx.Client, repo: str, url: str) -> dict:
    """GET one page from GitHub. Returns the JSON body and the rel="next" URL (or None).

    Waiting for the rate limit happens in LIMITER's request hook, shared by all workers; this
    task only reports what a failed response means and starts a shared pause where needed.
    """
    log = TaskLog("extract", repo=repo)
    try:
        response = client.get(url)
    except httpx.TransportError as exc:
        raise TransientHTTPError(f"Network error for {url}: {exc}") from exc
    data = _check_response(response, url, log)
    return {"data": data, "next_url": response.links.get("next", {}).get("url")}


def _check_response(response: httpx.Response, url: str, log: TaskLog):
    """Turn a GitHub HTTP response into its JSON body, or raise. Shared by REST and GraphQL.

    TransientHTTPError (retried): rate limits, 5xx, unparseable JSON. RuntimeError (not
    retried): any other non-200, e.g. 401 bad token or 404 wrong repo.
    """
    status = response.status_code
    remaining = response.headers.get("X-RateLimit-Remaining")
    reset = response.headers.get("X-RateLimit-Reset")

    if status in (403, 429):
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            log.warning("Secondary rate limit (%s) on %s: all workers pausing %s s", status, url, retry_after)
            LIMITER.pause(int(retry_after))
            raise TransientHTTPError(f"Secondary rate limit on {url}")
        if remaining == "0" and reset:
            # LIMITER already recorded remaining=0, so the retry waits for the reset in its hook.
            log.warning("Primary rate limit exhausted on %s: retry will wait for the reset", url)
            raise TransientHTTPError(f"Primary rate limit on {url}")
        if status == 429 or "secondary rate limit" in response.text.lower():
            log.warning("Secondary rate limit (%s) on %s with no Retry-After: all workers pausing 60 s",
                        status, url)
            LIMITER.pause(60)
            raise TransientHTTPError(f"Secondary rate limit on {url}")
    if status >= 500:
        raise TransientHTTPError(f"GitHub returned {status} for {url}")
    if status != 200:
        log.error("GitHub returned %s for %s: %s", status, url, response.text[:200])
        raise RuntimeError(f"GitHub returned {status} for {url}")  # retrying won't help

    try:
        return response.json()
    except ValueError as exc:
        raise TransientHTTPError(f"Unparseable JSON from {url}") from exc


# GraphQL "errors" messages that mean GitHub failed on its side (not our query): worth a retry.
GRAPHQL_TRANSIENT_MESSAGES = ("something went wrong while executing your query", "timeout", "timedout")


@task(
    name="fetch-graphql",
    retries=4,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),  # same policy as fetch-page
    retry_jitter_factor=0.5,
    retry_condition_fn=_retry_if_transient,
    cache_policy=NO_CACHE,
)
def fetch_graphql(client: httpx.Client, repo: str, label: str, query: str, variables: dict) -> dict:
    """POST one GraphQL query. Returns its "data" and logs the query's rateLimit block.

    GraphQL reports most failures with HTTP 200 and an "errors" list, so the HTTP checks from
    _check_response are not enough. Any error means we never land the (partial) data.
    """
    log = TaskLog("extract", repo=repo)
    try:
        response = client.post(GRAPHQL_URL, json={"query": query, "variables": variables})
    except httpx.TransportError as exc:
        raise TransientHTTPError(f"Network error for GraphQL {label}: {exc}") from exc
    body = _check_response(response, f"GraphQL {label}", log)

    errors = body.get("errors") or []
    if errors:
        summary = "; ".join(f"{e.get('type', '?')}: {e.get('message', '')}" for e in errors)[:300]
        if any(e.get("type") == "RATE_LIMITED" for e in errors):
            log.warning("GraphQL rate limited on %s: all workers pausing 60 s", label)
            LIMITER.pause(60)
            raise TransientHTTPError(f"GraphQL rate limited on {label}")
        if any(m in e.get("message", "").lower() for e in errors for m in GRAPHQL_TRANSIENT_MESSAGES):
            log.warning("GraphQL server-side failure on %s (will retry): %s", label, summary)
            raise TransientHTTPError(f"GraphQL server-side failure on {label}")
        log.error("GraphQL errors on %s: %s", label, summary)
        raise RuntimeError(f"GraphQL errors on {label}: {summary}")  # bad query/repo: retrying won't help

    data = body["data"]
    rate = data.get("rateLimit") or {}
    log.info("GraphQL %s: cost %s, %s points left, resets %s",
             label, rate.get("cost"), rate.get("remaining"), rate.get("resetAt"))
    return data


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
        _check_mode(doc, "next_url", _page_path(endpoint_dir, page))
        counts["pages_skipped"] += 1
        counts["records"] += len(doc["data"])
        url = doc["next_url"]
        if url is None or (stop and stop(doc["data"])):
            return counts  # this endpoint was already finished
        page += 1

    while url:
        result = fetch_page(client, repo, url)
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
        _explain_lock(exc, log)
        raise
    return {(repo, endpoint): _iso(ts) for repo, endpoint, ts in rows if ts is not None}


def _needs_reviews(pr: dict, window_start: str, cutoff: str) -> bool:
    """A PR whose reviews we fetch: created in the window and updated since the cutoff."""
    return pr["created_at"] >= window_start and pr["updated_at"] >= cutoff


def _review_targets(pulls_dir: Path, window_start: str, cutoff: str) -> list[int]:
    """PR numbers needing review calls, from the landed PR pages."""
    numbers = set()
    for path in sorted(pulls_dir.glob("page_*.json")):
        doc = _read_page(path)
        if doc is None:
            continue
        for pr in doc["data"]:
            if _needs_reviews(pr, window_start, cutoff):
                numbers.add(pr["number"])
    return sorted(numbers)


def _check_mode(doc: dict, key: str, path: Path) -> None:
    """Refuse to resume from a page the other INGESTION mode landed (REST: next_url, GraphQL: next_cursor)."""
    if key not in doc:
        raise RuntimeError(f"{path} was landed by the other INGESTION mode: "
                           "delete data/raw or switch INGESTION back, then rerun")


# ---------- Task 1 helpers: GraphQL (PRs and reviews) ----------
# rateLimit is asked for in every query so each page logs its own cost and remaining points.
PULLS_QUERY = """
query($owner: String!, $name: String!, $after: String, $reviewsFirst: Int!) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequests(first: 100, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number state title createdAt updatedAt closedAt mergedAt
        author { __typename login }
        reviews(first: $reviewsFirst) {
          pageInfo { hasNextPage endCursor }
          nodes { databaseId state submittedAt author { __typename login } }
        }
      }
    }
  }
}"""

REVIEWS_QUERY = """
query($owner: String!, $name: String!, $number: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviews(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes { databaseId state submittedAt author { __typename login } }
      }
    }
  }
}"""

# GraphQL has a separate MERGED state; REST reports merged PRs as closed (with merged_at set).
PR_STATES = {"OPEN": "open", "CLOSED": "closed", "MERGED": "closed"}


def _rest_user(author: dict | None) -> dict | None:
    """GraphQL author -> REST user {login, type}. None = deleted account (REST's "ghost")."""
    if author is None:
        return None
    login, kind = author["login"], author["__typename"]  # kind: User, Bot, Organization, Mannequin
    if kind == "Bot" and not login.endswith("[bot]"):
        login += "[bot]"  # REST spells bot logins "dependabot[bot]"; GraphQL just "dependabot"
    return {"login": login, "type": kind}


def _rest_pr(node: dict) -> dict:
    """GraphQL PR node -> the REST /pulls fields stg_pull_requests reads."""
    return {
        "number": node["number"],
        "state": PR_STATES[node["state"]],  # KeyError on a new state: fail loudly, don't guess
        "title": node["title"],
        "created_at": node["createdAt"],
        "updated_at": node["updatedAt"],
        "closed_at": node["closedAt"],
        "merged_at": node["mergedAt"],
        "user": _rest_user(node["author"]),
    }


def _rest_review(node: dict, repo: str, number: int) -> dict:
    """GraphQL review node -> the REST /reviews fields stg_pr_reviews reads."""
    return {
        "id": node["databaseId"],  # the same integer id REST uses
        "state": node["state"],
        "submitted_at": node["submittedAt"],
        "user": _rest_user(node["author"]),
        "pull_request_url": f"{API_ROOT}/repos/{repo}/pulls/{number}",  # dbt parses the trailing number
    }


def _next_cursor(page_info: dict) -> str | None:
    return page_info["endCursor"] if page_info["hasNextPage"] else None


def _paginate_graphql(client, repo: str, endpoint: str, endpoint_dir: Path, label: str,
                      query: str, variables: dict, connection, adapt, stop=None, on_nodes=None) -> dict:
    """Cursor-paginate one GraphQL connection into landing pages. Same shape as _paginate.

    connection(data) picks the connection ({pageInfo, nodes}) out of a response, adapt(node) turns
    one node into a REST-shaped record, and on_nodes(nodes), if given, runs before the page is
    written (used to land each PR's nested reviews first). Each page stores the cursor it was
    fetched with and the next one, so a crashed run resumes from the last page on disk.
    """
    counts = {"pages_skipped": 0, "pages_fetched": 0, "records": 0}
    page = 1
    cursor = None  # None = first page

    # Resume: walk the pages already on disk and continue from the last one's next_cursor.
    while True:
        doc = _read_page(_page_path(endpoint_dir, page))
        if doc is None:
            break
        _check_mode(doc, "next_cursor", _page_path(endpoint_dir, page))
        counts["pages_skipped"] += 1
        counts["records"] += len(doc["data"])
        cursor = doc["next_cursor"]
        if cursor is None or (stop and stop(doc["data"])):
            return counts  # this connection was already finished
        page += 1

    while True:
        data = fetch_graphql(client, repo, f"{label} page {page}", query, {**variables, "after": cursor})
        conn = connection(data)
        records = [adapt(node) for node in conn["nodes"]]
        next_cursor = _next_cursor(conn["pageInfo"])
        if on_nodes:
            on_nodes(conn["nodes"])
        doc = {
            "repo": repo,
            "endpoint": endpoint,
            "source": "graphql",
            "query": label,
            "page": page,
            "fetched_at": _utc_now(),
            "cursor": cursor,
            "next_cursor": next_cursor,
            "rate_limit": data.get("rateLimit"),
            "data": records,
        }
        _write_page(_page_path(endpoint_dir, page), doc)
        counts["pages_fetched"] += 1
        counts["records"] += len(records)
        if next_cursor is None or (stop and stop(records)):
            break
        cursor = next_cursor
        page += 1
    return counts


def _extract_prs_graphql(client, repo: str, repo_dir: Path, window_start: str, pulls_since: str) -> tuple:
    """Land PRs and their reviews via GraphQL. Returns (pulls counts, reviews counts).

    Each PR page also brings the first GRAPHQL_REVIEWS_FIRST reviews of every PR on it, so
    reviews cost no extra requests except for PRs with more reviews than that (follow-ups).
    """
    log = TaskLog("extract", repo=repo)
    owner, name = repo.split("/")
    reviews_dir = repo_dir / "reviews"
    nested = set()  # PR numbers whose first review page was landed from a PR page this run

    def land_nested_reviews(nodes):
        # Written BEFORE the PR page: a PR page on disk guarantees its review pages exist too.
        for node in nodes:
            pr = _rest_pr(node)
            if not _needs_reviews(pr, window_start, pulls_since):  # same targeting as REST
                continue
            conn = node["reviews"]
            _write_page(_page_path(reviews_dir / f"pr_{pr['number']}", 1), {
                "repo": repo,
                "endpoint": "reviews",
                "source": "graphql",
                "query": "nested in pulls",
                "page": 1,
                "fetched_at": _utc_now(),
                "cursor": None,
                "next_cursor": _next_cursor(conn["pageInfo"]),
                "data": [_rest_review(r, repo, pr["number"]) for r in conn["nodes"]],
            })
            nested.add(pr["number"])

    pulls = _paginate_graphql(
        client, repo, "pulls", repo_dir / "pulls", "pulls", PULLS_QUERY,
        {"owner": owner, "name": name, "reviewsFirst": GRAPHQL_REVIEWS_FIRST},
        connection=lambda data: data["repository"]["pullRequests"],
        adapt=_rest_pr, stop=_stop_before(pulls_since), on_nodes=land_nested_reviews)

    # Follow-ups: each targeted PR's review folder already holds page 1 (from disk, so "skipped");
    # _paginate_graphql only fetches more when that page's next_cursor says there are more.
    targets = _review_targets(repo_dir / "pulls", window_start, pulls_since)
    reviews = {"pages_skipped": 0, "pages_fetched": 0, "records": 0}
    for number in targets:
        pr_counts = _paginate_graphql(
            client, repo, "reviews", reviews_dir / f"pr_{number}", f"reviews #{number}", REVIEWS_QUERY,
            {"owner": owner, "name": name, "number": number},
            connection=lambda data: data["repository"]["pullRequest"]["reviews"],
            adapt=lambda node, n=number: _rest_review(node, repo, n))
        for key in reviews:
            reviews[key] += pr_counts[key]
    reviews["pages_skipped"] -= len(nested)  # nested pages came with the PR pages, not from disk
    log.info("%d PRs need reviews: %d first pages nested in PR pages this run, %d follow-up queries",
             len(targets), len(nested), reviews["pages_fetched"])
    return pulls, reviews


@task(name="extract-repo", task_run_name="extract-{repo}", cache_policy=NO_CACHE)
def extract_repo(repo: str, client: httpx.Client, watermarks: dict, window_start: str) -> dict:
    """Land one repo's commits, pulls, releases and reviews. Mapped over REPOS by Task 1.

    Writes only under data/raw/<owner>__<name>/, so parallel repo workers never share a file.
    NO_CACHE: the httpx client can't be hashed for a cache key, and every run must refetch.
    """
    log = TaskLog("extract", repo=repo)
    repo_dir = RAW_DIR / repo.replace("/", "__")
    base = f"{API_ROOT}/repos/{repo}"
    commits_since = watermarks.get((repo, "commits"), window_start)
    pulls_since = watermarks.get((repo, "pulls"), window_start)
    log.info("commits since %s, PRs updated since %s", commits_since, pulls_since)

    counts = {
        "commits": _paginate(client, repo, "commits", repo_dir / "commits",
                             _url(f"{base}/commits", since=commits_since)),
        "releases": _paginate(client, repo, "releases", repo_dir / "releases",
                              _url(f"{base}/releases")),
    }

    if INGESTION == "graphql":
        counts["pulls"], counts["reviews"] = _extract_prs_graphql(
            client, repo, repo_dir, window_start, pulls_since)
    else:
        counts["pulls"] = _paginate(client, repo, "pulls", repo_dir / "pulls",
                                    _url(f"{base}/pulls", state="all", sort="updated", direction="desc"),
                                    stop=_stop_before(pulls_since))
        targets = _review_targets(repo_dir / "pulls", window_start, pulls_since)
        log.info("%d PRs need review calls", len(targets))
        reviews = {"pages_skipped": 0, "pages_fetched": 0, "records": 0}
        for i, number in enumerate(targets, start=1):
            pr_counts = _paginate(client, repo, "reviews", repo_dir / "reviews" / f"pr_{number}",
                                  _url(f"{base}/pulls/{number}/reviews"))
            for key in reviews:
                reviews[key] += pr_counts[key]
            if i % 100 == 0:
                log.info("reviews for %d/%d PRs done", i, len(targets))
        counts["reviews"] = reviews

    for endpoint, c in counts.items():
        log.info("%s: %d records (%d pages fetched, %d pages skipped from disk)",
                 endpoint, c["records"], c["pages_fetched"], c["pages_skipped"])
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


# ---------- Tasks 1-2 ----------
@task(name="1-extract")
def extract() -> dict:
    """Task 1: land every page of commits, pulls, releases, and reviews into data/raw/."""
    log = TaskLog("extract")
    if INGESTION not in ("rest", "graphql"):
        raise ValueError(f'INGESTION must be "rest" or "graphql", not {INGESTION!r}')
    log.info("Ingestion mode for PRs and reviews: %s (commits and releases: REST)", INGESTION)
    marker = RAW_DIR / "_LOADED"
    if marker.exists():
        log.info("Found _LOADED: everything on disk is in DuckDB, deleting data/raw and starting fresh")
        shutil.rmtree(RAW_DIR)
    elif RAW_DIR.exists():
        log.info("No _LOADED marker: resuming, pages already on disk will be skipped")
    else:
        log.info("No data/raw yet: first run, fetching the full window")

    watermarks = _load_watermarks(log)  # read once here; the repo workers never touch DuckDB
    window_start = f"{START_DATE}T00:00:00Z"
    # Limiter before odometer: a request that must wait is counted when it is actually sent.
    hooks = {"request": [LIMITER.before_request, ODOMETER.on_request],
             "response": [LIMITER.on_response, ODOMETER.on_response]}
    with httpx.Client(headers=_github_headers(log), timeout=30, event_hooks=hooks) as client:
        # One mapped extract-repo run per repo, on the flow's ThreadPoolTaskRunner
        # (max_workers=EXTRACT_WORKERS). One shared client: one set of hooks, one budget.
        futures = extract_repo.map(REPOS, client=unmapped(client),
                                   watermarks=unmapped(watermarks), window_start=unmapped(window_start))
        futures.wait()  # every repo finishes (and keeps its pages) even if another one fails

    summary, failed = {}, []
    for repo, future in zip(REPOS, futures):
        if future.state.is_completed():
            summary[repo] = future.result()
        else:
            failed.append(repo)
            log.error("[%s] extraction failed: %s", repo, future.state.message)
    if failed:
        raise RuntimeError(f"Extraction failed for {', '.join(failed)}; rerun to resume from disk")

    pages = sum(c["pages_fetched"] for counts in summary.values() for c in counts.values())
    log.info("Extract finished: %d pages fetched this run (%d requests sent, retries included)",
             pages, ODOMETER.requests)
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
    loaded_at = datetime.now(UTC).replace(tzinfo=None)
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
        _explain_lock(exc, log)
        raise

    (RAW_DIR / "_LOADED").touch()
    log.info("All files loaded and committed; created data/raw/_LOADED")
    return counts


# ---------- Task 3 ----------
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


# ---------- Tasks 4-6 shared helpers ----------
def _connect_db(log: TaskLog) -> duckdb.DuckDBPyConnection:
    """Open packages.duckdb for Tasks 4-6, in UTC like dbt's session.

    Not read_only: dbt-duckdb keeps its read-write connection open in this same process, and
    DuckDB refuses a second connection to the same file with a different configuration.
    """
    try:
        con = duckdb.connect(str(DB_PATH))
    except duckdb.IOException as exc:
        _explain_lock(exc, log)
        raise
    try:
        con.execute("SET TimeZone = 'UTC'")  # same session zone as dbt, so week buckets line up
    except Exception:
        con.close()  # the caller's `with` hasn't started yet, so close it here
        raise
    return con


def _show_table(log: TaskLog, title: str, df: pd.DataFrame, index: bool = False) -> None:
    """Print a table to the screen and write the same lines to the Prefect logger and log file."""
    table = df.to_string(index=index)
    print(f"{title}\n{table}")
    log.info("%s", title)
    for line in table.splitlines():
        log.info("  %s", line)


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
    log.info("Outlier rules (upper side only): merge time > %d days (fixed, all repos); weekly commits "
             "> Q3 + %.0f*IQR per repo on RAW weekly counts (no log transform). Run time = %s",
             SLOW_MERGE_DAYS, OUTLIER_IQR_K, run_time)

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
             "not data errors. Task 5's merge metric is a median, so slow merges barely move it; "
             "commit bursts do feed commit_trend_pct (a regression slope), noted in the README.")
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

    _show_table(log, "Descriptive statistics (hours; PRs created in window):", stats)
    return stats


@task(name="4-validate")
def validate() -> dict:
    """Task 4 (Python, no dbt): end-to-end completeness checks, outlier scan, descriptive stats."""
    log = TaskLog("validate")
    with _connect_db(log) as con:  # this task only runs SELECTs
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


# ---------- Task 5 helpers ----------
# Metric in mart_health_inputs -> (dimension, direction): +1 = higher is healthier, -1 = lower is.
HEALTH_METRICS = {
    "commit_trend_pct": ("activity_trend", +1),
    "median_hours_to_first_review": ("responsiveness", -1),
    "pct_reviewed_within_7d": ("responsiveness", +1),
    "merge_rate": ("throughput", +1),
    "median_hours_to_merge": ("throughput", -1),
    "pct_open_stale": ("backlog", -1),
    "releases_per_year": ("release_cadence", +1),
    "release_interval_cv": ("release_cadence", -1),
}

# Fixed before looking at the scores and not tuned to them (reasoning in the README).
DIMENSION_WEIGHTS = {
    "activity_trend": 0.25,
    "responsiveness": 0.25,
    "throughput": 0.20,
    "backlog": 0.15,
    "release_cadence": 0.15,
}


def _check_model_config(columns) -> None:
    """Raise on structural config errors. These are bugs in the setup, not problems in the data."""
    dimensions = {dim for dim, _ in HEALTH_METRICS.values()}
    problems = []
    if abs(sum(DIMENSION_WEIGHTS.values()) - 1) > 1e-9:
        problems.append(f"weights sum to {sum(DIMENSION_WEIGHTS.values()):.3f}, not 1")
    if len(HEALTH_METRICS) < 4:
        problems.append(f"only {len(HEALTH_METRICS)} metrics; at least 4 are required")
    if len(dimensions) < 3:
        problems.append(f"only {len(dimensions)} dimensions; at least 3 are required")
    if dimensions != set(DIMENSION_WEIGHTS):
        problems.append(f"metric dimensions {sorted(dimensions)} != weighted dimensions "
                        f"{sorted(DIMENSION_WEIGHTS)}")
    bad_direction = [m for m, (_, d) in HEALTH_METRICS.items() if d not in (1, -1)]
    if bad_direction:
        problems.append(f"direction must be +1 or -1 for {bad_direction}")
    missing = sorted(set(HEALTH_METRICS) - set(columns))
    if missing:
        problems.append(f"columns missing from mart_health_inputs: {missing}")
    if problems:
        raise ValueError("Invalid Task 5 config: " + "; ".join(problems))


def _signed_zscores(inputs: pd.DataFrame, log: TaskLog) -> pd.DataFrame:
    """Per metric: z-score across repos, times its direction, so higher is always healthier.

    Population sd (ddof=0): the repos compared are the whole population, not a sample.
    A NULL value gets z = 0 (neutral) with a warning; a metric with no spread gives everyone 0.
    """
    z = pd.DataFrame(index=inputs.index)
    for metric, (_, direction) in HEALTH_METRICS.items():
        x = inputs[metric].astype(float)
        for repo in x[x.isna()].index:
            log.warning("%s: %s is NULL; its z-score is set to 0 (neutral)", repo, metric)
        sd = x.std(ddof=0)  # pandas skips NULLs in mean and sd
        if pd.isna(sd) or sd == 0:
            z[metric] = 0.0
        else:
            z[metric] = (direction * (x - x.mean()) / sd).fillna(0.0)
    return z


def _rank(z: pd.DataFrame, weights: dict) -> pd.DataFrame:
    """Dimension subscore = mean signed z of its metrics; score = weighted sum; rank 1 = healthiest."""
    ranked = pd.DataFrame({
        dim: z[[m for m, (d, _) in HEALTH_METRICS.items() if d == dim]].mean(axis=1)
        for dim in weights
    })
    ranked["score"] = sum(ranked[dim] * w for dim, w in weights.items())
    ranked["rank"] = ranked["score"].rank(ascending=False, method="min").astype(int)
    return ranked.sort_values("rank")


def _weight_sensitivity(z: pd.DataFrame, log: TaskLog) -> tuple[dict, int, int]:
    """Log the ranking under the chosen weights, equal weights, and each dimension dropped in turn.

    Returns (orders per scenario, alternatives where the chosen order holds, number of alternatives).
    """
    scenarios = {
        "chosen weights": DIMENSION_WEIGHTS,
        "equal weights": {dim: 1 / len(DIMENSION_WEIGHTS) for dim in DIMENSION_WEIGHTS},
    }
    for dropped in DIMENSION_WEIGHTS:
        rest = {dim: w for dim, w in DIMENSION_WEIGHTS.items() if dim != dropped}
        scenarios[f"drop {dropped}"] = {dim: w / sum(rest.values()) for dim, w in rest.items()}

    log.info("Weight sensitivity (repos healthiest first, score in brackets):")
    orders = {}
    for name, weights in scenarios.items():
        ranked = _rank(z, weights)
        orders[name] = list(ranked.index)
        log.info("  %-24s %s", name,
                 "  >  ".join(f"{repo} ({score:+.2f})" for repo, score in ranked["score"].items()))
    same = sum(order == orders["chosen weights"] for name, order in orders.items() if name != "chosen weights")
    alternatives = len(orders) - 1
    log.info("The chosen-weights ranking holds in %d of %d alternative scenarios", same, alternatives)
    return orders, same, alternatives


@task(name="5-model")
def model() -> dict:
    """Task 5: composite health score from mart_health_inputs; ranked table to screen, log, and DB."""
    log = TaskLog("model")
    with _connect_db(log) as con:
        inputs = con.execute("SELECT * FROM mart_health_inputs ORDER BY repo").df().set_index("repo")
        _check_model_config(inputs.columns)

        z = _signed_zscores(inputs, log)
        ranking = _rank(z, DIMENSION_WEIGHTS)
        dims = list(DIMENSION_WEIGHTS)
        _show_table(log, "Health ranking (dimension subscores = mean signed z; score = weighted sum):",
                    ranking[["rank", "score", *dims]].round(2), index=True)
        _show_table(log, "Signed z-scores per metric (positive = healthier than the group average):",
                    z.T.round(2), index=True)
        sensitivity, holds, alternatives = _weight_sensitivity(z, log)

        out = ranking.reset_index()[["repo", "rank", "score", *dims]]
        # Run-level facts, repeated on every row, so Task 6 can read them from the database.
        out["sensitivity_holds"] = holds
        out["sensitivity_scenarios"] = alternatives
        out["scored_at"] = datetime.now(UTC).replace(tzinfo=None)
        con.register("ranking_df", out)
        con.execute("CREATE OR REPLACE TABLE health_ranking AS SELECT * FROM ranking_df")
        con.unregister("ranking_df")
        log.info("Wrote %d rows to health_ranking", len(out))

    return {"ranking": out.drop(columns="scored_at").to_dict(orient="records"), "sensitivity": sensitivity}


# ---------- Task 6 helpers ----------
# mart_repo_monthly column -> (panel title, y-axis label with units, log scale?)
TREND_PANELS = {
    "commits": ("Commits", "commits / month", False),
    "prs_opened": ("PRs opened", "opened PRs / month", False),
    "prs_merged": ("PRs merged", "merged PRs / month", False),
    "merge_ratio": ("Merge ratio", "merged ÷ (merged + closed unmerged)", False),
    "active_contributors": ("Active contributors", "non-bot PR authors / month", False),
    "median_first_review_hours": ("Time to first review", "median hours (log scale)", True),
    "median_merge_hours": ("Time to merge", "median hours (log scale)", True),
}


def _load_plot_data(con) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read the two plot sources: the ranking (healthiest first) and complete months only.

    The run month is dropped because it is only partly observed. month::DATE (in the UTC
    session set by _connect_db) avoids month labels shifting to the last day of the prior month.
    """
    ranking = con.execute("SELECT * FROM health_ranking ORDER BY rank").df()
    monthly = con.execute(f"""
        SELECT repo, month::DATE AS month, {", ".join(TREND_PANELS)}
        FROM mart_repo_monthly
        WHERE NOT is_partial_month
        ORDER BY repo, month
    """).df()
    monthly["month"] = pd.to_datetime(monthly["month"])
    return ranking, monthly


def _check_repo_colors(repos) -> None:
    """Raise if a repo has no assigned color; otherwise matplotlib would silently pick one."""
    missing = sorted(set(repos) - set(REPO_COLORS))
    if missing:
        raise ValueError(f"Invalid Task 6 config: no REPO_COLORS entry for {missing}")


def _short(repo: str) -> str:
    return repo.split("/")[1]


def _rank_labels(ranking: pd.DataFrame) -> dict:
    """repo -> '#1 scikit-learn', so every legend shows the ranking the same way."""
    return {r.repo: f"#{r.rank} {_short(r.repo)}" for r in ranking.itertuples()}


def _save(fig, path: Path) -> Path:
    fig.savefig(path, dpi=PLOT_DPI, bbox_inches="tight")
    plt.close(fig)  # free the figure; open figures otherwise accumulate in memory
    return path


def _plot_dimension_profile(ranking: pd.DataFrame, path: Path) -> Path:
    """Small-multiples bars: one panel per repo in rank order, one bar per scored dimension."""
    dims = list(DIMENSION_WEIGHTS)
    limit = ranking[dims].abs().to_numpy().max() * 1.4  # room for the value labels
    # squeeze=False: always a 2-D array of axes, even with one repo (DEV_MODE), so zip() works.
    fig, axes = plt.subplots(1, len(ranking), figsize=(15, 4.8), sharex=True, sharey=True,
                             squeeze=False)
    axes = axes[0]  # the single row of panels

    for ax, row in zip(axes, ranking.itertuples()):
        values = [getattr(row, dim) for dim in dims]
        y = range(len(dims))
        ax.barh(y, values, color=REPO_COLORS[row.repo])
        for yi, v in zip(y, values):
            ax.text(v + (0.04 if v >= 0 else -0.04), yi, f"{v:+.2f}",
                    va="center", ha="left" if v >= 0 else "right", fontsize=9)
        ax.axvline(0, color="black", linewidth=0.8)
        ax.set_xlim(-limit, limit)
        # 3 decimals: the scores are a near tie, and 2 decimals made two repos look identical
        ax.set_title(f"#{row.rank} {_short(row.repo)}   score {row.score:+.3f}", fontweight="bold")
        ax.set_xlabel("subscore (mean signed z, σ units; + = healthier)")
        ax.grid(axis="x", alpha=0.3)

    axes[0].set_yticks(range(len(dims)),
                       [f"{d.replace('_', ' ')}\n(weight {DIMENSION_WEIGHTS[d]:.2f})" for d in dims])
    axes[0].invert_yaxis()  # first dimension at the top (shared y, so applies to all panels)

    # Both facts come from health_ranking (written by Task 5), so the subtitle can't go stale.
    span = ranking["score"].max() - ranking["score"].min()
    holds, scenarios = ranking[["sensitivity_holds", "sensitivity_scenarios"]].iloc[0]
    fig.suptitle("Health dimension profile, ordered by final rank (healthiest on the left)",
                 fontsize=14, y=1.04)
    fig.text(0.5, 0.97, f"Scores span {span:.2f} (a near tie); this order holds in "
             f"{holds} of {scenarios} alternative weight scenarios",
             ha="center", fontsize=10, style="italic")
    return _save(fig, path)


def _plot_monthly_trends(monthly: pd.DataFrame, ranking: pd.DataFrame, path: Path) -> Path:
    """2x4 small multiples: one panel per monthly metric, one line per repo; 8th slot = legend."""
    labels = _rank_labels(ranking)
    fig, axes = plt.subplots(2, 4, figsize=(19, 8), sharex=True)
    panels = axes.flat

    for ax, (col, (title, ylabel, log_scale)) in zip(panels, TREND_PANELS.items()):
        for repo in ranking["repo"]:  # rank order, so legend order = ranking
            d = monthly[monthly["repo"] == repo]
            ax.plot(d["month"], d[col], color=REPO_COLORS[repo], label=labels[repo],
                    linewidth=1.6, marker="o", markersize=2.5)  # NULL months leave a gap
        ax.set_title(title, fontweight="bold")
        ax.set_ylabel(ylabel)
        if log_scale:
            ax.set_yscale("log")
        if col == "merge_ratio":
            ax.set_ylim(-0.03, 1.05)  # a share; just below 0 so a month at exactly 0 isn't clipped
        ax.grid(alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
    axes[0, 3].tick_params(labelbottom=True)  # the legend slot is below it, so show its dates

    legend_ax = axes[1, 3]
    legend_ax.axis("off")
    handles, names = axes[0, 0].get_legend_handles_labels()
    legend_ax.legend(handles, names, title="Repo (by health rank)", loc="center",
                     fontsize=12, title_fontsize=12, frameon=False)

    first, last = monthly["month"].min(), monthly["month"].max()
    fig.suptitle(f"Monthly activity, {first:%b %Y} – {last:%b %Y}", fontsize=14, y=1.02)
    fig.text(0.5, 0.985, "Complete months only; the partial run month is excluded. x-axis: calendar month.",
             ha="center", fontsize=10, style="italic")
    fig.tight_layout()
    return _save(fig, path)


def _plot_relationship(monthly: pd.DataFrame, ranking: pd.DataFrame, path: Path,
                       log: TaskLog) -> Path:
    """Bubble chart, one bubble per repo-month: x review speed, y merge ratio, size contributors."""
    labels = _rank_labels(ranking)
    cols = ["median_first_review_hours", "merge_ratio"]
    points = monthly.dropna(subset=cols)
    log.info("Relationship plot: %d repo-months plotted, %d dropped for a NULL %s",
             len(points), len(monthly) - len(points), " or ".join(cols))

    size_per_author = 6  # marker area (pt^2) per active contributor
    fig, ax = plt.subplots(figsize=(11, 7))
    for repo in ranking["repo"]:
        d = points[points["repo"] == repo]
        color = REPO_COLORS[repo]
        ax.scatter(d["median_first_review_hours"], d["merge_ratio"],
                   s=d["active_contributors"] * size_per_author, color=color, alpha=0.5,
                   edgecolors=color, linewidths=0.5, label=labels[repo])
        ax.scatter(d["median_first_review_hours"].median(), d["merge_ratio"].median(),
                   s=260, marker="D", color=color, edgecolors="black", linewidths=1.5, zorder=3)

    ax.set_xscale("log")
    ax.set_xlabel("Median hours to first review (log scale)")
    ax.set_ylabel("Merge ratio  (merged ÷ (merged + closed unmerged))")
    ax.set_title("Review speed vs. merge ratio per repo-month (bubble area = active contributors)",
                 fontweight="bold")
    ax.grid(alpha=0.3, which="both")

    repo_legend = ax.legend(title="Repo (by health rank)", loc="upper left",
                            bbox_to_anchor=(1.01, 1), markerscale=0.6, frameon=False)
    for handle in repo_legend.legend_handles:
        handle.set_sizes([80])  # uniform swatches; the size legend below explains area
    ax.add_artist(repo_legend)
    size_handles = [ax.scatter([], [], s=n * size_per_author, color="grey", alpha=0.5) for n in (10, 40, 80)]
    size_handles.append(ax.scatter([], [], s=120, marker="D", color="white", edgecolors="black", linewidths=1.5))
    ax.legend(size_handles, ["10 contributors", "40 contributors", "80 contributors", "repo median month"],
              title="Marker", loc="upper left", bbox_to_anchor=(1.01, 0.6), labelspacing=1.5, frameon=False)
    return _save(fig, path)


@task(name="6-plot")
def plot() -> dict:
    """Task 6: three multi-dimensional PNGs in plots/, from health_ranking and mart_repo_monthly."""
    log = TaskLog("plot")
    PLOTS_DIR.mkdir(exist_ok=True)
    with _connect_db(log) as con:  # SELECTs only
        ranking, monthly = _load_plot_data(con)
    _check_repo_colors(set(ranking["repo"]) | set(monthly["repo"]))

    saved = [
        _plot_dimension_profile(ranking, PLOTS_DIR / "1_dimension_profile.png"),
        _plot_monthly_trends(monthly, ranking, PLOTS_DIR / "2_monthly_trends.png"),
        _plot_relationship(monthly, ranking, PLOTS_DIR / "3_relationship.png", log),
    ]
    for path in saved:
        log.info("Saved %s (%.0f KB)", path.relative_to(BASE_DIR), path.stat().st_size / 1024)
    return {"plots": [str(p) for p in saved]}


# ---------- Flow ----------
# The task runner only matters for Task 1's mapped extract-repo runs: no other task is submitted.
@flow(name="bxp6yd-package-health-flow", task_runner=ThreadPoolTaskRunner(max_workers=EXTRACT_WORKERS))
def package_health_flow() -> None:
    log = TaskLog("flow")
    ODOMETER.reset()
    LIMITER.reset()
    if DEV_MODE:
        log.warning("DEV_MODE is ON (start=%s, repos=%s). Set DEV_MODE = False before the final run.",
                    START_DATE, REPOS)
    try:
        extract()
        load()
        dbt_build()
        validate()
        model()
        plot()
    finally:  # runs on success, on a failed task, and on Ctrl-C, so every run reports its total
        log.info("API odometer: %s", ODOMETER.summary())


if __name__ == "__main__":
    package_health_flow()
