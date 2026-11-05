# Prefect Deployment: `bxp6yd-deployment`

## What it does

`bxp6yd-deployment` runs the full six-task flow `bxp6yd-package-health-flow` on the SDS Prefect server
(https://sds-prefect.pods.uvarc.io/) instead of on a laptop:

| Setting | Value |
| --- | --- |
| Flow (entrypoint) | `main.py:package_health_flow` |
| Source | `https://github.com/Mattcben1/DP2.git`, branch `main` (private; cloned with the `bxp6yd-pat` Secret block) |
| Work pool | `uvasds-k8s-pool` (Kubernetes: each run is a new pod) |
| Schedule | `Cron("0 0 28 * *", timezone="America/New_York")`: midnight Eastern on the 28th of every month |
| Job variables | `cpu_request "2"`, `cpu_limit "2"`, `memory_request "2Gi"`, `memory_limit "4Gi"` |

At run time the worker clones the branch and imports `main.py`. Then it runs `package_health_flow()`,
the same function `python main.py` calls.

## One-time setup

1. **Secret block.** `bxp6yd-pat` is a Prefect *Secret* block holding a classic GitHub PAT with the `repo` scope.
   The `repo` scope is needed because the repository is private. The block is used twice:
   - the worker uses it to clone the repo (`GitRepository(credentials=...)` in `deploy.py`), and
   - Task 1 uses it for GitHub API calls when `GH_PAT` is not set (`_github_headers()` in `main.py`).
2. **Push the branch.** The worker clones `main` from GitHub, so it runs whatever was last pushed.

## Deployment command

From the project root, inside the activated `.venv`:

```
python deploy.py
```

`deploy.py`:

```python
from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository
from prefect.schedules import Cron

from main import package_health_flow

GIT_URL = "https://github.com/Mattcben1/DP2.git"
BRANCH = "main"
ENTRYPOINT = "main.py:package_health_flow"
DEPLOYMENT_NAME = "bxp6yd-deployment"
WORK_POOL = "uvasds-k8s-pool"
TOKEN_SECRET_BLOCK = "bxp6yd-pat"
SCHEDULE = Cron("0 0 28 * *", timezone="America/New_York")
JOB_VARIABLES = {"cpu_request": "2", "cpu_limit": "2", "memory_request": "2Gi", "memory_limit": "4Gi"}

if __name__ == "__main__":
    package_health_flow.from_source(
        source=GitRepository(
            url=GIT_URL,
            branch=BRANCH,
            credentials={"access_token": Secret.load(TOKEN_SECRET_BLOCK)},
        ),
        entrypoint=ENTRYPOINT,
    ).deploy(
        name=DEPLOYMENT_NAME,
        work_pool_name=WORK_POOL,
        schedule=SCHEDULE,
        job_variables=JOB_VARIABLES,
    )
```

The `Secret` block object is passed, not `Secret.load(...).get()`. The deployment therefore stores only a
reference to the block, and the PAT never appears in the deployment's configuration.

## How the run gets its GitHub token

`_github_headers()` in `main.py` reads `GH_PAT` from the environment if it is set, as on a laptop.
If it is not set, as in the deployed pod, it reads the token from `Secret.load("bxp6yd-pat").get()`.
Task 1 logs which source was used, never the value:

```
GitHub token source: environment (GH_PAT)              <- local run
GitHub token source: Secret block 'bxp6yd-pat'         <- deployed run
```

## Dependencies on the worker

The pool's image, `ghcr.io/uvads/prefect-k8s-worker:3.9.0-python3.12` ("UVA SDS Prefect Kubernetes job image
(datasci deps)"), already includes everything the flow imports: `prefect 3.9.0`, `prefect-dbt 0.7.25`,
`dbt-core 1.12.6`, `dbt-duckdb 1.11.0`, `duckdb 2.0.0`, `httpx 0.28.1`, `pandas 3.1.0` and `matplotlib 3.11.2`.
These versions match `requirements.txt`, except that the image has `dbt-duckdb` 1.11.0 and we pin 1.11.1
(a patch release). Nothing is installed at run time.

## What a deployed run can and cannot do

- **Always a full run.** Each run starts in a new pod with no `packages.duckdb`, no `data/raw/` and no
  `_load_state` watermarks. Task 1 therefore fetches the whole window from 2024-01-01. A deployed run now uses
  GraphQL: about 200 requests and 4–5 minutes. The logged manual run below used the earlier REST code (7,327
  requests, 79 minutes). Incremental resume (the `_LOADED` marker and watermarks) only
  helps when the database survives between runs, as it does on a laptop.
- **Outputs stay in the pod.** `plots/*.png`, `logs/*.log` and `packages.duckdb` are written inside the pod
  and are deleted with it. The Prefect UI log is the permanent record of a deployed run: every task logs through
  the Prefect logger, including the ranking table and the API odometer total. In production the plots would be
  copied to remote storage such as S3.

## Manual run log

Triggered once from the "Run" button at https://sds-prefect.pods.uvarc.io/v2/deployments, at 7:59 PM EST on
Nov 4, 2026 (flow run `expert-piculet`). The shared `uvasds-k8s-pool` worker picked it up at 8:26 PM. The run took
79 minutes, including one 37-minute wait for the rate limit to reset. It ran commit `c634ad4`, the sequential REST
extract. The concurrency and GraphQL commits were pushed after it.

The log below is trimmed. Timestamps are UTC. Prefect's "Finished" line doesn't name the task, so each one is shown
with the task's own last line before it.

```
2026-11-05 01:26:48 UTC  Worker 'KubernetesWorker 59b8229a-1097-400e-afd7-b4547b8611e4' submitting flow run '6cf9906e-0540-42e4-8bbb-60cc0530c1d7'
2026-11-05 01:26:58 UTC  Beginning flow run 'expert-piculet' for flow 'bxp6yd-package-health-flow'
2026-11-05 01:26:58 UTC  GitHub token source: Secret block 'bxp6yd-pat'
...
2026-11-05 02:45:10 UTC  Extract finished: 7327 pages fetched this run (7327 requests sent, retries included)
2026-11-05 02:45:10 UTC  Finished in state Completed()                      <- 1-extract
2026-11-05 02:45:27 UTC  All files loaded and committed; created data/raw/_LOADED
2026-11-05 02:45:27 UTC  Finished in state Completed()                      <- 2-load
2026-11-05 02:45:48 UTC  dbt build finished: {'success': 8, 'pass': 43}
2026-11-05 02:45:48 UTC  Finished in state Completed()                      <- 3-dbt-build
2026-11-05 02:45:59 UTC  All completeness checks passed
2026-11-05 02:45:59 UTC  Finished in state Completed()                      <- 4-validate
2026-11-05 02:45:59 UTC  Wrote 3 rows to health_ranking
2026-11-05 02:45:59 UTC  Finished in state Completed()                      <- 5-model
2026-11-05 02:46:02 UTC  Saved plots/3_relationship.png (149 KB)
2026-11-05 02:46:02 UTC  Finished in state Completed()                      <- 6-plot
2026-11-05 02:46:02 UTC  API odometer: 7327 GitHub API requests this flow run (by status: 200: 7327; no response: 0)
2026-11-05 02:46:02 UTC  Finished in state Completed()                      <- flow run
```
