"""Publish the scheduled Prefect deployment of the package health flow.

Run once from the activated .venv:  python deploy.py
Importing this file does nothing; only running it publishes (the __main__ guard).
"""
from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository
from prefect.schedules import Cron

from main import package_health_flow

GIT_URL = "https://github.com/Mattcben1/DP2.git"
BRANCH = "extras"  # the worker clones this branch, so it must be pushed before a run
ENTRYPOINT = "main.py:package_health_flow"
DEPLOYMENT_NAME = "bxp6yd-deployment"
WORK_POOL = "uvasds-k8s-pool"
TOKEN_SECRET_BLOCK = "bxp6yd-pat"  # same block main.py falls back to for API calls
SCHEDULE = Cron("0 0 28 * *", timezone="America/New_York")  # midnight Eastern on the 28th
JOB_VARIABLES = {  # values fixed by DEPLOYMENTS.md
    "cpu_request": "2",
    "cpu_limit": "2",
    "memory_request": "2Gi",
    "memory_limit": "4Gi",
}

if __name__ == "__main__":
    package_health_flow.from_source(
        source=GitRepository(
            url=GIT_URL,
            branch=BRANCH,
            # The block itself, not .get(): the deployment stores only a reference, and the
            # worker fetches the token at clone time, so the PAT never enters its configuration.
            credentials={"access_token": Secret.load(TOKEN_SECRET_BLOCK)},
        ),
        entrypoint=ENTRYPOINT,
    ).deploy(
        name=DEPLOYMENT_NAME,
        work_pool_name=WORK_POOL,
        schedule=SCHEDULE,
        job_variables=JOB_VARIABLES,
    )
