# Week 3 | Homework
# from prefect orion
# import
from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from etl_web_gcs_hw_wk3 import etl_parent_local_gcs

# Fetch storage from GitHub
github_block = GitHub.load("ny-taxi-github-block")

# https://docs.prefect.io/api-ref/prefect/deployments/#prefect.deployments.Deployment.build_from_flow

gsc_git_dep = Deployment.build_from_flow(
    flow=etl_parent_local_gcs,
    name="ny-taxi-flow-bq-hw-wk3",
    storage=github_block,
)

print("Successfully deployed NY Taxi Github Block. Check app.prefect.cloud")

# execute
if __name__ == "__main__":
    gsc_git_dep.apply()

# to deploy
# prefect deployment run <> --params '{"year":2019, "months": [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 1]}'
