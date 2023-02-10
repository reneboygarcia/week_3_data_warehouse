# Week 2 | Homework
# from prefect orion
# import
from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from parametric_web_to_gcs_wk3 import etl_parent_gcs_flow

# Fetch storage from GitHub
github_block = GitHub.load("ny-taxi-github-block")

# https://docs.prefect.io/api-ref/prefect/deployments/#prefect.deployments.Deployment.build_from_flow

gsc_git_dep = Deployment.build_from_flow(
    flow=etl_parent_gcs_flow,
    name="ny-taxi-flow-wk3",
    storage=github_block,
)

print("Successfully deployed NY Taxi Github Block. Check app.prefect.cloud")

# execute
if __name__ == "__main__":
    gsc_git_dep.apply()

# to deploy
# prefect deployment run etl-parent/ny-taxi-flow-wk3 --params '{"year":2019, "months": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]}'
