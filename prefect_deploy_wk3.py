# Week 2 | Homework
# from prefect orion
# import
from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from parametric_web_to_gcs_hw_q5 import etl_parent_flow

# Set-up Infrastructure
github_block = GitHub.load("prefect-github-block")

# https://docs.prefect.io/api-ref/prefect/deployments/#prefect.deployments.Deployment.build_from_flow

gsc_git_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="github-flow-hw-q5",
    storage=github_block,
)

print(
    "Successfully deployed Prefect Github Block. Check Prefect Orion then Deployments"
)

# execute
if __name__ == "__main__":
    gsc_git_dep.apply()

# prefect deployment run etl-parent/github-flow-hw-q5 --params '{"color":"green", "year":2019, "months":[4]}'
