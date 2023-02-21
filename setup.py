from setuptools import find_packages, setup

setup(
    name="data-pipeline-tools",
    version="0.1",
    packages=['data_pipeline_tools'],
    url="https://github.com/tpximpact/GCP-Data-Pipelines.git",
    install_requires=["pandas", "aiohttp", "google-cloud"],
)
