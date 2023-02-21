from setuptools import find_packages, setup

setup(
    name="data-pipeline-tools",
    version="0.1",
    packages=find_packages(),
    install_requires=["pandas", "aiohttp", "google-cloud"],
)
