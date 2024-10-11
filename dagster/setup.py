from setuptools import find_packages, setup

setup(
    name="ndr",
    packages=find_packages(),
    package_data={
        "ndr": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster==1.8.9",
        "dagster-cloud==1.8.9",
        "dagster-dbt==0.24.9",
        "dagster-fivetran==0.24.9",
        "dbt-snowflake",
    ],
    extras_require={"dev": ["dagster-webserver"]},
)
