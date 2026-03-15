from setuptools import setup, find_packages

setup(
    name="sas-to-snowpark",
    version="1.0.0",
    description="Convert SAS PROC FREQ code to Snowflake Snowpark Python code",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "snowflake-snowpark-python>=1.0.0",
        "pandas>=1.3.0",
        "scipy>=1.7.0",
        "numpy>=1.20.0",
    ],
    entry_points={
        "console_scripts": [
            "sas2snowpark=sas_to_snowpark.cli:main",
        ],
    },
)
