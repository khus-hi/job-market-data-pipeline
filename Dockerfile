# Astro Runtime = Airflow 3 + Astronomer's defaults. The ONBUILD steps in this base image
# copy the repo in and pip-install requirements.txt (the ingestion code's dependencies).
FROM astrocrpublic.azurecr.io/runtime:3.3-12

# dbt gets its own virtualenv so its pinned dependencies can never conflict with Airflow's.
# duckdb must be the same version on both sides: the loader and dbt write the same file.
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core==1.12.6 dbt-duckdb==1.11.1 duckdb==2.0.0

ENV DBT_EXECUTABLE=/usr/local/airflow/dbt_venv/bin/dbt \
    PYTHONPATH=/usr/local/airflow \
    DATA_DIR=/usr/local/airflow/include/data \
    DUCKDB_PATH=/usr/local/airflow/include/data/warehouse.duckdb
