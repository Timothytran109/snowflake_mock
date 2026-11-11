FROM apache/airflow:3.3.2-python3.12 AS runtime

USER root
RUN mkdir -p /opt/dbt /opt/customer360 /opt/airflow/secrets/dbt /opt/airflow/secrets/keys \
    && chown -R airflow:0 /opt/dbt /opt/customer360 /opt/airflow/secrets \
    && ln -s /opt/dbt/bin/dbt /usr/local/bin/dbt

COPY --chown=airflow:0 airflow/requirements.txt /opt/customer360/airflow-requirements.txt
COPY --chown=airflow:0 dbt/requirements.txt /opt/customer360/dbt-requirements.txt

USER airflow
RUN python -m pip install --no-cache-dir \
      -r /opt/customer360/airflow-requirements.txt \
      --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt \
    && python -m pip check

# Do not put the dbt venv on PATH: Airflow must keep its own Python environment.
RUN python -m venv /opt/dbt \
    && PIP_USER=false /opt/dbt/bin/python -m pip install --no-cache-dir \
      -r /opt/customer360/dbt-requirements.txt \
    && /opt/dbt/bin/python -m pip check \
    && /opt/dbt/bin/python -c "import importlib.util; assert importlib.util.find_spec('airflow') is None, 'Airflow leaked into the dbt environment'" \
    && /opt/dbt/bin/dbt --version

ENV PYTHONDONTWRITEBYTECODE=1 \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false

# Optional tools inherit Airflow for DAG tests without entering either runtime.
# Build explicitly with --target development; Compose uses the final runtime.
FROM runtime AS development
COPY --chown=airflow:0 requirements-dev.txt /opt/customer360/requirements-dev.txt
RUN python -m venv --system-site-packages /opt/customer360/dev \
    && PIP_USER=false /opt/customer360/dev/bin/python -m pip install --no-cache-dir \
      -r /opt/customer360/requirements-dev.txt \
    && /opt/customer360/dev/bin/python -m pip check

FROM runtime AS final
