FROM apache/airflow:3.3.2

# Keep Airflow's Snowflake connection available after rebuilding the image.
USER airflow
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "apache-airflow-providers-snowflake"

# dbt in its own environment avoids changing Airflow's Python dependencies.
USER root
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
       "dbt-core==1.12.5" "dbt-snowflake==1.12.1"
USER airflow
