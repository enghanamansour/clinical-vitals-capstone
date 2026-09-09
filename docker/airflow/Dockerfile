# Airflow image with the capstone pipeline's runtime dependencies baked in.
#   docker build -f docker/airflow/Dockerfile -t capstone-airflow:latest .
# (built automatically by docker/docker-compose.airflow.yml)
FROM apache/airflow:2.10.3-python3.12

COPY docker/requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# project code is bind-mounted at /opt/project by the compose file
ENV PYTHONPATH=/opt/project
