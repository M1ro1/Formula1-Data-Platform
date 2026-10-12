FROM apache/airflow:2.10.5-python3.11

COPY airflow/requirements.txt /tmp/requirements.txt

# Install with Airflow's constraints so dependency versions stay compatible
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"
