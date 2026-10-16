# =============================================================================
# Custom Apache Airflow Docker Image
# Pinned version: 2.9.1 on Python 3.11
# =============================================================================
FROM apache/airflow:2.9.1-python3.11

USER root

# Install system compilation dependencies and PostgreSQL client libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy and set execution permissions for the custom entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow

# Install pinned Python analytical dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

ENTRYPOINT ["/entrypoint.sh"]
