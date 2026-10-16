#!/bin/bash
set -e

echo "Verifying PostgreSQL connectivity at ${POSTGRES_HOST:-postgres}:${POSTGRES_PORT:-5432}..."

python3 -c "
import socket
import time
import os
import sys

host = os.environ.get('POSTGRES_HOST', 'postgres')
port = int(os.environ.get('POSTGRES_PORT', '5432'))
retries = 30

for i in range(retries):
    try:
        with socket.create_connection((host, port), timeout=3):
            print(f'Successfully connected to PostgreSQL at {host}:{port}')
            sys.exit(0)
    except OSError:
        print(f'Waiting for PostgreSQL ({host}:{port}) to be ready... ({i+1}/{retries})')
        time.sleep(2)

print('Timeout: Could not connect to PostgreSQL.')
sys.exit(1)
"

# Handle container execution modes
if [ "$1" = "init" ]; then
    echo "Running Airflow database migrations..."
    airflow db migrate

    echo "Creating Airflow admin account if not already present..."
    airflow users create \
        --username "${AIRFLOW_ADMIN_USERNAME:-admin}" \
        --firstname "${AIRFLOW_ADMIN_FIRSTNAME:-Airflow}" \
        --lastname "${AIRFLOW_ADMIN_LASTNAME:-Admin}" \
        --role Admin \
        --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" \
        --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" || true

    echo "Airflow initialization completed successfully."
    exit 0
fi

# Pass through execution to airflow CLI (e.g. webserver, scheduler)
exec airflow "$@"
