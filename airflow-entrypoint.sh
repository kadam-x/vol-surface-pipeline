#!/bin/bash
set -e

airflow db init

sleep 5

airflow users create \
  --role Admin \
  --username "${_AIRFLOW_WWW_USER_USERNAME:-admin}" \
  --email admin@example.com \
  --password "${_AIRFLOW_WWW_USER_PASSWORD:-admin}" \
  --firstname Admin \
  --lastname User 2>/dev/null || true

airflow webserver &
sleep 5
airflow scheduler
