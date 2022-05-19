#!/usr/bin/env bash

docker build . --tag custom-airflow:2.3.0

echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
echo "AIRFLOW_IMAGE_NAME=custom-airflow:2.3.0" >> .env

docker-compose -f local.yaml up airflow-init
