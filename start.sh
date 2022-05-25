#!/usr/bin/env bash

docker build . --tag custom-airflow:2.3.0

echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0\nAIRFLOW_IMAGE_NAME=custom-airflow:2.3.0" > .env

docker-compose -f docker-compose.yaml up airflow-init
docker-compose -f docker-compose.yaml up
