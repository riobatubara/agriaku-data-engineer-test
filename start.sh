#!/bin/bash

set -e  # exit on any error

echo "Stopping any existing containers..."
docker-compose down

echo "Setting folder permissions for Airflow to write..."
mkdir -p processed_files output
sudo chmod -R 777 processed_files output

echo "Building containers..."
docker-compose up --build -d

echo "Initializing Airflow database..."
docker-compose run --rm airflow-webserver airflow db init

echo "Creating Airflow user..."
docker-compose run --rm airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "Starting all Airflow services..."
docker-compose up -d

echo "Airflow is starting... Access it at http://localhost:8080"
