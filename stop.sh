#!/bin/bash

echo "Stopping all containers..."
docker-compose down --volumes --remove-orphans

echo "All containers stopped and cleaned up."
