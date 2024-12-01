#!/bin/bash
echo "Starting docker containers..."
docker compose up -d

# Wait for Debezium Connect to be ready
echo "Waiting for Debezium Connect to be ready..."
until curl -s http://localhost:8083/connectors >/dev/null; do
  sleep 5
done

# Deploy the connector
echo "Deploying Debezium connector..."
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d @debezium-connector.json

sleep 1

# Verify connector status
echo "Verifying connector status..."
curl -s http://localhost:8083/connectors/transactions-connector/status
