#!/usr/bin/env bash
set -e

# Change directory to the repository root
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/.."

echo "============================================="
echo "Step 1: Compiling project and packaging JAR"
echo "============================================="
mvn package

echo "============================================="
echo "Step 2: Starting local Kafka cluster"
echo "============================================="
docker compose -f test-env/docker-compose.yml down -v
docker compose -f test-env/docker-compose.yml up --build -d

echo "============================================="
echo "Step 3: Waiting for Kafka Connect to start"
echo "============================================="
CONNECT_URL="http://localhost:8083"

# Poll Kafka Connect until HTTP 200 is returned
until curl -s -f -o /dev/null "$CONNECT_URL/connectors"; do
  echo "Waiting for Kafka Connect REST API at $CONNECT_URL..."
  sleep 5
done
echo "Kafka Connect is up and healthy!"

# Additional sleep to ensure Connect initialization completes
sleep 5

echo "============================================="
echo "Step 4: Registering XML Converter Connector"
echo "============================================="
curl -X POST -H "Content-Type: application/json" \
  --data @test-env/connector-config.json \
  "$CONNECT_URL/connectors"

echo ""
echo "Connector registered. Checking status..."
sleep 5
curl -s "$CONNECT_URL/connectors/xml-sink-connector/status" | grep -q "RUNNING" && echo "Connector is RUNNING!" || (echo "Connector status check failed. Details:" && curl -s "$CONNECT_URL/connectors/xml-sink-connector/status" && exit 1)

echo "============================================="
echo "Step 5: Producing Avro record to 'avro-source'"
echo "============================================="
SCHEMA_REGISTRY_CID=$(docker ps --filter "name=schema-registry" --format "{{.ID}}" | head -n 1)

if [ -z "$SCHEMA_REGISTRY_CID" ]; then
  echo "Error: Schema Registry container not found."
  exit 1
fi

echo "Sending record using Schema Registry container ($SCHEMA_REGISTRY_CID)..."
echo '{"id": 1001, "name": "Alice & Bob", "status": "active"}' | docker exec -i "$SCHEMA_REGISTRY_CID" kafka-avro-console-producer \
  --bootstrap-server kafka:29092 \
  --topic avro-source \
  --property value.schema='{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"status","type":"string"}]}'

echo "Avro record sent successfully."

echo "============================================="
echo "Step 6: Consuming XML output from 'xml-target'"
echo "============================================="
KAFKA_CID=$(docker ps --filter "name=kafka" --format "{{.ID}}" | head -n 1)

if [ -z "$KAFKA_CID" ]; then
  echo "Error: Kafka container not found."
  exit 1
fi

echo "Reading target topic 'xml-target' (waiting up to 15 seconds for message)..."
XML_OUTPUT=$(docker exec -i "$KAFKA_CID" kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic xml-target \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 15000)

echo ""
echo "------------------- RECEIVED XML -------------------"
echo "$XML_OUTPUT"
echo "----------------------------------------------------"

if echo "$XML_OUTPUT" | grep -q "<testRecord>" && echo "$XML_OUTPUT" | grep -q "<name>Alice &amp; Bob</name>"; then
  echo "Success: XML Conversion verification passed!"
else
  echo "Error: XML Output verification failed or empty."
  exit 1
fi

echo "============================================="
echo "Step 7: Tearing down Docker environment"
echo "============================================="
docker compose -f test-env/docker-compose.yml down -v
echo "Cleaned up successfully!"
