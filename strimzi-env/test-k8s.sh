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
echo "Step 2: Building Connect Docker image locally"
echo "============================================="
docker build -t test-env-kafka-connect:strimzi-3.0 -f strimzi-env/Dockerfile.connect-strimzi .

echo "============================================="
echo "Step 3: Creating namespace and checking Strimzi"
echo "============================================="
kubectl create namespace kafka || true

if ! kubectl get deployment -n kafka strimzi-cluster-operator &>/dev/null; then
  echo "Installing Strimzi Cluster Operator (this may take a minute)..."
  kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
  
  echo "Waiting for Strimzi Operator to start..."
  kubectl wait --namespace kafka --for=condition=ready pod --selector=name=strimzi-cluster-operator --timeout=300s
fi
echo "Strimzi Cluster Operator is ready."
sleep 15

echo "============================================="
echo "Step 4: Deploying Kafka and Schema Registry"
echo "============================================="
kubectl apply -f strimzi-env/kafka-cluster.yaml
kubectl apply -f strimzi-env/schema-registry.yaml

echo "Waiting for Strimzi Kafka cluster to be ready (usually takes 1-2 minutes)..."
kubectl wait --namespace kafka --for=condition=ready kafka/my-cluster --timeout=300s
echo "Kafka cluster is up!"

echo "Waiting for Schema Registry pod to be ready..."
kubectl wait --namespace kafka --for=condition=ready pod --selector=app=schema-registry --timeout=120s
echo "Schema Registry is ready."

echo "============================================="
echo "Step 5: Deploying Kafka Connect & Connector"
echo "============================================="
kubectl apply -f strimzi-env/kafka-connect.yaml

echo "Waiting for Kafka Connect cluster to be ready..."
kubectl wait --namespace kafka --for=condition=ready kafkaconnect/my-connect-cluster --timeout=300s
echo "Kafka Connect is up!"

# Give connector resource a few seconds to initialize
echo "Waiting for XML Connector to initialize..."
sleep 10

echo "============================================="
echo "Step 6: Producing Avro record inside Kubernetes"
echo "============================================="
SCHEMA_REGISTRY_POD=$(kubectl get pod -n kafka -l app=schema-registry -o jsonpath="{.items[0].metadata.name}")

echo "Sending record using Schema Registry pod ($SCHEMA_REGISTRY_POD)..."
echo '{"id": 2002, "name": "Alice & Bob (K8s)", "status": "active"}' | kubectl exec -i -n kafka "$SCHEMA_REGISTRY_POD" -- kafka-avro-console-producer \
  --bootstrap-server my-cluster-kafka-bootstrap:9092 \
  --topic avro-source \
  --property value.schema='{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"status","type":"string"}]}'

echo "Avro record sent successfully."

echo "============================================="
echo "Step 7: Consuming XML output from 'xml-target'"
echo "============================================="
KAFKA_POD="my-cluster-kafka-node-pool-0"

echo "Reading target topic 'xml-target' (waiting up to 30 seconds for message)..."
XML_OUTPUT=$(kubectl exec -i -n kafka "$KAFKA_POD" -c kafka -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic xml-target \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 30000)

echo ""
echo "------------------- RECEIVED XML (K8s) -------------------"
echo "$XML_OUTPUT"
echo "----------------------------------------------------------"

if echo "$XML_OUTPUT" | grep -q "<testRecord>" && echo "$XML_OUTPUT" | grep -q "<name>Alice &amp; Bob (K8s)</name>"; then
  echo "Success: Kubernetes Strimzi XML Conversion verification passed!"
else
  echo "Error: XML Output verification failed or empty."
  exit 1
fi
