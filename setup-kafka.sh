#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "=================================================="
echo "Starting FIT3182 Kafka Streaming Node Configuration"
echo "=================================================="

# 1. System Updates & Core Dependencies
echo "[1/5] Updating packages and installing Docker utilities..."
sudo apt update -y
sudo apt install docker.io docker-compose -y

# 2. Configure Service Initialization & Permissions
echo "[2/5] Starting Docker daemon and configuring group permissions..."
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER

# 3. Establish Isolated Workspace Workspace
echo "[3/5] Initializing workspace directory..."
mkdir -p ~/fit3182-kafka
cd ~/fit3182-kafka

# 4. Dynamically Resolve EC2 Public IP Address
echo "[4/5] Dynamically fetching AWS EC2 External Public IP..."
# Reverts to a public curl check if AWS token metadata constraints are tightly restricted
PUBLIC_IP=$(curl -s https://checkip.amazonaws.com || curl -s http://169.254.169.254/latest/meta-data/public-ipv4)

if [ -z "$PUBLIC_IP" ]; then
    echo "❌ Error: Could not resolve Public IP address automatically."
    echo "Falling back to placeholder. Please manually modify docker-compose.yml"
    PUBLIC_IP="YOUR_EC2_PUBLIC_IP"
else
    echo "✅ Successfully captured Public IP Target: $PUBLIC_IP"
fi

# 5. Programmatically Author Docker-Compose Specification
echo "[5/5] Generating docker-compose.yml configuration deployment manifest..."
cat << EOF > docker-compose.yml
version: '3'

services:
  zookeeper:
    image: fit3182/zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    restart: unless-stopped

  kafka:
    image: fit3182/kafka
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: STREAMING://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: STREAMING://${PUBLIC_IP}:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: STREAMING:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: STREAMING
    restart: unless-stopped
EOF

echo "=================================================="
echo "Setup Complete! Running deployment containers..."
echo "=================================================="

# Apply docker group privileges and run remaining commands in subshell
newgrp docker << 'DOCKER_COMMANDS'

# Navigate directly into the Kafka directory layout

# Fire up the containers in detached daemon configuration mode
docker compose up -d

echo "Waiting for Kafka broker to initialize..."
sleep 5  # Gives Kafka a few seconds to fully boot up before accepting topics

echo "Creating Kafka topics..."

# 1. Create Topic A
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic camera-events-A \
  --partitions 3 \
  --replication-factor 1

# 2. Create Topic B
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic camera-events-B \
  --partitions 3 \
  --replication-factor 1

# 3. Create Topic C
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic camera-events-C \
  --partitions 3 \
  --replication-factor 1

echo ""
echo "Pipeline Status Check:"
docker ps
echo ""

echo "To test consumer events stream, execute:"
echo "docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic camera-events-A --from-beginning"

DOCKER_COMMANDS