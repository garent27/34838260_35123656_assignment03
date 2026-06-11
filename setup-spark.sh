#!/bin/bash

# Exit immediately if any command fails
set -e

echo "=================================================="
echo "Starting FIT3182 Spark Streaming Node Configuration"
echo "=================================================="

# 1. Memory Safeguard (Enforce Swap Allocation)
echo "[1/6] Setting up virtual memory cushion (Swap Space)..."
if [ ! -f /swapfile ]; then
    sudo fallocate -l 2G /swapfile
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
    sudo swapon /swapfile
    echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
    echo "✅ 2GB Swap Memory initialized successfully."
else
    echo "ℹ️ Swap file already exists. Skipping allocation."
fi

# 2. System Core Engine Installation
echo "[2/6] Installing Docker core and deployment tools..."
sudo apt update -y
sudo apt install docker.io docker-compose -y
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER

# 3. Setting Up Directory Architecture
echo "[3/6] Setting up project workspace structure..."
mkdir -p ~/fit3182-spark
cd ~/fit3182-spark

# 4. User Interaction Prompt for Network Targets
echo "[4/6] Prompting for network endpoints..."
read -p "👉 Enter your AWS Kafka Node PUBLIC IP address: " KAFKA_IP
read -p "👉 Enter your complete MongoDB Atlas Connection URI: " MONGO_URI

# 5. Programmatically Drafting Supporting Files
echo "[5/6] Creating config.py, utils.py, and streaming_app.py placeholders..."

# Write config.py automatically injecting the input IP
cat << EOF > config.py
# System Network Configurations
IP_ADDRESS = "${KAFKA_IP}"
MONGO_ATLAS_URI = "${MONGO_URI}"
EOF

# Touch placeholders for custom operational files
touch utils.py
touch streaming_app.py

# 6. Structuring Docker-Compose Specification Manifest
echo "[6/6] Generating docker-compose.yml configuration file..."
cat << 'EOF' > docker-compose.yml
version: '3.8'

services:
  spark-stream:
    image: fit3182/pyspark
    container_name: spark_streaming_app
    volumes:
      - ./:/app
    working_dir: /app
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2048M
    command: >
      sh -c "pip uninstall -y kafka && pip install kafka-python && spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 streaming_app.py"
    restart: unless-stopped
EOF

echo "=================================================="
echo "Setup Complete! Staging directories created."
echo "=================================================="
echo "👉 Next Steps:"
echo "1. Drop your final streaming python logic inside: ~/fit3182-spark/streaming_app.py"
echo "2. Drop any helper functions inside: ~/fit3182-spark/utils.py"
echo "3. Run 'docker-compose up -d' within ~/fit3182-spark to launch your engine."
echo "=================================================="