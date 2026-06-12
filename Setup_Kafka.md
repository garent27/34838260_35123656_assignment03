# FIT3182 Kafka Streaming Node Setup Guide

This guide walks you through configuring an AWS EC2 instance as a dedicated Apache Kafka broker node for FIT3182 streaming pipelines.

The Kafka node will:

* Host Apache Kafka
* Manage real-time message streaming
* Serve producer and consumer applications
* Provide communication between Spark and your camera producers

---

# 🛠 Prerequisites & Requirements

## Application and OS Images 

Recommended Applicatioon:

* Ubuntu Server

## AWS EC2 Instance

Recommended instance types:

* minimum: `t3.small`
* or something better

---

## Storage Allocation

Minimum recommended:

* **20 GB**

This helps avoid:

```text
no space left on device
```

errors when pulling Kafka Docker images and storing streaming logs.

---

## Security Group Rules (`fit3182-cluster-sg`)

Configure the following inbound rules:

| Port | Protocol   | Access               |
| ---- | ---------- | -------------------- |
| 22   | SSH        | Your IP or Anywhere  |
| 9092 | Custom TCP | Spark Node / Your IP |
| 2181 | Custom TCP | Internal Use         |
| 8889 | Custom TCP | Anywhere (optional)  |

---

## ssh into the kakfa EC2 instance
```bash
ssh -i "your-key-pair.pem" ubuntu@YOUR_NODE_PUBLIC_IP
```

# 🚀 Step-by-Step Installation

## Option 1: Automated Setup (Recommended)

Run the automated setup script to configure everything at once:

```bash
# Download or create the setup script
chmod +x setup-kafka.sh

# Run the setup script
./setup-kafka.sh
```

This script will:
1. Update packages and install Docker utilities
2. Start Docker daemon and configure group permissions
3. Create the `~/fit3182-kafka` workspace directory
4. Automatically detect and capture your EC2 public IP
5. Generate the `docker-compose.yml` configuration
6. Start Kafka and Zookeeper containers
7. Create all three Kafka topics automatically
8. Display container status

Skip to **Verification** section below if using the automated script.

---

## Option 2: Manual Setup

Follow these steps to configure Kafka manually.

---

#  Install Docker Engine

Docker allows Kafka and Zookeeper to run inside isolated containers.

## Install Docker

```bash
# Update packages
sudo apt update

# Install Docker
sudo apt install docker.io -y
sudo systemctl start docker
sudo systemctl enable docker

# Allow your user to run Docker without typing 'sudo'
sudo usermod -aG docker $USER

# Install Docker Compose
sudo apt install docker-compose -y

# Refreshes Docker group permissions
newgrp docker
```



---

# 📂 Project Directory Structure

Create your Kafka workspace:

```bash
mkdir ~/fit3182-kafka
cd ~/fit3182-kafka
```



---

# Create the docker-compose.yml File

Create a file named:

```text
nano docker-compose.yml
```

Paste the following Kafka configuration:


```yaml
version: '3'
services:
  zookeeper:
    image: fit3182/zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"

  kafka:
    image: fit3182/kafka
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: STREAMING://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: STREAMING://YOUR_EC2_PUBLIC_IP:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: STREAMING:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: STREAMING
    restart: unless-stopped
```

```text
Save and exit (Ctrl+O, Enter, Ctrl+X).
```

---

# ⚠️ Important Configuration

Replace:

```text
YOUR_EC2_PUBLIC_IP
```

with your Kafka EC2 instance's:

```text
Private IPv4 address
```

You can find this in the AWS EC2 dashboard.

Example:

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://15.135.163.115:9092
```

---

# 🏎 Running Kafka

## 1. Start Kafka Services

Launch Kafka and Zookeeper in detached mode:

```bash
docker compose up -d
```

---

## 2. Verify Containers

Check running containers:

```bash
docker ps
```

You should see:

```text
kafka
zookeeper
```

---


## 3. Check if reciving stream from producer A

```bash
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic camera-events-A --from-beginning
```

---

# ✅ Verification

After running either the automated or manual setup, verify your Kafka cluster is working:

## 1. Check Containers are Running

```bash
docker ps
```

You should see both containers running:
- `zookeeper`
- `kafka`

## 2. List Created Topics

```bash
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

You should see:
```text
camera-events-A
camera-events-B
camera-events-C
```

## 3. Test Topic Connectivity

From your Spark or producer node, test the connection to your Kafka broker:

```bash
# Replace YOUR_EC2_PUBLIC_IP with your instance's public IP
telnet YOUR_EC2_PUBLIC_IP 9092
```

---

# 🔧 Troubleshooting

## Containers not starting

```bash
# Check container logs
docker logs kafka
docker logs zookeeper

# Restart containers
docker compose restart
```

## Topics not created

```bash
# Check if Kafka is ready
docker exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Manually create topics
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic camera-events-A --partitions 3 --replication-factor 1
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic camera-events-B --partitions 3 --replication-factor 1
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic camera-events-C --partitions 3 --replication-factor 1
```

## Connection refused from remote producer/consumer

- Verify the security group allows inbound traffic on port `9092`
- Confirm `KAFKA_ADVERTISED_LISTENERS` is set to your **public IP** (not localhost or private IP)
- Check the EC2 instance public IP matches the configuration