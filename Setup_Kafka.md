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