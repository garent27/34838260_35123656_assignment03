# FIT3182 Spark Streaming Node Setup Guide

This guide walks you through configuring an AWS EC2 instance as a dedicated Apache Spark streaming node that reads from your AWS Kafka broker and writes to MongoDB Atlas.

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

Minimum required:

* **25 GB EBS Volume**

This helps prevent:

```text
no space left on device
```

errors when extracting the Spark Docker image.

---

## Security Group Rules (`fit3182-cluster-sg`)

Configure the following inbound rules:

| Port | Protocol   | Access              |
| ---- | ---------- | ------------------- |
| 22   | SSH        | Your IP or Anywhere |
| 8889 | Custom TCP | Anywhere            |

> Port `8889` is required if using browser-based visualizations.

---

# 🚀 Step-by-Step Installation

### (OPTIONAL) but if more ram is needed 
Because Spark runs on a Java Virtual Machine (JVM), it is memory-intensive. Adding 2GB of swap memory helps prevent instance crashes or freezes.


```bash id="m62npw"
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```


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

## Step 1: Create the Workspace Directory

Create the main Spark workspace folder and move into it:

```bash
mkdir ~/fit3182-spark
cd ~/fit3182-spark
```

---

# Step 2: Create Required Project Files

Create the required project files:

```bash
touch config.py
touch utils.py
touch streaming_app.py
touch docker-compose.yml
```

Verify the files were created successfully:

```bash
ls
```

You should see:

```text
config.py
utils.py
streaming_app.py
docker-compose.yml
```

---

# Step 3: Edit Files Using Nano

## Edit `config.py`

Open the file:

```bash
nano config.py
```

Paste your config.py here.

Change IP address for config configuration:

```python
IP_ADDRESS = "YOUR_KAFKA_PRIVATE_IP"
```

Save and exit:

* Press `CTRL + O` → Press `Enter`
* Press `CTRL + X`

---

## Edit `utils.py`

Open the file:

```bash
nano utils.py
```

Paste your utility/helper functions.

Save and exit:

* Press `CTRL + O`
* Press `Enter`
* Press `CTRL + X`

---

## Edit `streaming_app.py`

Open the streaming application file:

```bash
nano streaming_app.py
```

Paste your converted Spark streaming Python code from your notebook.

Save and exit:

* Press `CTRL + O`
* Press `Enter`
* Press `CTRL + X`

---

## Edit `docker-compose.yml`

Open the Docker Compose configuration:

```bash
nano docker-compose.yml
```

Paste the following configuration:

```yaml
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
```

Save and exit:

* Press `CTRL + O`
* Press `Enter`
* Press `CTRL + X`

---

# Step 4: Verify File Contents

You can quickly inspect a file using:

```bash
cat docker-compose.yml
```

or:

```bash
cat config.py
```

---

# Alternative: Upload Files From Local Machine

Instead of manually typing files into Nano, you may also upload them directly from your local machine using:

```powershell
scp -i "PATH_TO_KEY.pem" filename ubuntu@EC2_PUBLIC_IP:~/fit3182-spark/
```

Example:

```powershell
scp -i "fit3182-cloud-key.pem" streaming_app.py ubuntu@13.211.39.46:~/fit3182-spark/
```
# 🏎 Running the Streaming Pipeline

## 1. Launch the Application

Start the Spark streaming engine in detached mode:

```bash id="sj8i9v"
docker compose up -d
```

### First-Time Startup

On the first run:

* Docker downloads the course image stack
* Extraction takes approximately **1–2 minutes**

---


## 2. Monitor Live Data & Pipeline Output

To see Spark's actual system output, print statements, and streaming batch updates in real time, run:
```bash
docker compose logs -f spark-stream
```


View real-time logs, debugging exceptions, and micro-batch updates:

```bash id="zy2x5e"
docker compose logs -f
```

---

## 3. Stop the Pipeline

Shut down the streaming pipeline cleanly:

```bash id="n8tw2v"
docker compose down
```
