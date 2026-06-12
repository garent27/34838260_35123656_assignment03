# Manual Setup Guide - Kafka & Spark (Without Scripts)

This guide provides detailed manual steps to setup Kafka and Spark nodes without using the automated shell scripts.

---

## 📋 Prerequisites

Before starting, ensure you have:

### AWS Resources
- Kafka EC2 Instance (t3.small, Ubuntu Server, 20GB EBS)
- Spark EC2 Instance (t3.small, Ubuntu Server, 25GB EBS)
- Security groups with appropriate inbound rules

### MongoDB Atlas
- Cluster created and accessible
- Connection URI obtained
- Network access set to `0.0.0.0/0`

### Local Machine
- SSH key pair (.pem file)
- Terminal/SSH client

---

# MANUAL KAFKA NODE SETUP

## Step 1: SSH into Kafka EC2 Instance

```bash
ssh -i "your-key-pair.pem" ubuntu@YOUR_KAFKA_PUBLIC_IP
```

Example:
```bash
ssh -i "fit3182-cloud-key.pem" ubuntu@13.55.144.115
```

---

## Step 2: Update System Packages

```bash
sudo apt update -y
sudo apt upgrade -y
```

---

## Step 3: Install Docker

### Install Docker Engine

```bash
sudo apt install docker.io -y
```

### Start Docker Service

```bash
sudo systemctl start docker
sudo systemctl enable docker
```

### Verify Docker Installation

```bash
docker --version
```

Expected output:
```
Docker version 29.x.x, build xxxxx
```

---

## Step 4: Install Docker Compose

```bash
sudo apt install docker-compose -y
```

### Verify Docker Compose Installation

```bash
docker-compose --version
```

Expected output:
```
Docker Compose version 2.x.x
```

---

## Step 5: Configure Docker Permissions

Allow your user to run Docker without `sudo`:

```bash
sudo usermod -aG docker $USER
```

Apply new group membership:

```bash
newgrp docker
```

Verify you can run Docker commands:

```bash
docker ps
```

---

## Step 6: Create Kafka Workspace Directory

```bash
mkdir -p ~/fit3182-kafka
cd ~/fit3182-kafka
```

Verify directory was created:

```bash
pwd
```

Expected output:
```
/home/ubuntu/fit3182-kafka
```

---

## Step 7: Get Your EC2 Public IP Address

Get your instance's public IP dynamically:

```bash
PUBLIC_IP=$(curl -s https://checkip.amazonaws.com || curl -s http://169.254.169.254/latest/meta-data/public-ipv4)
echo $PUBLIC_IP
```

**Note this IP - you'll need it in the next step.**

Example output:
```
13.55.144.115
```

---

## Step 8: Create docker-compose.yml Manually

```bash
nano docker-compose.yml
```

**Paste the following content** (replace `YOUR_PUBLIC_IP` with your actual IP from Step 7):

```yaml
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
      KAFKA_ADVERTISED_LISTENERS: STREAMING://YOUR_PUBLIC_IP:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: STREAMING:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: STREAMING
    restart: unless-stopped
```

**Save and exit:**
- Press `CTRL+O`
- Press `Enter`
- Press `CTRL+X`

---

## Step 9: Verify docker-compose.yml

Verify the file was created correctly:

```bash
cat docker-compose.yml
```

Check that your PUBLIC_IP is correctly inserted in the `KAFKA_ADVERTISED_LISTENERS` line.

---

## Step 10: Start Zookeeper and Kafka Containers

```bash
docker compose up -d
```

Expected output:
```
[+] Running 2/2
 ✔ Container zookeeper  Started
 ✔ Container kafka      Started
```

---

## Step 11: Wait for Kafka to Initialize

Give Kafka a few seconds to fully startup:

```bash
sleep 5
```

---

## Step 12: Verify Containers are Running

```bash
docker ps
```

Expected output:
```
CONTAINER ID   IMAGE              STATUS              NAMES
xxxxxxxxxxxxx   fit3182/zookeeper  Up X seconds       zookeeper
xxxxxxxxxxxxx   fit3182/kafka      Up X seconds       kafka
```

---

## Step 13: Create Kafka Topics

### Create Topic A

```bash
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic camera-events-A \
  --partitions 3 \
  --replication-factor 1
```

Expected output:
```
Created topic camera-events-A.
```

### Create Topic B

```bash
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic camera-events-B \
  --partitions 3 \
  --replication-factor 1
```

### Create Topic C

```bash
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic camera-events-C \
  --partitions 3 \
  --replication-factor 1
```

---

## Step 14: Verify Topics Created

```bash
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Expected output:
```
camera-events-A
camera-events-B
camera-events-C
```

---

## Step 15: Verify Kafka Broker Health

```bash
docker exec kafka kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092
```

You should see broker API information (not an error).

---

## ✅ Kafka Node Setup Complete!

**Keep note of:**
- Your Kafka EC2 **PUBLIC IP**: Used for connecting from local/Spark
- Container status verified with `docker ps`
- All three topics created successfully

---

---

# MANUAL SPARK NODE SETUP

## Step 1: SSH into Spark EC2 Instance

```bash
ssh -i "your-key-pair.pem" ubuntu@YOUR_SPARK_PUBLIC_IP
```

Example:
```bash
ssh -i "fit3182-cloud-key.pem" ubuntu@13.211.39.46
```

---

## Step 2: Create Swap Memory (Recommended)

Spark is memory-intensive. Adding 2GB swap prevents crashes:

### Create Swap File

```bash
sudo fallocate -l 2G /swapfile
```

### Set Permissions

```bash
sudo chmod 600 /swapfile
```

### Format as Swap

```bash
sudo mkswap /swapfile
```

### Enable Swap

```bash
sudo swapon /swapfile
```

### Make Swap Permanent

```bash
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

### Verify Swap

```bash
free -h
```

You should see the 2G swap listed.

---

## Step 3: Update System Packages

```bash
sudo apt update -y
sudo apt upgrade -y
```

---

## Step 4: Install Docker

### Install Docker Engine

```bash
sudo apt install docker.io -y
```

### Start Docker Service

```bash
sudo systemctl start docker
sudo systemctl enable docker
```

### Verify Docker Installation

```bash
docker --version
```

---

## Step 5: Install Docker Compose

```bash
sudo apt install docker-compose -y
```

### Verify Installation

```bash
docker-compose --version
```

---

## Step 6: Configure Docker Permissions

```bash
sudo usermod -aG docker $USER
newgrp docker
```

Verify:

```bash
docker ps
```

---

## Step 7: Create Spark Workspace Directory

```bash
mkdir -p ~/fit3182-spark
cd ~/fit3182-spark
```

Verify:

```bash
pwd
```

Expected:
```
/home/ubuntu/fit3182-spark
```

---

## Step 8: Create config.py File

```bash
nano config.py
```

**Paste the following content** (replace with your actual values):

```python
# System Network Configurations
IP_ADDRESS = "13.55.144.115"  # Your Kafka EC2 PUBLIC IP
MONGO_URI = "mongodb+srv://username:password@cluster0.xxxxx.mongodb.net/?appName=Cluster0"

# Database Configuration
LOCAL_HOST = "127.0.0.1"
DB_NAME = "fit3182_awas"
RETRY_COUNT = 3

# Streaming Configuration
WATERMARK = "5 minutes"
WINDOW_INTERVAL_AB = None
WINDOW_INTERVAL_BC = None

# Speed Limits by Camera
CAMERA_A_SPEED_LIMIT = None
CAMERA_B_SPEED_LIMIT = None
CAMERA_C_SPEED_LIMIT = None
```

**Save and exit:**
- Press `CTRL+O`
- Press `Enter`
- Press `CTRL+X`

---

## Step 9: Verify config.py

```bash
cat config.py
```

Verify:
- `IP_ADDRESS` contains your Kafka PUBLIC IP
- `MONGO_URI` contains your MongoDB connection string

---

## Step 10: Create utils.py File

```bash
nano utils.py
```

**Paste your helper functions** (or leave blank for now):

```python
# Add your utility functions here

def parse_camera_data(data):
    """Parse incoming camera event data"""
    pass

def validate_vehicle_speed(speed):
    """Validate vehicle speed"""
    pass

# Add more functions as needed
```

**Save and exit:**
- Press `CTRL+O`
- Press `Enter`
- Press `CTRL+X`

---

## Step 11: Create streaming_app.py File

```bash
nano streaming_app.py
```

**Paste your Spark streaming code** (example template):

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import config

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FIT3182-Streaming") \
    .config("spark.mongodb.output.uri", config.MONGO_URI) \
    .config("spark.mongodb.output.database", config.DB_NAME) \
    .getOrCreate()

# Configure logging
spark.sparkContext.setLogLevel("INFO")

try:
    # Read from Kafka Topics
    kafka_brokers = f"{config.IP_ADDRESS}:9092"
    
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", "camera-events-A,camera-events-B,camera-events-C") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON from Kafka messages
    parsed_df = df_stream.select(
        from_json(col("value").cast("string"), 
                  StructType([
                      StructField("camera_id", StringType()),
                      StructField("vehicle_id", StringType()),
                      StructField("timestamp", StringType()),
                      StructField("speed", DoubleType())
                  ])).alias("data")
    ).select("data.*")
    
    # Write to MongoDB
    query = parsed_df.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .start()
    
    # Keep streaming
    query.awaitTermination()
    
except Exception as e:
    print(f"Error in streaming pipeline: {e}")
    spark.stop()
```

**Save and exit:**
- Press `CTRL+O`
- Press `Enter`
- Press `CTRL+X`

---

## Step 12: Create docker-compose.yml File

```bash
nano docker-compose.yml
```

**Paste the following content:**

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
      sh -c "pip uninstall -y kafka && 
             pip install kafka-python && 
             spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 streaming_app.py"
    restart: unless-stopped
    environment:
      SPARK_LOCAL_IP: 0.0.0.0
```

**Save and exit:**
- Press `CTRL+O`
- Press `Enter`
- Press `CTRL+X`

---

## Step 13: Verify All Files Created

```bash
ls -la
```

Expected output:
```
config.py
docker-compose.yml
streaming_app.py
utils.py
```

---

## Step 14: Verify config.py Contents

```bash
cat config.py
```

Check:
- IP_ADDRESS is set to Kafka PUBLIC IP
- MONGO_URI is correct
- All other constants present

---

## Step 15: Start Spark Streaming Container

```bash
docker compose up -d
```

Expected output:
```
[+] Running 1/1
 ✔ Container spark_streaming_app  Started
```

---

## Step 16: Wait for Docker Image to Extract

First time startup takes 1-2 minutes. Wait:

```bash
sleep 30
```

---

## Step 17: Verify Container is Running

```bash
docker ps
```

Expected output:
```
CONTAINER ID   IMAGE            STATUS              NAMES
xxxxxxxxxxxxx   fit3182/pyspark  Up X seconds       spark_streaming_app
```

---

## Step 18: Check Spark Logs

Monitor the Spark application in real-time:

```bash
docker compose logs -f spark-stream
```

Watch for messages indicating:
- Spark session initialized
- Connected to Kafka brokers
- Processing micro-batches
- Writing to MongoDB

**Press CTRL+C to stop viewing logs** (container keeps running).

---

## Step 19: Verify Kafka Connection

Check if Spark can reach Kafka:

```bash
docker exec spark_streaming_app bash -c "nc -zv YOUR_KAFKA_IP 9092"
```

Replace `YOUR_KAFKA_IP` with your Kafka PUBLIC IP.

Expected output:
```
Connection to YOUR_KAFKA_IP port 9092 [tcp/*] succeeded!
```

---

## Step 20: Verify MongoDB Connection

Test MongoDB connection from container:

```bash
docker exec spark_streaming_app python3 << 'EOF'
from pymongo import MongoClient
import config

try:
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    print("✅ MongoDB connection successful!")
    print(f"Databases: {client.list_database_names()}")
except Exception as e:
    print(f"❌ MongoDB connection failed: {e}")
EOF
```

Expected output:
```
✅ MongoDB connection successful!
Databases: [...]
```

---

## ✅ Spark Node Setup Complete!

**Verify:**
- Container running: `docker ps`
- Logs show no critical errors: `docker compose logs -f`
- Can connect to Kafka: `docker exec spark_streaming_app bash -c "nc -zv ..."`
- Can connect to MongoDB

---

---

# LOCAL MACHINE SETUP (MANUAL)

## Step 1: Configure MongoDB Atlas Network Access

1. Log into **MongoDB Atlas Dashboard**
2. Navigate to: **Security → Network Access**
3. Click **Add IP Address**
4. Enter: `0.0.0.0/0`
5. Click **Confirm**

---

## Step 2: Create `.env` File

In your local project root:

```bash
# Windows PowerShell
@"
KAFKA_IP=13.55.144.115
MONGO_URI=mongodb+srv://username:password@cluster0.xxxxx.mongodb.net/?appName=Cluster0
DB_NAME=fit3182_awas
"@ | Out-File -Encoding UTF8 .env

# Or on Mac/Linux
cat > .env << EOF
KAFKA_IP=13.55.144.115
MONGO_URI=mongodb+srv://username:password@cluster0.xxxxx.mongodb.net/?appName=Cluster0
DB_NAME=fit3182_awas
EOF
```

---

## Step 3: Update Local `src/config.py`

Edit `src/config.py` with your values:

```python
# System Network Configurations
IP_ADDRESS = "13.55.144.115"  # YOUR Kafka EC2 PUBLIC IP
LOCAL_HOST = "127.0.0.1"

MONGO_URI = "mongodb+srv://username:password@cluster0.xxxxx.mongodb.net/?appName=Cluster0"

# MONGODB
DB_NAME = "fit3182_awas"
RETRY_COUNT = 3

# STREAMING
BATCH_PUBLISH_RATE_A = 1
BATCH_PUBLISH_RATE_B = 1/10
BATCH_PUBLISH_RATE_C = 1/14
WATERMARK = "5 minutes"
WINDOW_INTERVAL_AB = None
WINDOW_INTERVAL_BC = None

# PARAMETERIZABLE SPEED LIMITS
CAMERA_A_SPEED_LIMIT = None
CAMERA_B_SPEED_LIMIT = None
CAMERA_C_SPEED_LIMIT = None
```

---

## Step 4: Start Docker Desktop

- Open Docker Desktop
- Wait for status: **Engine Running**

---

## Step 5: Deploy Local Containers

```powershell
# Navigate to project root
cd C:\Users\YourUsername\FIT3182\34838260_35123656_assignment03

# Stop any existing containers
docker compose down

# Start containers
docker compose up -d
```

---

## Step 6: Access Jupyter

Open browser: `http://localhost:8889`

---

## Step 7: Seed Database

1. Navigate to `src/` folder
2. Open `collections.ipynb`
3. Run all cells

---

## Step 8: Start Producers

In separate browser tabs, run:
- `src/producer_a.ipynb` → Run All
- `src/producer_b.ipynb` → Run All
- `src/producer_c.ipynb` → Run All

---

## Step 9: View Dashboards

1. Open `src/visualisations.ipynb`
2. Run all cells
3. View live dashboards

---

# 🔍 VERIFICATION CHECKLIST

### Kafka Node
- [ ] SSH connected
- [ ] Docker installed and running
- [ ] docker-compose.yml created with correct PUBLIC IP
- [ ] Containers running: `docker ps`
- [ ] Topics created: `docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list`

### Spark Node
- [ ] SSH connected
- [ ] Swap memory created: `free -h`
- [ ] Docker installed and running
- [ ] config.py has correct Kafka IP and MongoDB URI
- [ ] Container running: `docker ps`
- [ ] Logs show no errors: `docker compose logs`

### Local Machine
- [ ] Docker Desktop running
- [ ] Jupyter accessible: http://localhost:8889
- [ ] MongoDB collections created
- [ ] Producers publishing messages
- [ ] Dashboards displaying data

---

# 🛠️ TROUBLESHOOTING

## Kafka Issues

### Topics not showing
```bash
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Kafka logs
```bash
docker logs kafka
```

### Recreate topics
```bash
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic camera-events-A
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic camera-events-A --partitions 3 --replication-factor 1
```

---

## Spark Issues

### Check container logs
```bash
docker compose logs -f spark-stream
```

### Restart container
```bash
docker compose restart
```

### Check resource usage
```bash
docker stats
```

---

## MongoDB Issues

### Test connection
```bash
python3 << 'EOF'
from pymongo import MongoClient
client = MongoClient("YOUR_MONGO_URI")
print(client.admin.command('ping'))
EOF
```

### Verify network access
- Log into MongoDB Atlas
- Go to Security → Network Access
- Confirm `0.0.0.0/0` is added

---

## Docker Issues

### Clear all containers
```bash
docker compose down -v
```

### Remove unused images
```bash
docker system prune -a
```

---

# ✅ You're All Set!

Your Kafka and Spark nodes are now manually configured and running. Proceed to:

1. Seed MongoDB collections
2. Start local producers
3. Monitor real-time dashboards
4. Analyze streaming data
