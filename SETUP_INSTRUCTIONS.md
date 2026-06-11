# FIT3182 Complete Setup Instructions Using Shell Scripts

This guide walks you through setting up your entire real-time vehicle streaming analytics system using the provided shell scripts.

---

## 📋 System Architecture Overview

```
┌─────────────────────────────────────┐
│   Local Machine (Windows/Mac)       │
│  ├─ Docker: Jupyter + Producers    │
│  ├─ producer_a.ipynb               │
│  ├─ producer_b.ipynb               │
│  ├─ producer_c.ipynb               │
│  └─ visualisations.ipynb           │
└──────────────────┬──────────────────┘
                   │
          ┌────────▼────────┐
          │  AWS Kafka EC2  │
          │  (setup-kafka.sh)
          │  ├─ Zookeeper   │
          │  └─ Kafka       │
          └────────┬────────┘
                   │
          ┌────────▼────────┐
          │  AWS Spark EC2  │
          │ (setup-spark.sh)│
          │  Spark Stream   │
          └────────┬────────┘
                   │
          ┌────────▼──────────┐
          │ MongoDB Atlas     │
          │ (Updated w/ ML)   │
          └───────────────────┘
```

---

## ⚠️ Prerequisites

Before running the scripts, ensure you have:

### 1. AWS EC2 Instances Created
- **Kafka Instance**: `t3.small` or better, Ubuntu Server, 20GB EBS
- **Spark Instance**: `t3.small` or better, Ubuntu Server, 25GB EBS

### 2. Security Groups Configured

#### Kafka Security Group (`fit3182-cluster-sg`)
| Port | Protocol   | Access |
|------|-----------|--------|
| 22   | SSH       | Your IP or Anywhere |
| 9092 | Custom TCP | Spark Node / Your IP |
| 2181 | Custom TCP | Internal Use |

#### Spark Security Group (`fit3182-cluster-sg`)
| Port | Protocol   | Access |
|------|-----------|--------|
| 22   | SSH       | Your IP or Anywhere |
| 8889 | Custom TCP | Anywhere (optional) |

### 3. Required Information
- AWS EC2 **public IP addresses** (for both Kafka and Spark instances)
- **MongoDB Atlas URI** (connection string)
- SSH key pair (.pem file) for EC2 access

### 4. MongoDB Atlas Configuration
1. Log into MongoDB Atlas Dashboard
2. Go to **Security → Network Access**
3. Click **Add IP Address**
4. Add `0.0.0.0/0` to allow connections from anywhere

---

## 🚀 Step 1: Setup Kafka Node

### 1.1 SSH into Kafka EC2 Instance

```bash
ssh -i "your-key-pair.pem" ubuntu@YOUR_KAFKA_PUBLIC_IP
```

Example:
```bash
ssh -i "fit3182-cloud-key.pem" ubuntu@13.55.144.115
```

### 1.2 Download the Kafka Setup Script

```bash
# Option A: Using curl to download directly
curl -O https://raw.githubusercontent.com/YOUR_REPO/setup-kafka.sh

# Option B: Create the file manually
nano setup-kafka.sh
# Then paste the contents of setup-kafka.sh
```

### 1.3 Make the Script Executable

```bash
chmod +x setup-kafka.sh
```

### 1.4 Run the Setup Script

```bash
./setup-kafka.sh
```

**What this script does:**
- ✅ Updates system packages
- ✅ Installs Docker and Docker Compose
- ✅ Creates workspace directory (`~/fit3182-kafka`)
- ✅ Automatically detects your EC2 public IP
- ✅ Generates `docker-compose.yml` with correct IP configuration
- ✅ Starts Zookeeper and Kafka containers
- ✅ Verifies containers are running with `docker ps`

### 1.5 Verify Kafka is Running

```bash
docker ps
```

You should see:
```
CONTAINER ID   IMAGE              STATUS              NAMES
xxxxxxxxxxxxx   fit3182/zookeeper  Up X seconds       zookeeper
xxxxxxxxxxxxx   fit3182/kafka      Up X seconds       kafka
```

### 1.6 Test Kafka Consumer (Optional)

```bash
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic camera-events-A --from-beginning
```

Press `CTRL+C` to stop.

---

## 🚀 Step 2: Setup Spark Streaming Node

### 2.1 SSH into Spark EC2 Instance

```bash
ssh -i "your-key-pair.pem" ubuntu@YOUR_SPARK_PUBLIC_IP
```

### 2.2 Download the Spark Setup Script

```bash
curl -O https://raw.githubusercontent.com/YOUR_REPO/setup-spark.sh
```

Or manually create it:
```bash
nano setup-spark.sh
# Paste contents of setup-spark.sh
```

### 2.3 Make the Script Executable

```bash
chmod +x setup-spark.sh
```

### 2.4 Run the Setup Script

```bash
./setup-spark.sh
```

**What this script does:**
- ✅ Creates 2GB swap memory (prevents OOM errors)
- ✅ Installs Docker and Docker Compose
- ✅ Creates workspace directory (`~/fit3182-spark`)
- ✅ **Prompts you for:**
  - AWS Kafka Node PUBLIC IP address
  - MongoDB Atlas Connection URI
- ✅ Generates `config.py` with your inputs
- ✅ Creates placeholder files: `utils.py`, `streaming_app.py`
- ✅ Generates `docker-compose.yml`

### 2.5 Provide Required Information When Prompted

When the script runs, it will ask:

```
👉 Enter your AWS Kafka Node PUBLIC IP address: 
```

Enter your Kafka instance's public IP (e.g., `13.55.144.115`)

```
👉 Enter your complete MongoDB Atlas Connection URI:
```

Paste your full MongoDB URI:
```
mongodb+srv://username:password@cluster0.xxxxx.mongodb.net/?appName=Cluster0
```

### 2.6 Verify Setup

Check the generated files:

```bash
cd ~/fit3182-spark
ls
```

You should see:
```
config.py
docker-compose.yml
streaming_app.py
utils.py
```

Verify config was generated correctly:
```bash
cat config.py
```

---

## 📝 Step 3: Customize Spark Streaming Application

The script creates placeholder files. Now you need to populate them.

### 3.1 Update `streaming_app.py`

This is your main streaming logic. You need to convert your Jupyter notebook streaming code to a Python script.

```bash
nano streaming_app.py
```

**Key components to include:**
1. Initialize SparkSession with Kafka packages
2. Read from Kafka topics (camera-events-A, B, C)
3. Parse and process incoming data
4. Apply Spark ML predictions (once trained)
5. Write results to MongoDB Atlas

**Example structure:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import config

spark = SparkSession.builder \
    .appName("FIT3182-Streaming") \
    .config("spark.mongodb.write.connection.uri", config.MONGO_ATLAS_URI) \
    .getOrCreate()

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{config.IP_ADDRESS}:9092") \
    .option("subscribe", "camera-events-A,camera-events-B,camera-events-C") \
    .load()

# Process and write to MongoDB
query = df.writeStream \
    .format("mongodb") \
    .option("database", config.DB_NAME) \
    .option("collection", "violations") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .start()

query.awaitTermination()
```

### 3.2 Update `utils.py`

Add any helper functions:

```bash
nano utils.py
```

Example helpers:
- Data parsing functions
- Feature engineering utilities
- MongoDB connection helpers
- Data validation functions

### 3.3 Verify `config.py`

Should contain:
```bash
cat config.py
```

Expected content:
```python
IP_ADDRESS = "13.55.144.115"  # Your Kafka EC2 public IP
MONGO_ATLAS_URI = "mongodb+srv://...@cluster0...mongodb.net/..."
DB_NAME = "fit3182_awas"
```

---

## 🏎 Step 4: Launch Spark Streaming Pipeline

### 4.1 Start the Spark Streaming Container

```bash
cd ~/fit3182-spark
docker compose up -d
```

**First run only:**
- Docker pulls the `fit3182/pyspark` image (~2-3 GB)
- Extraction takes 1-2 minutes
- Wait for completion before checking logs

### 4.2 Monitor Real-Time Logs

Watch Spark's output and streaming batches:

```bash
docker compose logs -f spark-stream
```

You should see:
```
spark-stream_1  | Spark batch training session initialized.
spark-stream_1  | Fetching historical violation entries...
spark-stream_1  | Processing micro-batch: count=45
```

Press `CTRL+C` to stop watching logs (container still runs).

### 4.3 Check Container Status

```bash
docker ps
```

You should see:
```
CONTAINER ID   IMAGE            STATUS              NAMES
xxxxxxxxxxxxx   fit3182/pyspark  Up X minutes       spark_streaming_app
```

---

## 🖥️ Step 5: Run Local Machine Producer & Visualization

### 5.1 Prepare Your Local Machine

On your local Windows/Mac:

1. **Start Docker Desktop**
   - Wait for status: "Engine Running"

2. **Open Terminal and navigate to project root**
   ```powershell
   cd "c:\Users\phabd\Monash\FIT3182\FIT3182 AS3\34838260_35123656_assignment03"
   ```

3. **Start Docker containers**
   ```powershell
   docker compose down
   docker compose up -d
   ```

### 5.2 Access Jupyter Environment

1. Open browser: `http://localhost:8889`
2. Navigate to `src/` folder

### 5.3 Seed MongoDB Collections

1. Open `collections.ipynb`
2. Execute all cells
3. Verify collections exist in MongoDB Atlas

### 5.4 Start Producers (in separate browser tabs)

#### Tab 1: Producer A
- Open `producer_a.ipynb`
- Click "Run All"
- Watch for: "Message published successfully."

#### Tab 2: Producer B
- Open `producer_b.ipynb`
- Click "Run All"
- Watch for: "Message published successfully."

#### Tab 3: Producer C
- Open `producer_c.ipynb`
- Click "Run All"
- Watch for: "Message published successfully."

### 5.5 View Live Visualizations

1. Open `visualisations.ipynb`
2. Click "Run All"
3. View real-time dashboards:
   - Time-series violations chart
   - Top 10 violators report
   - Geographic heatmap

---

## 🔍 Verification Checklist

### Kafka Node (AWS EC2)
- [ ] SSH connection established
- [ ] Docker containers running: `docker ps`
- [ ] Zookeeper and Kafka visible in output
- [ ] Kafka topics exist: `docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list`

### Spark Node (AWS EC2)
- [ ] SSH connection established
- [ ] Docker container running: `docker ps`
- [ ] `spark_streaming_app` container status: Up
- [ ] Logs show no critical errors: `docker compose logs -f`
- [ ] Connected to Kafka IP successfully

### Local Machine
- [ ] Docker Desktop running
- [ ] Jupyter accessible: `http://localhost:8889`
- [ ] MongoDB collections created and seeded
- [ ] Producers publishing messages (no errors)
- [ ] Visualizations displaying live data

### MongoDB Atlas
- [ ] Collections visible: violations, camera_baseline_stats
- [ ] New documents being written in real-time
- [ ] No connection errors in collection browser

---

## 🛠️ Troubleshooting

### Kafka Node Issues

**Problem: "Cannot connect to Kafka"**
```bash
# Check Kafka logs
docker compose logs kafka

# Verify Kafka is listening
docker exec -it kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

**Problem: "No topics found"**
```bash
# Create topics manually (if needed)
docker exec -it kafka kafka-topics.sh --create --bootstrap-server localhost:9092 --topic camera-events-A --partitions 1 --replication-factor 1
```

### Spark Node Issues

**Problem: "Out of memory" errors**
```bash
# Check if swap was created
free -h

# If not, create manually:
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

**Problem: "Cannot reach Kafka"**
- Verify Kafka IP in `config.py` is correct (PUBLIC IP, not private)
- Verify Spark security group allows traffic from Spark instance
- Check Kafka firewall: `sudo ufw status`

**Problem: "Cannot connect to MongoDB"**
- Verify MongoDB URI in `config.py` is correct
- Check MongoDB Atlas network access: `0.0.0.0/0` allowed
- Test connection: `ping cluster0.xxxxx.mongodb.net`

### Local Machine Issues

**Problem: "Containers won't start"**
```powershell
# Clean up
docker compose down --volumes
docker compose up -d
```

**Problem: "Cannot access Jupyter"**
- Check if container is running: `docker ps`
- Check logs: `docker compose logs`
- Try: `http://127.0.0.1:8889` instead

---

## 📊 Monitoring & Maintenance

### View Real-Time Logs

**Kafka Node:**
```bash
docker compose logs -f kafka
```

**Spark Node:**
```bash
docker compose logs -f spark-stream
```

### Stop Services

**Kafka:**
```bash
cd ~/fit3182-kafka
docker compose down
```

**Spark:**
```bash
cd ~/fit3182-spark
docker compose down
```

### Restart Services

```bash
docker compose restart
```

---

## ✅ Next Steps: Implement Spark ML

Once streaming is working, add ML predictions:

1. **Complete `spark_ml_pipeline.py`** with model methods
2. **Run batch training** with `train_ml.py`
3. **Load trained models** in `streaming_app.py`
4. **Apply predictions** to streaming data
5. **Write predictions** back to MongoDB Atlas

See `SPARK_ML_STEPS.md` for detailed ML implementation guide.

---

## 📞 Support

For issues:
1. Check logs: `docker compose logs -f`
2. Verify firewall rules in AWS Security Groups
3. Check MongoDB Atlas network access settings
4. Ensure all IPs/URIs in config files are correct
