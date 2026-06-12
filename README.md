# Real-Time Vehicle Streaming Analytics System

## 📋 Complete System Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                    Local Machine (Windows/Mac)                       │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ Docker Container (Jupyter Environment)                         │  │
│  │ ├─ collections.ipynb (Seed MongoDB)                           │  │
│  │ ├─ producer_a.ipynb (Camera A Stream)                         │  │
│  │ ├─ producer_b.ipynb (Camera B Stream)                         │  │
│  │ ├─ producer_c.ipynb (Camera C Stream)                         │  │
│  │ └─ visualisations.ipynb (Live Dashboards)                     │  │
│  └────────────────────────────────────────────────────────────────┘  │
└──────────────────────────┬─────────────────────────────────────────────┘
                           │
                ┌──────────┴──────────┐
                │                     │
    ┌───────────▼────────────┐  ┌─────▼───────────────────┐
    │  AWS EC2: Kafka Node   │  │  AWS EC2: Spark Node    │
    │  ┌──────────────────┐  │  │  ┌──────────────────┐   │
    │  │ Zookeeper (2181) │  │  │  │ Spark Streaming  │   │
    │  │ Kafka (9092)     │  │  │  │ - Consumes from  │   │
    │  │ - Topics A, B, C │  │  │  │   Kafka          │   │
    │  └──────────────────┘  │  │  │ - Writes to      │   │
    │                        │  │  │   MongoDB        │   │
    │                        │  │  └──────────────────┘   │
    └────────────────────────┘  └────────────┬────────────┘
                                              │
                                    ┌─────────▼──────────┐
                                    │  MongoDB Atlas     │
                                    │  ├─ violations     │
                                    │  └─ statistics     │
                                    └────────────────────┘
```

---

## ⚠️ Prerequisites

Before starting, ensure you have:

### 1. AWS Resources Created
- **Kafka EC2 Instance**: `t3.small` (minimum), Ubuntu Server, 20GB EBS
- **Spark EC2 Instance**: `t3.small` (minimum), Ubuntu Server, 25GB EBS
- Security groups configured with appropriate inbound rules (ports 22, 9092, 2181)

### 2. MongoDB Atlas Setup
- MongoDB Atlas cluster created and accessible
- Database connection URI obtained
- Network access configured to allow connections from anywhere (`0.0.0.0/0`)

### 3. Local Setup
- Docker Desktop installed and running
- SSH key pair (.pem file) for EC2 access

---

# 🚀 COMPLETE SETUP GUIDE

## PHASE 1: KAFKA NODE SETUP (AWS EC2)

### Step 1.1: SSH into Kafka Instance

```bash
ssh -i "your-key-pair.pem" ubuntu@YOUR_KAFKA_PUBLIC_IP
```

Example:
```bash
ssh -i "fit3182-cloud-key.pem" ubuntu@13.55.144.115
```

### Step 1.2: Download Setup Script
Either just copy and paste the contents of setup-kafka.sh from your repository or follow the steps below.
nano setup-kafka.sh
# Paste the entire contents of setup-kafka.sh from your repository
# Press CTRL+O, Enter, CTRL+X to save and exit
```

### Step 1.3: Run Kafka Setup (Automated)

```bash
chmod +x setup-kafka.sh
./setup-kafka.sh
```

**This script automatically:**
- ✅ Updates system packages
- ✅ Installs Docker and Docker Compose
- ✅ Creates `~/fit3182-kafka` workspace
- ✅ Detects your EC2 public IP automatically
- ✅ Generates `docker-compose.yml` with correct configuration
- ✅ Starts Zookeeper and Kafka containers
- ✅ Creates Kafka topics: `camera-events-A`, `camera-events-B`, `camera-events-C`

### Step 1.4: Verify Kafka is Running

```bash
docker ps
```

You should see:
```
CONTAINER ID   IMAGE              STATUS          NAMES
xxxxxxxxxxxxx   fit3182/zookeeper  Up X seconds   zookeeper
xxxxxxxxxxxxx   fit3182/kafka      Up X seconds   kafka
```

### Step 1.5: Verify Topics Created

```bash
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Expected output:
```
camera-events-A
camera-events-B
camera-events-C
```

**✅ Kafka Node Setup Complete! Keep this terminal open or note the public IP.**

---

## PHASE 2: SPARK NODE SETUP (AWS EC2)

### Step 2.1: SSH into Spark Instance

```bash
ssh -i "your-key-pair.pem" ubuntu@YOUR_SPARK_PUBLIC_IP
```

### Step 2.2: Download Spark Setup Script
Either just copy and paste the contents of setup-spark.sh from your repository or follow the steps below.
```bash
nano setup-spark.sh
# Paste the entire contents of setup-spark.sh from your repository
# Press CTRL+O, Enter, CTRL+X to save and exit
```

### Step 2.3: Run Spark Setup (Automated with Prompts)

```bash
chmod +x setup-spark.sh
./setup-spark.sh
```

**When prompted, provide:**

1. **Kafka Node PUBLIC IP Address**
   ```
   👉 Enter your AWS Kafka Node PUBLIC IP address: 13.55.144.115
   ```

2. **MongoDB Atlas Connection URI**
   ```
   👉 Enter your complete MongoDB Atlas Connection URI: 
   mongodb+srv://username:password@cluster0.xxxxx.mongodb.net/?appName=Cluster0
   ```

**This script automatically:**
- ✅ Creates 2GB swap memory (prevents OOM errors)
- ✅ Installs Docker and Docker Compose
- ✅ Creates `~/fit3182-spark` workspace
- ✅ Generates `config.py` with your Kafka IP and MongoDB URI
- ✅ Generates `docker-compose.yml` with all constants:
  - `LOCAL_HOST`, `DB_NAME`, `RETRY_COUNT`
  - `WATERMARK`, `WINDOW_INTERVAL_AB`, `WINDOW_INTERVAL_BC`
  - `CAMERA_A_SPEED_LIMIT`, `CAMERA_B_SPEED_LIMIT`, `CAMERA_C_SPEED_LIMIT`
- ✅ Creates placeholder files: `utils.py`, `streaming_app.py`

### Step 2.4: Verify Generated Files

```bash
cd ~/fit3182-spark
ls -la
cat config.py
```

You should see:
```python
# System Network Configurations
IP_ADDRESS = "13.55.144.115"  # Your Kafka EC2 public IP
MONGO_URI = "mongodb+srv://..."

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

### Step 2.5: (OPTIONAL) Upload Your Streaming Code

If you have a pre-built `streaming_app.py`:

```bash
# From your local machine
scp -i "fit3182-cloud-key.pem" streaming_app.py ubuntu@YOUR_SPARK_IP:~/fit3182-spark/
scp -i "fit3182-cloud-key.pem" utils.py ubuntu@YOUR_SPARK_IP:~/fit3182-spark/
```

Otherwise, copy-paste your code into the EC2 instance using nano.

### Step 2.6: Start Spark Streaming Pipeline

```bash
cd ~/fit3182-spark
docker compose up -d
```

### Step 2.7: Monitor Spark Logs

```bash
docker compose logs -f spark-stream
```

Watch for messages indicating successful Kafka connection and data processing.

**Press CTRL+C to stop viewing logs (container continues running).**

**✅ Spark Node Setup Complete!**

---

## PHASE 3: LOCAL MACHINE SETUP

### Step 3.1: Configure MongoDB Atlas Network Access

1. Log into **MongoDB Atlas Dashboard**
2. Navigate to: **Security → Network Access**
3. Click **Add IP Address**
4. Add: `0.0.0.0/0` (allows connections from anywhere)

This ensures your local machine and AWS instances can connect to MongoDB.

### Step 3.2: Create `.env` File (Optional but Recommended)

In your local project root directory, create a `.env` file:

```bash
# On your local machine, in the project root
cat > .env << EOF
KAFKA_IP=13.55.144.115
MONGO_URI=mongodb+srv://username:password@cluster0.xxxxx.mongodb.net/?appName=Cluster0
DB_NAME=fit3182_awas
EOF
```

### Step 3.3: Update Local `src/config.py`

Update `src/config.py` with your AWS infrastructure details:

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

### Step 3.4: Start Docker Desktop

- Open Docker Desktop on your local machine
- Wait for status: **Engine Running**

### Step 3.5: Deploy Local Jupyter Environment

```powershell
# Navigate to project root (where docker-compose.yaml is located)
cd C:\Users\YourUsername\FIT3182\34838260_35123656_assignment03

# Clean up any existing containers
docker compose down

# Deploy containers
docker compose up -d
```

### Step 3.6: Access Jupyter Environment

Open your web browser and navigate to:

```
http://localhost:8889
```

You should see the Jupyter Lab interface with your project files.

**✅ Local Setup Complete!**

---

## PHASE 4: POPULATE DATABASE

### Step 4.1: Seed MongoDB Collections

1. In Jupyter Lab (http://localhost:8889), navigate to `src/` folder
2. Open `collections.ipynb`
3. Execute all cells sequentially (Run → Run All Cells)

This will:
- Read `camera.csv` and `vehicle.csv`
- Create collections in MongoDB Atlas
- Index the data for fast queries

### Step 4.2: Verify Collections Created

Log into MongoDB Atlas Dashboard:
1. Click **Browse Collections**
2. Verify you see:
   - `camera` collection
   - `vehicle` collection
   - (Will add more collections as streaming starts)

**✅ Database Seeding Complete!**

---

## PHASE 5: START REAL-TIME STREAMING

### Step 5.1: Open Producer Notebooks

In Jupyter Lab (http://localhost:8889), open the following **in separate browser tabs**:

**Tab 1 - Camera A Producer**
```
src/producer_a.ipynb
```
Click: **Run → Run All Cells** (or press Ctrl+Shift+Enter)

**Tab 2 - Camera B Producer**
```
src/producer_b.ipynb
```
Click: **Run → Run All Cells**

**Tab 3 - Camera C Producer**
```
src/producer_c.ipynb
```
Click: **Run → Run All Cells**

### Step 5.2: Verify Messages Publishing

Watch the output cells for:
```
Message published successfully.
```

These will appear repeatedly as data streams to Kafka.

### Step 5.3: Verify Spark Processing

On your Spark EC2 instance, check logs:

```bash
docker compose logs -f spark-stream
```

You should see processing logs indicating:
- Messages consumed from Kafka
- Data written to MongoDB

---

## PHASE 6: VIEW REAL-TIME DASHBOARDS

### Step 6.1: Start Visualization Dashboard

In Jupyter Lab (http://localhost:8889):
1. Open `src/visualisations.ipynb`
2. Click: **Run → Run All Cells**

### Step 6.2: View Live Dashboards

The notebook will display:

#### 🔴 Real-Time Time-Series Chart
- Live violation count metrics
- Automated anomaly detection
- Traffic analytics

#### 📊 Daily Top 10 Violators Report
- Interactive filtering
- Drop-down query parameters
- Dynamic speed violation rankings

#### 🗺️ Geographic Map Overlay
- Live-updating Folium heat map
- Route severity visualization
- Geographic risk analysis

**✅ System is now fully operational!**

---

## 🔍 VERIFICATION CHECKLIST

### Kafka Node (AWS EC2)
- [ ] SSH connection established
- [ ] Containers running: `docker ps` shows zookeeper and kafka
- [ ] Topics exist: `docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list`

### Spark Node (AWS EC2)
- [ ] SSH connection established
- [ ] Container running: `docker ps` shows spark_streaming_app
- [ ] Logs show no critical errors: `docker compose logs -f`
- [ ] Connected to Kafka successfully

### Local Machine
- [ ] Docker Desktop running
- [ ] Jupyter accessible: http://localhost:8889
- [ ] MongoDB collections created and visible in Atlas
- [ ] Producers publishing messages successfully
- [ ] Visualizations displaying live data

### MongoDB Atlas
- [ ] Collections visible in Browse Collections
- [ ] New documents being written in real-time
- [ ] Network access configured for `0.0.0.0/0`

---

## 🛠️ TROUBLESHOOTING

### Kafka Connection Issues
```bash
# Check Kafka logs
docker logs kafka

# Test connectivity from local machine
telnet YOUR_KAFKA_IP 9092

# Verify topics exist
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Spark Connection Issues
```bash
# Check Spark logs
docker compose logs -f spark-stream

# Verify config.py has correct Kafka IP
cat ~/fit3182-spark/config.py
```

### MongoDB Connection Issues
```bash
# Test connection string in Python
python -c "from pymongo import MongoClient; client = MongoClient('YOUR_MONGO_URI'); print(client.list_database_names())"

# Verify network access in MongoDB Atlas
# Security → Network Access → Confirm 0.0.0.0/0 is added
```

### Docker Container Issues
```bash
# Restart all containers
docker compose restart

# Check resource usage
docker stats

# Stop and remove all containers
docker compose down -v
```

---

## 📝 KEY CONFIGURATION FILES

### `.env` (Optional)
```
KAFKA_IP=YOUR_KAFKA_PUBLIC_IP
MONGO_URI=YOUR_MONGODB_CONNECTION_STRING
```

### `src/config.py`
```python
IP_ADDRESS = "YOUR_KAFKA_PUBLIC_IP"
MONGO_URI = "YOUR_MONGODB_URI"
DB_NAME = "fit3182_awas"
# ... other constants
```

### AWS EC2 Instances
- **Kafka**: `~/fit3182-kafka/docker-compose.yml`
- **Spark**: `~/fit3182-spark/docker-compose.yml` and `~/fit3182-spark/config.py`

---

## 📚 Additional Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [MongoDB Atlas Connection Guide](https://docs.mongodb.com/manual/reference/connection-string/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

---

## 🎯 Next Steps

After completing setup:
1. Explore the generated MongoDB collections
2. Customize the Spark streaming logic in `streaming_app.py`
3. Adjust speed limits and window intervals in `config.py`
4. Monitor real-time dashboards for anomalies
5. Analyze historical data in the visualizations notebook


Notice: 
local directory is for local running of the producer and streaming similar to assignment 2. And we used that to calculate the performance analysis