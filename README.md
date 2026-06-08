# Real-Time Vehicle Streaming Analytics System

## 1. Configure MongoDB Atlas Network Firewall

Before executing any scripts, log into your MongoDB Atlas Cloud Dashboard.

### Steps

1. Navigate to:

   ```text
   Security → Network Access
   ```

2. Click **Add IP Address**

3. Add the following IP address:

   ```text
   0.0.0.0/0
   ```

This allows incoming write requests from:

* Your local machine
* Your changing AWS cloud nodes

---

# RUNNING THE PROJECT

## Step 1: Start Your Cloud Infrastructure

Connect to your AWS Kafka Node via SSH terminal and start the streaming services.

### Start Kafka Services

```bash
cd fit3182-kafka
docker compose up -d
```

### Verify Kafka Topics

```bash
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

---

## Step 2: Initialize Local Workspace Services

### Start Docker Desktop

Launch Docker Desktop on your local Windows/Mac machine and wait until the status changes to:

```text
Engine Running
```

### Deploy Local Jupyter Environment

Open a local terminal and navigate to the main project directory containing the `docker-compose.yaml` file.

```powershell
# Move out of src/ if currently inside it
cd ..

# Deploy local workspace container
docker compose down
docker compose up -d
```

---

## Step 3: Seed Database Master Records

1. Open your web browser and navigate to:

   ```text
   http://localhost:8889
   ```

2. Navigate into the `src/` directory

3. Open:

   ```text
   collections.ipynb
   ```

4. Execute all notebook cells sequentially

This process will:

* Read `camera.csv`
* Read `vehicle.csv`
* Resolve duplicate registration states
* Build and index collections inside MongoDB Atlas

### Verify Database Collections

Log into the MongoDB Atlas Dashboard and check:

```text
Browse Collections
```

---

## Step 4: Launch Real-Time Stream Ingestion

Since the streaming processes run continuously, they are executed via independent notebook tabs within your local browser ecosystem.

### Access Workspace Environment

Open your web browser and navigate to:

```text
http://localhost:8889
```

Navigate into the `src/` directory.

### Start Camera Producers Concurrent Stream

Open the following three notebooks in separate browser tabs and click **Run All** or execute the streaming cells.

#### Tab 1

```text
producer_a.ipynb
```

#### Tab 2

```text
producer_b.ipynb
```

#### Tab 3

```text
producer_c.ipynb
```

### Verify Streaming

Ensure the active output stream blocks under the execution loops display:

```text
Message published successfully.
```

---

## Step 5: Start Real-Time Visualization Dashboards

Return to the Jupyter environment browser tab:

```text
http://localhost:8889
```

Open:

```text
visualisations.ipynb
```

Click:

```text
Run All
```

---

# Dashboard Features

## Real-Time Time-Series Chart

### Features

* Sliding metrics
* Automated anomaly detection
* Live traffic analytics

---

## Daily Top 10 Violators Report

### Features

* Interactive filtering
* Drop-down query parameters
* Dynamic reporting

---

## Geographic Map Overlay

### Features

* Live-updating Folium heat map
* Route severity visualization
* Geographic risk analysis
