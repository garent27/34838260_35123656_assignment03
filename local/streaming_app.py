# %% [markdown]
# # Part 2 : Streaming Application (2.1.2 → 2.1.4)

# %%
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, ArrayType, BooleanType
from pyspark.sql.streaming import StreamingQueryListener
from utils import *
from pymongo import MongoClient, UpdateOne
import config
import math
import time
import shutil
from datetime import datetime

# %% [markdown]
# ## Spark Structured Streaming Ingestion
# 
# Spark Structured Streaming consumes vehicle events continuously from the Kafka topics.
# 
# The ingestion pipeline:
# 
# 1. Reads JSON messages from Kafka
# 2. Parses incoming data using a predefined schema
# 3. Extracts nested vehicle events
# 4. Flattens batched event structures into individual streaming records
# 5. Converts timestamps and speed readings into appropriate data types
# 
# At this stage, each camera stream becomes a structured streaming DataFrame representing real-time vehicle detections.

# %%
try:
    KAFKA_BOOTSTRAP_SERVERS = f"{config.IP_ADDRESS}:9092"

    SPARK_PACKAGES = (
        "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.5,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
    )

    # When starting the Spark Session, include the relevant Kafka packages
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {SPARK_PACKAGES} pyspark-shell"

    # Tuned Spark Session for cloud infrastructure optimization
    spark = (
        SparkSession.builder
        .master("local[2]") # Restricts Spark to 2 worker cores to prevent CPU thrashing
        .appName("AWAS speed processing")
        .config("spark.driver.memory", "512m") # Caps the memory utilization footprint 
        .config("spark.executor.memory", "512m") 
        .config("spark.sql.shuffle.partitions", "10") # Prevents creating 200 overhead threads
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("Spark session started.")
except Exception as ex:
    print(str(ex))

# %%
# Define the schema of the data to receive from the Kafka broker
schema = StructType([
    StructField("batch_id", IntegerType()),
    StructField("camera_id", IntegerType()),
    StructField("producer_id", StringType()),
    StructField("events", 
        ArrayType(
            StructType([
                StructField("event_id", StringType()),
                StructField("car_plate", StringType()),
                StructField("timestamp", StringType()),
                StructField("speed_reading", StringType()),
                StructField("producer_sent_time", StringType())
            ])
        )
    )
])

# %%
def read_number_stream(topic_name):
    """
    Read vehicle event data from a Kafka topic as a streaming DataFrame.

    Parameters
    ----------
    topic_name : str
        Kafka topic name.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        Flattened streaming DataFrame of vehicle events.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select(
            col("data.batch_id"),
            col("data.camera_id"),
            col("data.producer_id"),
            explode(col("data.events")).alias("events")
        )
        .select(
            col("batch_id"),
            col("camera_id"),
            col("producer_id"),
            col("events.event_id"),
            col("events.car_plate"),
            to_timestamp(col("events.timestamp")).alias("event_time"),
            to_timestamp(col("events.producer_sent_time")).alias("producer_sent_time"),
            col("events.speed_reading").cast("double").alias("speed_reading")
        )
    )

def get_latency(df):
    return (
        df.withColumn("ingest_time", current_timestamp())
          .withColumn(
              "latency_seconds",
              expr(
                  "unix_timestamp(ingest_time) - unix_timestamp(producer_sent_time)"
              )
          )
          .select(
              col("event_id"),
              col("event_time").alias("timestamp"),
              col("latency_seconds")
          )
    )

try:
    stream_a = read_number_stream("camera-events-A")
    stream_b = read_number_stream("camera-events-B")
    stream_c = read_number_stream("camera-events-C")
    latency_a = get_latency(stream_a)
    latency_b = get_latency(stream_b)
    latency_c = get_latency(stream_c)

    print("Kafka streams created.")
except Exception as ex:
    print(str(ex))

# %% [markdown]
# ## Data Validation Pipeline
# 
# Incoming events are validated before violation detection begins.
# 
# The validation stage checks for:
# 
# * Missing vehicle plate numbers
# * Invalid timestamps
# * Unknown camera identifiers
# * Negative speed readings
# 
# The pipeline separates records into:
# 
# * Valid event streams
# * Invalid event streams
# 
# Invalid records are redirected into a dedicated MongoDB collection for auditing and troubleshooting purposes.
# 
# This stage ensures that downstream analytics operate only on reliable and clean streaming data.

# %%
# Validation pipeline
VALID_CAMERA_IDS = [1, 2, 3]

def split_valid_invalid(df):
    """
    Split vehicle events into valid and invalid records.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input streaming DataFrame of vehicle events.

    Returns
    -------
    tuple
        Valid and invalid DataFrames.
    """

    validated = (
        df
        .withColumn(
            "invalid_reason",
            when(
                col("car_plate").isNull() |
                (trim(col("car_plate")) == ""),
                lit("missing_car_plate")
            )
            .when(
                col("event_time").isNull(),
                lit("invalid_timestamp")
            )
            .when(
                ~col("camera_id").isin(VALID_CAMERA_IDS),
                lit("unknown_camera_id")
            )
            .when(
                col("speed_reading") < 0,
                lit("negative_speed")
            )
        )
    )

    invalid_rows = (
        validated
        .filter(col("invalid_reason").isNotNull())
    )

    valid_rows = (
        validated
        .filter(col("invalid_reason").isNull())
        .drop("invalid_reason")
    )

    return valid_rows, invalid_rows

# %%
stream_a, invalid_a = split_valid_invalid(stream_a)
stream_b, invalid_b = split_valid_invalid(stream_b)
stream_c, invalid_c = split_valid_invalid(stream_c)

# %% [markdown]
# ## Camera Configuration and Distance Computation
# 
# Camera metadata is retrieved from MongoDB, including:
# 
# * Camera identifiers
# * Geographic coordinates
# * Configurable speed limits
# 
# Using the geographic coordinates of adjacent cameras, the system calculates:
# 
# * Distance between camera pairs
# * Expected travel time windows between cameras
# 
# These computations are required for average-speed violation detection.
# 
# The calculated travel windows define the maximum allowable time interval for a vehicle to travel 

# %%
# Pass the full MONGO_URI string instead of an IP and Port
mongo_client = MongoClient(config.IP_ADDRESS, 27017)
db = mongo_client[config.DB_NAME]
cameras = list(db.cameras.find({}, {'_id' : 0}))
stream_metrics = db["stream_metrics"]

# Parameterizable speed-limit thresholds per camera as per the requirements
try:
    float(config.CAMERA_A_SPEED_LIMIT)
    cameras[0]["speed_limit"] = config.CAMERA_A_SPEED_LIMIT
except Exception as ex:
    pass
    
try:
    float(config.CAMERA_B_SPEED_LIMIT)
    cameras[0]["speed_limit"] = config.CAMERA_B_SPEED_LIMIT
except Exception as ex:
    pass

try:
    float(config.CAMERA_C_SPEED_LIMIT)
    cameras[0]["speed_limit"] = config.CAMERA_C_SPEED_LIMIT
except Exception as ex:
    pass

# For ease of average violation computation later
speed_limit_B = float(cameras[1]["speed_limit"])
speed_limit_C = float(cameras[2]["speed_limit"])

distance_AB = haversine_distance(
    float(cameras[0]["latitude"]), float(cameras[0]["longitude"]),
    float(cameras[1]["latitude"]), float(cameras[1]["longitude"])
)
distance_BC = haversine_distance(
    float(cameras[1]["latitude"]), float(cameras[1]["longitude"]),
    float(cameras[2]["latitude"]), float(cameras[2]["longitude"])
)

# Speed = Distance / Time, Time = Speed / Distance (Time here can be used as window interval, in seconds)
# Anything more than this time is not violation
estimated_time_window_AB = math.ceil((distance_AB / speed_limit_B) * 3600)
estimated_time_window_BC = math.ceil((distance_BC / speed_limit_C) * 3600)

try:
    float(config.WINDOW_INTERVAL_AB)
    estimated_time_window_AB = config.WINDOW_INTERVAL_AB
except Exception as ex:
    pass

try:
    float(config.WINDOW_INTERVAL_BC)
    estimated_time_window_BC = config.WINDOW_INTERVAL_BC
except Exception as ex:
    pass

# %% [markdown]
# ## Streaming Join Logic (2.1.2)
# 
# Watermarks are applied before performing online stream-to-stream joins to handle delayed or out-of-order events while controlling state retention and memory usage.
# 
# The system then performs real-time joins between adjacent camera streams:
# 
# - Camera A ↔ Camera B, Camera B ↔ Camera C
# 
# Vehicles are matched using:
# 
# - Vehicle plate number
# - Chronological event ordering
# - Maximum travel time constraints
# 
# These joins reconstruct vehicle journeys across road segments for average-speed detection. If the computed average speed exceeds the configured segment speed threshold, the vehicle is flagged for an average-speed violation. This enables the system to detect drivers attempting to evade enforcement by slowing down only near individual cameras.

# %%
try:
    # Add watermark to handle late events
    watermarked_a = stream_a.withWatermark("event_time", config.WATERMARK)
    watermarked_b = stream_b.withWatermark("event_time", config.WATERMARK)
    watermarked_c = stream_c.withWatermark("event_time", config.WATERMARK)

    # Join streams from Camera A and Camera B
    joined_stream_AB = (
        watermarked_a.alias("a")
            .join(
                watermarked_b.alias("b"),
                expr(f"""
                    a.car_plate = b.car_plate AND
                    b.event_time >= a.event_time AND
                    b.event_time <= a.event_time + interval {estimated_time_window_AB} seconds
                """),
                "inner"
        )
        .select(
            col("a.event_id").alias("a_event_id"),
            col("b.event_id").alias("b_event_id"),            
            coalesce(col("a.car_plate"), col("b.car_plate")).alias("car_plate"),
            col("a.camera_id").alias("camera_id_start"),
            col("b.camera_id").alias("camera_id_end"),
            col("a.event_time").alias("timestamp_start"),
            col("b.event_time").alias("timestamp_end"),
            col("a.speed_reading").alias("speed_start"),
            col("b.speed_reading").alias("speed_end")
        )
    )

    # Join streams from Camera B and Camera C
    joined_stream_BC = (
        watermarked_b.alias("b")
            .join(
                watermarked_c.alias("c"),
                expr(f"""
                    b.car_plate = c.car_plate AND
                    c.event_time >= b.event_time AND
                    c.event_time <= b.event_time + interval {estimated_time_window_BC} seconds
                """),
                "inner"
        )
        .select(
            col("b.event_id").alias("b_event_id"),
            col("c.event_id").alias("c_event_id"),   
            coalesce(col("b.car_plate"), col("c.car_plate")).alias("car_plate"),
            col("b.camera_id").alias("camera_id_start"),
            col("c.camera_id").alias("camera_id_end"),
            col("b.event_time").alias("timestamp_start"),
            col("c.event_time").alias("timestamp_end"),
            col("b.speed_reading").alias("speed_start"),
            col("c.speed_reading").alias("speed_end")
        )
    )

    print("Joins created successfully.")

except Exception as ex:
    print(str(ex))

# %% [markdown]
# ## Speed Violation Detection (2.1.4)
# 
# The system detects two types of speeding violations:
# 
# * Instantaneous speed violations — triggered when a vehicle exceeds the speed limit at an individual camera checkpoint.
# * Average-speed violations — detected by joining adjacent camera streams and calculating average speed using travel time and distance between cameras.
# 
# Vehicles exceeding the configured thresholds are flagged and forwarded to the persistence pipeline. This enables detection of both direct speeding and drivers attempting to evade enforcement by slowing down

# %%
def is_speeding(camera_id, speed):
    """
    Determine whether a vehicle exceeds the camera speed limit.

    Parameters
    ----------
    camera_id : int
        Camera identifier.
    speed : float
        Recorded vehicle speed.

    Returns
    -------
    bool
        True if the vehicle exceeds the speed limit.
    """
    return speed > float(cameras[camera_id - 1]["speed_limit"])

is_speeding_udf = udf(is_speeding, BooleanType())

instant_violations_A = stream_a.filter(is_speeding_udf(col("camera_id"), col("speed_reading")))
instant_violations_B = stream_b.filter(is_speeding_udf(col("camera_id"), col("speed_reading")))
instant_violations_C = stream_c.filter(is_speeding_udf(col("camera_id"), col("speed_reading")))

# %%
# Calculate average speed between Camera A and Camera B
ab_with_avg_speed = (
    joined_stream_AB
    .withColumn(
        "travel_time_hours",
        (unix_timestamp(col("timestamp_end")) - unix_timestamp(col("timestamp_start"))) / lit(3600.0)
    )
    .withColumn("avg_speed", lit(distance_AB) / col("travel_time_hours"))
).filter(col("avg_speed") > speed_limit_B)

# Calculate average speed between Camera B and Camera C
bc_with_avg_speed = (
    joined_stream_BC
    .withColumn(
        "travel_time_hours",
        (unix_timestamp(col("timestamp_end")) - unix_timestamp(col("timestamp_start"))) / lit(3600.0)
    )
    .withColumn("avg_speed", lit(distance_BC) / col("travel_time_hours"))
).filter(col("avg_speed") > speed_limit_C)

# %% [markdown]
# ## Streaming Application - MongoDB Sink Integration (2.1.3) 
# 
# Detected violations are written into MongoDB using partition-level batch operations.
# 
# The persistence layer:
# 
# * Aggregates violations by vehicle plate and date
# * Stores violations as embedded records
# * Prevents duplicate insertions during retries
# * Supports fault-tolerant bulk writes
# 
# Two categories of records are stored:
# 
# * Valid speeding violations
# * Invalid streaming events
# 
# MongoDB serves as the operational datastore for:
# 
# * Reporting
# * Analytics
# * Dashboards
# * Visualization layers

# %%
def write_violations(iterator):
    """
    Write violation records into MongoDB collections.

    Parameters
    ----------
    iterator : iterator
        Partition iterator containing violation records.
    """
    mongo_client = MongoClient(config.IP_ADDRESS, 27017)
    db = mongo_client[config.DB_NAME]

    violation_operations = []
    raw_violation_operations = []

    for row in iterator:
        row_dict = row.asDict()

        is_instantaneous = "event_time" in row_dict

        timestamp_start = row["event_time"] if is_instantaneous else row["timestamp_start"]
        timestamp_end = row["event_time"] if is_instantaneous else row["timestamp_end"]

        camera_id_start = row["camera_id"] if is_instantaneous else row["camera_id_start"]
        camera_id_end = row["camera_id"] if is_instantaneous else row["camera_id_end"]

        speed_reading = row["speed_reading"] if is_instantaneous else row["avg_speed"]

        violation_type = "INSTANTANEOUS" if is_instantaneous else "AVERAGE"

        violation = {
            "violation_type": violation_type,
            "camera_id_start": camera_id_start,
            "camera_id_end": camera_id_end,
            "timestamp_start": timestamp_start,
            "timestamp_end": timestamp_end,
            "speed_reading": speed_reading
        }

        # Aggregate violations by car plate and date
        violation_operations.append(
            UpdateOne(
                {
                    "car_plate": row["car_plate"],
                    "date": timestamp_start.date().isoformat()
                },
                {
                    "$setOnInsert": {
                        "car_plate": row["car_plate"],
                        "date": timestamp_start.date().isoformat()
                    },

                    # Prevent duplicate violations during retries
                    "$addToSet": {
                        "violations": violation
                    }
                },
                upsert=True
            )
        )

    # Bulk write aggregated violations
    if violation_operations:
        for attempt in range(config.RETRY_COUNT):
            try:
                db.violations.bulk_write(violation_operations)
                break
            except Exception as ex:
                print(str(ex))
                time.sleep(2 ** attempt)


def write_invalid(iterator):
    """
    Write invalid vehicle events into MongoDB.

    Parameters
    ----------
    iterator : iterator
        Partition iterator containing invalid records.
    """
    mongo_client = MongoClient(config.IP_ADDRESS, 27017)
    db = mongo_client[config.DB_NAME]

    records = []

    for row in iterator:
        record = row.asDict()
        records.append(record)

    if records:
        for attempt in range(config.RETRY_COUNT):
            try:
                db.invalid_events.insert_many(records)
                break
            except Exception as ex:
                print(str(ex))
                time.sleep(2 ** attempt)



def write_to_mongo_violations(batch_df, batch_id):
    """
    Write violation batch data to MongoDB.

    Parameters
    ----------
    batch_df : pyspark.sql.DataFrame
        Micro-batch DataFrame.
    batch_id : int
        Streaming batch identifier.
    """
    batch_df.foreachPartition(write_violations)

    
def write_to_mongo_invalid(batch_df, batch_id):
    """
    Write invalid batch data to MongoDB.

    Parameters
    ----------
    batch_df : pyspark.sql.DataFrame
        Micro-batch DataFrame.
    batch_id : int
        Streaming batch identifier.
    """
    batch_df.foreachPartition(write_invalid)

def write_latency_partition(iterator):
    mongo_client = MongoClient(config.IP_ADDRESS, 27017)
    db = mongo_client[config.DB_NAME]

    records = [row.asDict() for row in iterator]

    if records:
        for attempt in range(config.RETRY_COUNT):
            try:
                db.stream_latency.insert_many(records)
                break
            except Exception as ex:
                print(str(ex))
                time.sleep(2 ** attempt)


def write_latency_to_mongo(batch_df, batch_id):
    batch_df.foreachPartition(write_latency_partition)



# %% [markdown]
# ## Checkpointing and Recovery
# 
# Spark Structured Streaming queries use checkpoint directories for fault tolerance.
# 
# Checkpointing enables:
# 
# * Stream recovery after crashes
# * Stateful operation consistency
# * Exactly-once-like processing guarantees
# * Recovery of stream progress and join states
# 
# Separate checkpoints are maintained for:
# 
# * Instantaneous violation streams
# * Average-speed violation streams
# * Invalid event pipelines

# %%
# Remove existing Spark checkpoints to restart the stream from scratch
shutil.rmtree("./checkpoints/qA", ignore_errors=True)
shutil.rmtree("./checkpoints/qB", ignore_errors=True)
shutil.rmtree("./checkpoints/qC", ignore_errors=True)
shutil.rmtree("./checkpoints/qAB", ignore_errors=True)
shutil.rmtree("./checkpoints/qBC", ignore_errors=True)
shutil.rmtree("./checkpoints/invalid_a", ignore_errors=True)
shutil.rmtree("./checkpoints/invalid_b", ignore_errors=True)
shutil.rmtree("./checkpoints/invalid_c", ignore_errors=True)

# %%
try:
    spark.sparkContext.setLogLevel("ERROR")
    q_latency_A = latency_a.writeStream.foreachBatch(write_latency_to_mongo).start()
    q_latency_B = latency_b.writeStream.foreachBatch(write_latency_to_mongo).start()
    q_latency_C = latency_c.writeStream.foreachBatch(write_latency_to_mongo).start()

    
    # Start streaming queries with checkpointing for fault tolerance
    qA = instant_violations_A.writeStream \
        .foreachBatch(write_to_mongo_violations) \
        .option("checkpointLocation", "./checkpoints/qA") \
        .start()

    qB = instant_violations_B.writeStream \
        .foreachBatch(write_to_mongo_violations) \
        .option("checkpointLocation", "./checkpoints/qB") \
        .start()

    qC = instant_violations_C.writeStream \
        .foreachBatch(write_to_mongo_violations) \
        .option("checkpointLocation", "./checkpoints/qC") \
        .start()

    qAB = ab_with_avg_speed.writeStream \
        .foreachBatch(write_to_mongo_violations) \
        .option("checkpointLocation", "./checkpoints/qAB") \
        .start()

    qBC = bc_with_avg_speed.writeStream \
        .foreachBatch(write_to_mongo_violations) \
        .option("checkpointLocation", "./checkpoints/qBC") \
        .start()

    invalid_query_A = (
        invalid_a.writeStream
        .foreachBatch(write_to_mongo_invalid)
        .option("checkpointLocation", "./checkpoints/invalid_a")
        .start()
    )
    
    invalid_query_B= (
        invalid_b.writeStream
        .foreachBatch(write_to_mongo_invalid)
        .option("checkpointLocation", "./checkpoints/invalid_b")
        .start()
    )

    
    invalid_query_C= (
        invalid_c.writeStream
        .foreachBatch(write_to_mongo_invalid)
        .option("checkpointLocation", "./checkpoints/invalid_c")
        .start()
    )


    queries = {
        "latency_A": q_latency_A,
        "latency_B": q_latency_B,
        "latency_C": q_latency_C
    }

    last_batches = {
        "latency_A": -1,
        "latency_B": -1,
        "latency_C": -1
    }

    while any(q.isActive for q in queries.values()):

        for query_name, query in queries.items():

            progress = query.lastProgress

            if progress:

                batch_id = progress["batchId"]

                if batch_id > last_batches[query_name]:

                    stream_metrics.insert_one({
                        "query": query_name,
                        "batch_id": batch_id,
                        "timestamp": progress["timestamp"],
                        "num_input_rows": progress["numInputRows"],
                        "input_rows_per_second": progress["inputRowsPerSecond"],
                        "processed_rows_per_second": progress["processedRowsPerSecond"]
                    })

                    last_batches[query_name] = batch_id

        time.sleep(1)

    spark.streams.awaitAnyTermination()
except Exception as ex:
    print(str(ex))

