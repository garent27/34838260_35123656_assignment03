# 1. Keep Kafka pointing to AWS Cloud Broker
IP_ADDRESS = "34.207.236.209" 

# 2. Add a Local IP for MongoDB / Visualizations on your laptop
LOCAL_HOST = "127.0.0.1"

MONGO_URI = "mongodb+srv://khangwei0001_db_user:7gttxKZRqrKYmKqC@fit3182-awas.70l4zwt.mongodb.net/?appName=fit3182-awas"

# MONGODB
DB_NAME = "fit3182_awas" # Set mongodb database name for AWAS application
RETRY_COUNT = 3 # Set how many times to retry writing to mongodb before stopping

# STREAMING
BATCH_PUBLISH_RATE_A = 1 # Set publish rate here for camera A (in seconds)
BATCH_PUBLISH_RATE_B = 1/10 # Set publish rate here for camera B (in seconds)
BATCH_PUBLISH_RATE_C = 1/14 # Set publish rate here for camera C (in seconds)
WATERMARK = "5 minutes" # Set watermark [Need to add second(s), minute(s), hour(s), day(s) after the numerical value]
WINDOW_INTERVAL_AB = None # Set A and B interval (in seconds) [Default value provided in streaming notebook]
WINDOW_INTERVAL_BC = None # Set B and C interval (in seconds) [Default value provided in streaming notebook]

# PARAMETERIZABLE SPEED LIMIT FOR EACH CAMERA
CAMERA_A_SPEED_LIMIT = None
CAMERA_B_SPEED_LIMIT = None
CAMERA_C_SPEED_LIMIT = None
