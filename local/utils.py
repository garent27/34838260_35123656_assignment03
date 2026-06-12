import logging
import csv
import datetime as dt
from math import radians, sin, cos, sqrt, asin
import json
from kafka import KafkaProducer
import config



def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the distance between two geographic coordinates in kilometres.

    Parameters
    ----------
    lat1, lon1 : float
        Latitude and longitude of the first location.
    lat2, lon2 : float
        Latitude and longitude of the second location.

    Returns
    -------
    float
        Distance between the two coordinates in kilometres.
    """

    # Earth radius in km
    R = 6371

    # Convert degrees to radians
    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    # Differences
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Haversine formula
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))

    distance = R * c
    return distance


def read_csv_to_dict_list(file_path):
    """
    Read a CSV file into a list of dictionaries.

    Parameters
    ----------
    file_path : str
        Path to the CSV file.

    Returns
    -------
    list
        List of rows represented as dictionaries.
    """
    rows = []

    with open(file_path, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            rows.append(dict(row))

    return rows


def filter_row(row):
    """
    Convert row values into appropriate Python data types.

    Parameters
    ----------
    row : dict
        Input row dictionary.

    Returns
    -------
    dict
        Cleaned row with converted values.
    """
    clean_row = {}

    for key, value in row.items():
        if value == "" or value is None:
            clean_row[key] = None
            continue

        # date check first
        try:
            clean_row[key] = dt.datetime.fromisoformat(value)
            continue
        except:
            pass

        # number check
        try:
            num = float(value)

            # integer should remain integer
            if num.is_integer():
                clean_row[key] = int(num)
            else:
                clean_row[key] = num

            continue
        except:
            pass

        # fallback string
        clean_row[key] = value

    return clean_row


def publish_message(producer_instance, topic_name, data):
    """
    Publish a message to a Kafka topic.

    Parameters
    ----------
    producer_instance : KafkaProducer
        Active Kafka producer instance.
    topic_name : str
        Kafka topic name.
    data : dict
        Message payload.
    """
    try:
        message = json.dumps(data).encode("utf-8")

        producer_instance.send(topic_name, value=message)
        producer_instance.flush()

        print("Message published successfully.")
        print(data)
        print()

    except Exception as ex:
        print("Exception in publishing message.")
        print(str(ex))

        
def connect_kafka_producer():
    """
    Create and return a Kafka producer connection.

    Returns
    -------
    KafkaProducer
        Kafka producer instance.
    """
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=[f'{config.IP_ADDRESS}:9092'],
                                  api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka.')
        print(str(ex))
    finally:
        return _producer
            