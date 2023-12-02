import argparse
import csv
import json
import traceback
from google.cloud import logging, pubsub_v1


# import pprint
# pp = pprint.PrettyPrinter(indent=4)


LOG_NAME = "horror_movie_dataset"

def log_error(log, err_message):
    log.log_struct(
        {
            "message": err_message,
            "error": traceback.format_exc()
        },
        severity="ERROR"
    )

logging_client = logging.Client()
logger = logging_client.logger(LOG_NAME)


try:
    # Parse argument from user
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        dest="project_id")
    parser.add_argument(
        "--topic",
        dest="topic")
    parser.add_argument(
        "--infile",
        dest="infile")       
    args = parser.parse_args()
except:
    log_error(logger, "Error occurred while parsing command line arguments")
    raise 

try:
    # Create PubSub publisher
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project_id, args.topic)
except:
    log_error(logger, "Error occurred while creating Pub/Sub publisher")
    raise

logger.log_text("Commencing stream of horror movie data to Pub/Sub from CSV", severity="INFO")

# Each line of the csv is published as a dictionary where the keys are the column names
try:
    with open(args.infile, mode="r") as infile:
        messages = csv.DictReader(infile)

        for message in messages:
            future = publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
            future.result()
except:
    log_error(logger, "Error occurred while streaming data from CSV file to Pub/Sub")    
    raise