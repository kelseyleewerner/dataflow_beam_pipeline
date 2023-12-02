import argparse
import csv
import json
from google.cloud import logging, pubsub_v1



# import pprint
# pp = pprint.PrettyPrinter(indent=4)


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
parser.add_argument(
      '--log-name',
      dest='log_name')        
args = parser.parse_args()

# Create PubSub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(args.project_id, args.topic)

logging_client = logging.Client()
logger = logging_client.logger(args.log_name)
logger.log_text("Commencing stream of horror movie data to Pub/Sub", severity="INFO")

# Each line of the csv is published as a dictionary where the keys are the column names
try:
    with open(args.infile, mode="r") as infile:
        messages = csv.DictReader(infile)

        for message in messages:
            future = publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
            future.result()
except BaseException as err:
    logger.log_struct(
        {
            "message": "Error occurred while streaming data from CSV file to Pub/Sub",
            "error_type": F"{type(err)}",
            "error_body": F"{err}"
        },
        severity="ERROR"
    )
    raise