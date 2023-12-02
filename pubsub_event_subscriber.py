import argparse
import json
import pprint
import traceback
from datetime import datetime
from google.cloud import logging, pubsub_v1


pp = pprint.PrettyPrinter(indent=4)


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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        dest="project_id")
    parser.add_argument(
        "--sub-name",
        dest="sub_name")
    args = parser.parse_args()
except:
    log_error(logger, "Error occurred while parsing command line arguments")
    raise 


class MessageCount:
    def __init__(self):
        self.total_count = 0

    def callback(self, message):
        try:
            msg = message.data.decode('utf-8')
            msg_obj = json.loads(msg)
            msg_count = msg_obj['film_count']

            self.total_count += msg_count

            print("Original Message:")
            pp.pprint(message.data)
            print(F"Message Count: {msg_count}")
            print(F"Timestamp: {datetime.fromtimestamp(msg_obj['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}")
            print(F"RUNNING TOTAL: {self.total_count}")    

            message.ack()
        except:
            log_error(logger, "Error occurred while processing message from Pub/Sub topic")
            raise 
        
counter = MessageCount()


# Code created from this example: 
# https://cloud.google.com/python/docs/reference/pubsub/latest/google.cloud.pubsub_v1.subscriber.client.Client#google_cloud_pubsub_v1_subscriber_client_Client_subscribe
logger.log_text("Commencing stream of horror movie data from Pub/Sub and displaying in terminal", severity="INFO")

try:
    # Create PubSub subscriber
    subscriber_client = pubsub_v1.SubscriberClient()
    subscription = subscriber_client.subscription_path(args.project_id, args.sub_name)

    future = subscriber_client.subscribe(subscription, counter.callback)
except:
    log_error(logger, "Error occurred while subscribig to Pub/Sub topic")
    raise 

try:
    future.result()
except KeyboardInterrupt:
    future.cancel()
    future.result()
except:
    log_error(logger, "Error occurred while receiving messages from Pub/Sub topic")
    raise 