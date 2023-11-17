import argparse
import json
import pprint
from datetime import datetime
from google.cloud import pubsub_v1

pp = pprint.PrettyPrinter(indent=4)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--project-id",
    dest="project_id")
parser.add_argument(
    "--sub-name",
    dest="sub_name")
args = parser.parse_args()

# Code created from this example: 
# https://cloud.google.com/python/docs/reference/pubsub/latest/google.cloud.pubsub_v1.subscriber.client.Client#google_cloud_pubsub_v1_subscriber_client_Client_subscribe

# def callback(message):
#     pp.pprint(message.data)
#     message.ack()

class MessageCount:
    def __init__(self):
        self.total_count = 0

    def callback(self, message):
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
        
counter = MessageCount()

# Create PubSub subscriber
subscriber_client = pubsub_v1.SubscriberClient()
subscription = subscriber_client.subscription_path(args.project_id, args.sub_name)

future = subscriber_client.subscribe(subscription, counter.callback)

try:
    future.result()
except KeyboardInterrupt:
    future.cancel()
    future.result()
